import csv, datetime, json, pathlib, subprocess, time, sys

ROOT = pathlib.Path(__file__).resolve().parent
RUN = ROOT / (sys.argv[1] if len(sys.argv)>1 else datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=3))).strftime('%Y%m%d-%H%M%S'))
RUN.mkdir(exist_ok=True)
VARIANTS = [(fmt, codec) for fmt in ('parquet', 'orc') for codec in ('none', 'snappy', 'gzip', 'zstd')]
rows = json.loads((RUN / 'results.json').read_text()) if (RUN / 'results.json').exists() else []
writes = json.loads((RUN / 'write_times.json').read_text()) if (RUN / 'write_times.json').exists() else {}

def sql(query, label):
    start = time.perf_counter()
    p = subprocess.run(['docker', 'exec', 'trino', 'trino', '--output-format', 'CSV', '--execute', query], capture_output=True, text=True)
    elapsed = time.perf_counter() - start
    (RUN / (label + '.log')).write_text(query + '\n\nSTDOUT\n' + p.stdout + '\nSTDERR\n' + p.stderr)
    if p.returncode:
        raise RuntimeError(label + ': ' + p.stderr + p.stdout)
    return p.stdout, elapsed

def data(query, label):
    out, _ = sql(query, label)
    return list(csv.reader(out.splitlines()))

def ready():
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        p = subprocess.run(['docker', 'exec', 'trino', 'curl', '-fsS', '--max-time', '3', 'http://localhost:8080/v1/info'], capture_output=True, text=True)
        if p.returncode == 0:
            try:
                if not json.loads(p.stdout).get('starting', True):
                    probe = subprocess.run(['docker','exec','trino','trino','--output-format','CSV','--execute', "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'"], capture_output=True, text=True)
                    if probe.returncode == 0 and probe.stdout.strip().strip(chr(34)).isdigit() and int(probe.stdout.strip().strip(chr(34))) > 0:
                        time.sleep(3)
                        return
            except ValueError:
                pass
        time.sleep(2)
    raise RuntimeError('Trino did not become ready')

def save():
    (RUN / 'results.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    if rows:
        with (RUN / 'section10.csv').open('w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)

print('Artifacts:', RUN, flush=True)
# Confirm all existing inputs before any benchmark writes.
inputs = {}
for fmt, codec in VARIANTS:
    variant = fmt + '_' + codec
    table = 'bench.lakehouse.lineitem_' + variant
    props = dict(data(f'SELECT key,value FROM bench.lakehouse."lineitem_{variant}$properties"', variant + '_properties'))
    actual = props.get(f'write.{fmt}.compression-codec', '').lower()
    allowed = {'none', 'uncompressed'} if codec == 'none' else ({'gzip','zlib'} if codec == 'gzip' else {codec})
    if actual not in allowed:
        raise RuntimeError(f'{variant}: expected {codec}, got {actual}')
    stats = data(f'SELECT count(*), sum(file_size_in_bytes), sum(record_count) FROM bench.lakehouse."lineitem_{variant}$files"', variant + '_files')[0]
    files, size, records = map(int, stats)
    if records != 6001215:
        raise RuntimeError(f'{variant}: not sf1: {records}')
    inputs[variant] = (files, size, records, actual)
print('All 8 tables: sf1 and compression verified', flush=True)
for fmt, codec in VARIANTS:
    variant = fmt + '_' + codec
    files, size, records, actual = inputs[variant]
    tmp = 'bench.lakehouse.measure_' + variant + '_' + RUN.name.replace('-', '_')
    if any(r['variant'] == variant for r in rows):
        continue
    print('Measuring write:', variant, flush=True)
    create = f"SET SESSION bench.compression_codec = '{codec.upper()}'; CREATE TABLE {tmp} WITH (format='{fmt.upper()}') AS SELECT * FROM tpch.sf1.lineitem"
    if variant in writes:
        write_s = writes[variant]
    else:
        _, write_s = sql(create, variant + '_write')
        writes[variant] = write_s
        (RUN / 'write_times.json').write_text(json.dumps(writes, indent=2))
        try:
            count = data(f'SELECT count(*) FROM {tmp}', variant + '_write_check')[0][0]
            if int(count) != records:
                raise RuntimeError('Temporary table row count mismatch')
        finally:
            sql(f'DROP TABLE {tmp}', variant + '_cleanup')
    print(f'{variant}: write {write_s:.3f}s; restarting Trino and MinIO for first read', flush=True)
    p = subprocess.run(['docker', 'restart', 'trino', 'lakehouse-minio'], capture_output=True, text=True)
    (RUN / (variant + '_restart.log')).write_text(p.stdout + p.stderr)
    if p.returncode:
        raise RuntimeError('Restart failed')
    ready()
    query = f'''SELECT returnflag, linestatus, count(*) AS cnt,
 sum(quantity) AS sum_qty, sum(extendedprice) AS sum_price,
 sum(discount) AS sum_disc,
 sum(extendedprice * (1-discount)) AS sum_charge
 FROM bench.lakehouse.lineitem_{variant}
 GROUP BY returnflag, linestatus'''
    cold_out, cold = sql(query, variant + '_cold')
    warm_out, warm = sql(query, variant + '_warm')
    cold_rows = list(csv.reader(cold_out.splitlines()))
    warm_rows = list(csv.reader(warm_out.splitlines()))
    if len(cold_rows) != len(warm_rows) or sum(int(r[2]) for r in cold_rows) != records or sum(int(r[2]) for r in warm_rows) != records:
        raise RuntimeError('Aggregation row counts mismatch')
    rows.append(dict(variant=variant, bytes=size, size_gib=round(size/1024**3,6), files=files, records=records, codec=actual, write_seconds=round(write_s,3), cold_seconds=round(cold,3), warm_seconds=round(warm,3)))
    save()
    print(f'{variant}: cold {cold:.3f}s; warm {warm:.3f}s; {len(rows)}/8 complete', flush=True)

lines = ['# §10. Сводка результатов — sf1', '', 'Измерения выполнены последовательно. Размеры и число файлов относятся к исходным таблицам. Время записи измерено на временных копиях из tpch.sf1 с теми же форматом и кодеком; после проверки копии удалены.', '', 'Все времена — wall time процесса docker exec trino trino, включая запуск CLI; SET SESSION включён во время записи. DROP и проверка количества строк в время записи не входят.', '', 'Cold — первый запрос после перезапуска Trino и MinIO; warm — немедленный повтор. Перезапуск контейнеров не гарантирует очистку page cache ОС, поэтому cold здесь условный. Выполнена одна пара замеров для каждого варианта.', '', '| Вариант | Размер, ГиБ | Файлов | Запись, с | Cold, с | Warm, с |', '|---|---:|---:|---:|---:|---:|']
for r in rows:
    lines.append(f"| {r['variant']} | {r['size_gib']:.4f} | {r['files']} | {r['write_seconds']:.3f} | {r['cold_seconds']:.3f} | {r['warm_seconds']:.3f} |")
(RUN / 'section10.md').write_text('\n'.join(lines)+'\n')
print('COMPLETE', RUN, flush=True)
