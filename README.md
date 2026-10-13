# Отчёт по Практической работе №1

В работе я использовал масштаб **sf1**, из-за того что на ноутбуке мало свободного места и его производительности недостаточно для выполнения всех экспериментов на больших объёмах. Вместо `sf10` из базового примера я использовал `sf1`. Во всех восьми таблицах получилось по **6 001 215 строк**. Для сравнения форматов и кодеков объём данных оставался одинаковым.

| Поле | Значение |
|---|---|
| ФИО | Грачев Ярослав |
| Тема | Бенчмарк Parquet и ORC, кодеки сжатия и выполнение аналитических запросов |
| Масштаб | sf1 |

## 1. Цель работы

Цель — сравнить влияние формата и сжатия на размер файлов, время материализации и скорость агрегации. 

Для каждого варианта использовались одинаковые исходные строки. Менялись формат файлов и кодек. Работа выполнялась локально, поэтому выполнение по временени зависило от нагрузки на ноутбук.

## 2. Поднятые сервисы и архитектура

```mermaid
flowchart LR
    D[DBeaver] -->|SQL, порт 8080| T[Trino 476]
    C[Trino CLI из терминала] --> T
    T -->|Iceberg REST, порт 8181| L[Lakekeeper]
    T -->|S3 API, порт 9000| S[Silo: bucket bench]
    L -->|проверка и запись метаданных в S3| S
    L -->|postgres:5432 / lakekeeper| P[(PostgreSQL 16)]
```

| Сервис | Контейнер | Образ | Назначение и порты |
|---|---|---|---|
| Silo | `lakehouse-minio` | `pgsty/silo:latest` | S3-хранилище: API 9000, веб-консоль 9001 |
| PostgreSQL | `lakekeeper-postgres` | `postgres:16` | База `lakekeeper`; порт хоста 5433 → порт контейнера 5432 |
| Lakekeeper | `lakekeeper` | `quay.io/lakekeeper/catalog:latest-main` | Iceberg REST Catalog, порт 8181 |
| Trino | `trino` | `trinodb/trino:476` | SQL-движок, порт 8080 |
| PostgreSQL из первоначальной настройки | `lakehouse-pg` | `postgres:17` | Порт 5432; текущий Lakekeeper подключён к другому контейнеру — `lakekeeper-postgres` |

Файлы данных и Iceberg-метаданные размещаются в Silo. Lakekeeper регистрирует таблицы и управляет каталогом с использованием PostgreSQL. Trino получает сведения о таблицах через Lakekeeper и читает файлы через S3 API.

Во время замеров `docker stats` показывал общий предел памяти Docker около 3,828 ГиБ. Это лимит среды Docker, а не установленная память ноутбука. Точное число ядер и объём памяти Mac в результатах не фиксировались.

## 3. Структура папки работы

```text
laba2/
├── catalog/
│   ├── bench.properties
│   ├── lakekeeper.properties
│   ├── tpch.properties
│   └── bench.properties.before-laba2-fix.bak
├── measurements/
│   ├── measure_section10.py
│   └── 20261013-104905/
│       ├── section10.md
│       ├── section10.csv
│       ├── results.json
│       ├── write_times.json
│       └── *.log
├── tpch-spark/                  # материалы следующей части
├── гайд-3-бенчмарк.md
├── гайд-3-доп-схема-эволюция.ipynb
└── res.md
```

Конфигурация каталогов первоначально находилась в `~/lakehouse/catalog`. В ходе оформления работы она перенес в `laba2/catalog`. Trino был пересоздан с новым bind mount; после переноса проверены `SHOW CATALOGS` и чтение 6 001 215 строк из `lineitem_parquet_zstd`.

## 4. Как поднимался стек

Контейнеры запускались отдельными командами Docker. Ниже приведены команды из гайдов, адаптированные к фактически используемым именам, портам и путям. Команды первичного создания предназначены для отсутствующих контейнеров; для уже созданного стека использовался `docker start`.

### 4.1. Silo

```bash
docker run -d --name lakehouse-minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v minio-data:/data \
  pgsty/silo:latest server /data --console-address ":9001"
```

Том `minio-data` хранит файлы отдельно от жизненного цикла контейнера. В консоли на `http://localhost:9001` использовалась учебная учётная запись `minioadmin`.

### 4.2. PostgreSQL для Lakekeeper

В исходном гайде предлагается `lakehouse-pg` на PostgreSQL 17. Такой контейнер сохранился в окружении, однако фактическое подключение Lakekeeper настроено на `lakekeeper-postgres` с PostgreSQL 16 и базой `lakekeeper`. Эквивалент команды его создания:

```bash
docker run -d --name lakekeeper-postgres \
  -p 5433:5432 \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=lakekeeper \
  postgres:16
```

У существующего контейнера каталог `/var/lib/postgresql/data` размещён в автоматически созданном Docker-томе. Готовность базы проверяется так:

```bash
docker exec lakekeeper-postgres pg_isready -U postgres -d lakekeeper
```

### 4.3. Lakekeeper

В текущей конфигурации имя `postgres` внутри Lakekeeper связано с контейнером `lakekeeper-postgres` через Docker link. Команды инициализации по гайду, адаптированные под это подключение:

```bash
docker run --rm --link lakekeeper-postgres:postgres \
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@postgres:5432/lakekeeper' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@postgres:5432/lakekeeper' \
  quay.io/lakekeeper/catalog:latest-main migrate

docker run -d --name lakekeeper \
  --link lakekeeper-postgres:postgres \
  -p 8181:8181 \
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@postgres:5432/lakekeeper' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@postgres:5432/lakekeeper' \
  quay.io/lakekeeper/catalog:latest-main serve
```

Шаг `migrate` относится к первичной подготовке каталога; журнал его первоначального выполнения в материалах отчёта не сохранён. При работе с лабораторной использовался уже инициализированный экземпляр Lakekeeper.

### 4.4. Бакет и warehouse bench

Для создания бакета я выполнил:

```bash
docker run --rm --entrypoint /bin/sh \
  --add-host host.docker.internal:host-gateway \
  pgsty/mc:latest -c \
  "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin && mc mb --ignore-existing m/bench"
```

Результат команды:

```text
Added `m` successfully.
Bucket created successfully `m/bench`.
```

Команда регистрации warehouse из гайда:

```bash
curl -X POST http://localhost:8181/management/v1/warehouse \
  -H "Content-Type: application/json" \
  -d '{
    "warehouse-name": "bench",
    "project-id": "00000000-0000-0000-0000-000000000000",
    "storage-profile": {
      "type": "s3",
      "bucket": "bench",
      "key-prefix": "",
      "endpoint": "http://host.docker.internal:9000",
      "region": "us-east-1",
      "path-style-access": true,
      "flavor": "s3-compat",
      "sts-enabled": false
    },
    "storage-credential": {
      "type": "s3",
      "credential-type": "access-key",
      "access-key-id": "minioadmin",
      "secret-access-key": "minioadmin"
    }
  }'
```

При повторной регистрации я получил `Storage profile overlaps with existing warehouse bench`. Warehouse уже существовал, поэтому повторное создание было пропущено. Его работоспособность впоследствии подтверждена созданием и чтением таблиц.

### 4.5. Каталоги Trino

Основная настройка бенчмарка в `catalog/bench.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://host.docker.internal:8181/catalog
iceberg.rest-catalog.warehouse=bench
iceberg.rest-catalog.nested-namespace-enabled=true
iceberg.unique-table-location=true
fs.native-s3.enabled=true
s3.endpoint=http://host.docker.internal:9000
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
```

В `catalog/tpch.properties` находится настройка генератора:

```properties
connector.name=tpch
```

Также сохранён `lakekeeper.properties` от предыдущей работы. Он обращается к warehouse `lakehouse`, тогда как все измерения этой работы выполнялись через каталог `bench`.

### 4.6. Trino и перенос конфигурации

После переноса конфигов я использую следующую рабочую папку и команду создания Trino:

```bash

docker run -d --name trino \
  -p 8080:8080 \
  --add-host host.docker.internal:host-gateway \
  -v "$(pwd)/catalog:/etc/trino/catalog" \
  trinodb/trino:476
```

Для смены пути монтирования старый контейнер был остановлен и удалён, затем создан с этой командой. Он также подключён к созданной во время диагностики сети `lakehouse-lab`:

```bash
docker network connect lakehouse-lab trino
```

В итоговой конфигурации адреса Lakekeeper и Silo остались такими, как в задании: `host.docker.internal:8181` и `host.docker.internal:9000`.

Для запуска существующих контейнеров после остановки Docker:

```bash
docker start lakehouse-minio lakekeeper-postgres lakekeeper trino
```

Проверки готовности:

```bash
docker ps
curl -s http://localhost:8181/health
curl -s http://localhost:8080/v1/info
docker exec trino trino --execute "SHOW CATALOGS"
```

Полученный список каталогов: `bench`, `lakekeeper`, `system`, `tpch`. SQL также выполнялся через DBeaver, подключённый к Trino на `localhost:8080`.

## 5. Создание восьми таблиц на sf1

Я использовал схему `bench.lakehouse`. Ниже приведён полный набор команд первичного создания. В Trino 476 кодек задаётся через `SET SESSION`, а формат — через `WITH (format=...)`. Обе команды должны выполняться в одной сессии.

```bash
docker exec -i trino trino <<'SQL'
CREATE SCHEMA IF NOT EXISTS bench.lakehouse;


SET SESSION bench.compression_codec = 'NONE';
CREATE TABLE bench.lakehouse.lineitem_parquet_none
WITH (format = 'PARQUET')
AS SELECT * FROM tpch.sf1.lineitem;


SET SESSION bench.compression_codec = 'SNAPPY';
CREATE TABLE bench.lakehouse.lineitem_parquet_snappy
WITH (format = 'PARQUET')
AS SELECT * FROM tpch.sf1.lineitem;


SET SESSION bench.compression_codec = 'GZIP';
CREATE TABLE bench.lakehouse.lineitem_parquet_gzip
WITH (format = 'PARQUET')
AS SELECT * FROM tpch.sf1.lineitem;


SET SESSION bench.compression_codec = 'ZSTD';
CREATE TABLE bench.lakehouse.lineitem_parquet_zstd
WITH (format = 'PARQUET')
AS SELECT * FROM tpch.sf1.lineitem;


SET SESSION bench.compression_codec = 'NONE';
CREATE TABLE bench.lakehouse.lineitem_orc_none
WITH (format = 'ORC')
AS SELECT * FROM tpch.sf1.lineitem;


SET SESSION bench.compression_codec = 'SNAPPY';
CREATE TABLE bench.lakehouse.lineitem_orc_snappy
WITH (format = 'ORC')
AS SELECT * FROM tpch.sf1.lineitem;


SET SESSION bench.compression_codec = 'GZIP';
CREATE TABLE bench.lakehouse.lineitem_orc_gzip
WITH (format = 'ORC')
AS SELECT * FROM tpch.sf1.lineitem;


SET SESSION bench.compression_codec = 'ZSTD';
CREATE TABLE bench.lakehouse.lineitem_orc_zstd
WITH (format = 'ORC')
AS SELECT * FROM tpch.sf1.lineitem;

SQL
```

Уже существующие таблицы повторно создавать этим блоком не требуется. В ходе исправления неверных кодеков я пересоздавал только соответствующие учебные таблицы, выполняя `DROP TABLE`, `SET SESSION` и `CREATE TABLE` в одной CLI-сессии.

Проверка списка:

```sql
SHOW TABLES IN bench.lakehouse;
```

Проверка свойств конкретной таблицы:

```sql
SELECT key, value
FROM bench.lakehouse."lineitem_parquet_zstd$properties"
WHERE key LIKE 'write.%.compression-codec'
   OR key = 'write.format.default';
```

Для Parquet ZSTD получены `PARQUET` и `zstd`. Проверка всех восьми таблиц перед замерами дала:

| Таблица | Формат | Фактический кодек в свойствах |
|---|---|---|
| lineitem_parquet_none | PARQUET | uncompressed |
| lineitem_parquet_snappy | PARQUET | snappy |
| lineitem_parquet_gzip | PARQUET | gzip |
| lineitem_parquet_zstd | PARQUET | zstd |
| lineitem_orc_none | ORC | none |
| lineitem_orc_snappy | ORC | snappy |
| lineitem_orc_gzip | ORC | zlib |
| lineitem_orc_zstd | ORC | zstd |

## 6. Размер и количество файлов

Для одной таблицы использовался запрос:

```sql
SELECT count(*) AS files,
       sum(file_size_in_bytes) AS bytes
FROM bench.lakehouse."lineitem_parquet_zstd$files";
```

Чтобы собрать размеры сразу всех вариантов:

```sql

SELECT 'parquet_none' AS t, count(*) AS files, sum(file_size_in_bytes) AS bytes
FROM bench.lakehouse."lineitem_parquet_none$files"
UNION ALL
SELECT 'parquet_snappy' AS t, count(*) AS files, sum(file_size_in_bytes) AS bytes
FROM bench.lakehouse."lineitem_parquet_snappy$files"
UNION ALL
SELECT 'parquet_gzip' AS t, count(*) AS files, sum(file_size_in_bytes) AS bytes
FROM bench.lakehouse."lineitem_parquet_gzip$files"
UNION ALL
SELECT 'parquet_zstd' AS t, count(*) AS files, sum(file_size_in_bytes) AS bytes
FROM bench.lakehouse."lineitem_parquet_zstd$files"
UNION ALL
SELECT 'orc_none' AS t, count(*) AS files, sum(file_size_in_bytes) AS bytes
FROM bench.lakehouse."lineitem_orc_none$files"
UNION ALL
SELECT 'orc_snappy' AS t, count(*) AS files, sum(file_size_in_bytes) AS bytes
FROM bench.lakehouse."lineitem_orc_snappy$files"
UNION ALL
SELECT 'orc_gzip' AS t, count(*) AS files, sum(file_size_in_bytes) AS bytes
FROM bench.lakehouse."lineitem_orc_gzip$files"
UNION ALL
SELECT 'orc_zstd' AS t, count(*) AS files, sum(file_size_in_bytes) AS bytes
FROM bench.lakehouse."lineitem_orc_zstd$files";
```

Проверка партиций:

```sql
SELECT record_count, file_count, total_size
FROM bench.lakehouse."lineitem_parquet_zstd$partitions";
```

Результат: **6 001 215 строк, 4 файла, 153 231 705 байт**. Одна строка в `$partitions` соответствует таблице без заданного партиционирования.

| Вариант | Размер, байт | Размер, ГиБ | Файлов |
|---|---:|---:|---:|
| parquet_none | 375 159 684 | 0,3494 | 4 |
| parquet_snappy | 206 800 822 | 0,1926 | 4 |
| parquet_gzip | 149 525 399 | 0,1393 | 4 |
| parquet_zstd | 153 231 705 | 0,1427 | 4 |
| orc_none | 452 619 231 | 0,4215 | 4 |
| orc_snappy | 221 430 469 | 0,2062 | 4 |
| orc_gzip | 155 824 766 | 0,1451 | 4 |
| orc_zstd | 161 174 051 | 0,1501 | 4 |

Пересчёт единиц: `ГиБ = bytes / 1 073 741 824`. Здесь измеряется сумма файлов текущего состояния таблицы, а не весь занятый объём бакета вместе с историческими файлами и метаданными.

## 7. Методика замеров записи и агрегации

### 7.1. Запись

Поскольку при первоначальном создании времена всех таблиц не были сохранены, для заполнения §10 были выполнены дополнительные замеры на временных копиях. Источник — `tpch.sf1.lineitem`, формат и кодек соответствуют каждому варианту. После проверки числа строк копии удалялись.

Эквивалент замера для Parquet ZSTD, адаптированный из гайда:

```bash
time docker exec trino trino --execute \
  "SET SESSION bench.compression_codec = 'ZSTD';
   CREATE TABLE bench.lakehouse.measure_parquet_zstd
   WITH (format = 'PARQUET')
   AS SELECT * FROM tpch.sf1.lineitem;"
```

Проверка и очистка выполняются отдельно от таймера:

```bash
docker exec trino trino --execute \
  "SELECT count(*) FROM bench.lakehouse.measure_parquet_zstd"
docker exec trino trino --execute \
  "DROP TABLE bench.lakehouse.measure_parquet_zstd"
```

Фактически серии запускались скриптом `measurements/measure_section10.py`: wall time измерялся через `time.perf_counter()` вокруг `subprocess.run()` команды CLI. В именах временных таблиц скрипт добавлял идентификатор прогона. Период измерения включает `SET SESSION`, запуск CLI, генерацию TPC-H и запись Iceberg, но исключает проверку количества строк и удаление копии.

### 7.2. Чтение и агрегация

Использовалась агрегация из §7 гайда:

```bash
time docker exec trino trino --execute \
  "SELECT returnflag, linestatus,
          count(*) AS cnt,
          sum(quantity) AS sum_qty,
          sum(extendedprice) AS sum_price,
          sum(discount) AS sum_disc,
          sum(extendedprice * (1 - discount)) AS sum_charge
   FROM bench.lakehouse.lineitem_parquet_zstd
   GROUP BY returnflag, linestatus;"
```

Для остальных вариантов менялось только имя таблицы. Каждый запрос выполнялся дважды. До первой агрегации каждой пары перезапускались сервисы:

```bash
docker restart trino lakehouse-minio
```

Скрипт ожидал готовности Trino и активного вычислительного узла. Повторная агрегация выполнялась сразу после первой. Успешность контролировалась по коду завершения CLI и суммарному количеству строк в группах: 6 001 215.

### 7.3. Что означают метрики

| Метрика | Как получена | Единица |
|---|---|---|
| Размер | `sum(file_size_in_bytes)` из `$files` | байт, ГиБ |
| Число файлов | `count(*)` из `$files` | файлы |
| Время записи | wall time команды с CTAS | с |
| Cold | время первой агрегации после перезапуска сервисов | с |
| Warm | время немедленного повторного выполнения | с |
| Экономия места | `(1 − размер_сжатого / размер_NONE) × 100` | % |

`Cold` — условное обозначение по гайду: перезапуск контейнеров не гарантирует очистку page cache ОС. Все времена включают запуск CLI. Для каждой комбинации получен один замер записи и одна пара чтений; это ограничивает точность сравнения.

## 8. Сводная таблица §10

| Вариант | Размер, ГиБ | Файлов | Время записи, с | Агрегация cold, с | Агрегация warm, с |
|---|---:|---:|---:|---:|---:|
| parquet_none | 0,3494 | 4 | 167,844 | 2,696 | 4,088 |
| parquet_snappy | 0,1926 | 4 | 88,188 | 6,186 | 10,168 |
| parquet_gzip | 0,1393 | 4 | 141,611 | 2,377 | 2,642 |
| parquet_zstd | 0,1427 | 4 | 63,306 | 2,489 | 1,899 |
| orc_none | 0,4215 | 4 | 76,782 | 3,211 | 3,363 |
| orc_snappy | 0,2062 | 4 | 77,938 | 2,751 | 2,708 |
| orc_gzip | 0,1451 | 4 | 71,412 | 3,137 | 1,824 |
| orc_zstd | 0,1501 | 4 | 59,083 | 2,827 | 2,201 |

Результаты сохранены в `measurements/20261013-104905/section10.csv`, пояснения — в `section10.md`, исходные SQL и выводы — в `.log`-файлах той же папки.

### 8.1. Экономия места относительно NONE

| Формат | SNAPPY | GZIP | ZSTD |
|---|---:|---:|---:|
| PARQUET | 44,88% | 60,14% | 59,16% |
| ORC | 51,08% | 65,57% | 64,39% |

Самые компактные таблицы получились с GZIP. ZSTD близок к нему по размеру. При каждом кодеке Parquet занимал меньше места, чем ORC.

### 8.2. Время записи и чтения

В этой серии минимальное время записи у ZSTD: 63,306 с для Parquet и 59,083 с для ORC. Для Parquet GZIP получено 141,611 с, NONE — 167,844 с: GZIP в этом запуске оказался примерно на 15,6% быстрее NONE. Для ORC соответствующие времена — 71,412 и 76,782 с, разница около 7,0%. Поэтому предположение из гайда, что GZIP обязательно будет медленнее NONE, мои одиночные замеры не подтвердили.

Первый Parquet NONE измерялся на ранее работавшем экземпляре Trino, а следующие записи — после выполненных между вариантами перезапусков и чтений. Во время длинного первого CTAS дополнительно выполнялся диагностический запрос к состоянию Trino. Эти различия, ограниченная память Docker и фоновые процессы могли влиять на время; для устойчивого ранжирования нужна повторная серия с одинаковым прогревом.

Самый быстрый первый запуск агрегации в серии — Parquet GZIP, 2,377 с; самый быстрый повтор — ORC GZIP, 1,824 с. По Parquet ZSTD повтор ускорился с 2,489 до 1,899 с, приблизительно в 1,31 раза. По ORC GZIP — с 3,137 до 1,824 с, приблизительно в 1,72 раза. У некоторых вариантов повтор оказался медленнее. Следовательно, эффект кэширования по одной паре измерений нельзя отделить от других факторов нагрузки.

## 9. Статистика таблицы

Я выполнил:

```sql
SHOW STATS FOR bench.lakehouse.lineitem_parquet_zstd;
```

| Показатель | Полученное значение |
|---|---|
| Число строк, итоговый `row_count` | 6 001 215 |
| NDV `returnflag` | 3 |
| NDV `linestatus` | 2 |
| Оценка NDV `orderkey` | 1 498 948 |
| NDV `partkey` | 200 000 |
| Оценка NDV `suppkey` | 9 920 |
| NDV `linenumber` | 7 |
| Доля NULL по колонкам | 0 |
| Диапазон `orderkey` | 1–6 000 000 |
| Диапазон `quantity` | 1–50 |
| Диапазон `shipdate` | 1992-01-02–1998-12-01 |

NDV — число различных значений, представленное статистикой оптимизатора; оно может быть оценочным. `NULL` в некоторых ячейках таблицы статистики означает отсутствие конкретной оценки, а не наличие пропусков в данных.

Команды следующего упражнения §11.2 для моей таблицы:

```sql
ANALYZE bench.lakehouse.lineitem_parquet_zstd;

EXPLAIN
SELECT count(*)
FROM bench.lakehouse.lineitem_parquet_zstd
WHERE returnflag = 'A';
```

Результаты этих двух команд пока не сохранены в отчёте; приведены адаптированные команды для продолжения работы.

## 10. Манифесты и снимки Iceberg

Команды §8 для используемой таблицы:

```sql
SELECT count(*) AS manifests,
       sum(length) AS manifest_bytes
FROM bench.lakehouse."lineitem_parquet_zstd$manifests";

SELECT committed_at, snapshot_id, operation
FROM bench.lakehouse."lineitem_parquet_zstd$snapshots";
```

Результаты этих запросов пока не предоставлены. После их выполнения долю манифестов можно вычислить как `manifest_bytes / 153231705 × 100%`. Размер манифестов представляет только часть метаданных Iceberg; его нельзя выдавать за полный объём метаданных.

## 11. Продвинутая часть: конкурентные запросы

Так как исходные таблицы уже созданы на `sf1`, отдельная копия с именем `lineitem_parquet_zstd_sf1` не нужна. Во всех командах используется `bench.lakehouse.lineitem_parquet_zstd`.

### 11.1. Одиночный запуск

В терминале выполнено:

```bash
time docker exec trino trino --execute \
  "SELECT sum(extendedprice) FROM bench.lakehouse.lineitem_parquet_zstd"
```

Получены сумма `2.295773109011972E11` и полное время **4,357 с**. Одиночная пропускная способность по методике гайда: `1 / 4,357 ≈ 0,230 запроса/с`.

### 11.2 Параллельный запуск

| Метрика | Формула | Подтверждённое значение |
|---|---|---|
| Пропускная способность, 1 поток | `1 / 4,357` | 0,230 запроса/с |
| Пропускная способность, 10 потоков | `10 / t_пачки` | требуется повторный замер |
| Отношение длительности пачки к одиночному запуску | `t_пачки / 4,357` | требуется повторный замер |

Отношение времени пачки к одиночному запуску не является точным замедлением каждого отдельного запроса. Для этого нужны индивидуальные длительности.

### 11.3. Сбор времени из Trino

Адаптированный запрос §11.3:

```sql
SELECT query_id, state,
       queued_time_ms, analysis_time_ms, planning_time_ms,
       date_diff('millisecond', started, "end") AS exec_ms,
       query
FROM system.runtime.queries
WHERE query LIKE '%lineitem_parquet_zstd%'
  AND query NOT LIKE '%runtime.queries%'
ORDER BY created DESC
LIMIT 20;
```

Эти значения позволяют сопоставить серверные интервалы с полным временем CLI. История runtime не является постоянным архивом; результаты нужно сохранять после измерения. Таблица вывода этого упражнения пока не приложена.

### 11.4. Сравнение Trino и Spark

Для сравнения нужно выполнить одинаковый запрос из §11.4 на одной таблице в обоих движках:

```sql
SELECT returnflag, linestatus,
       sum(quantity) AS sum_qty,
       sum(extendedprice) AS sum_price,
       sum(extendedprice * (1 - discount)) AS sum_disc,
       count(*) AS cnt
FROM bench.lakehouse.lineitem_parquet_zstd
WHERE shipdate <= DATE '1998-12-01'
GROUP BY returnflag, linestatus
ORDER BY returnflag, linestatus;
```

Результатов Spark в материалах пока нет. Времена основной агрегации из §7 не подставляются вместо этого запроса: SQL отличается. Сравнение движков и полный набор Q1–Q22 из §13 остаются следующими этапами.

## 13. Выводы

Я настроил и использовал локальный Lakehouse, материализовал одинаковую таблицу TPC-H в восемь сочетаний формата и сжатия, проверил кодеки и собрал размеры, число файлов и времена выполнения.

По полученным данным GZIP обеспечил минимальный размер. ZSTD дал близкий размер и минимальное время записи среди вариантов каждого формата в данной серии. SNAPPY уменьшил объём относительно NONE, но его чтение в этой серии не было самым быстрым. Практический выбор кодека зависит от того, что важнее: размер хранения, скорость записи или задержка чтения. Полученные значения не подтверждают универсального превосходства одного кодека по всем критериям.

На `sf1` все таблицы содержат 6 001 215 строк, поэтому сравнение размеров выполнено на одинаковом объёме данных. Влияние ограниченных ресурсов ноутбука и условий прогрева особенно заметно во временах: повторные чтения не всегда ускорялись. Для более точных выводов нужны повторные серии при одинаковой нагрузке.

Одиночный запрос продвинутой части выполнен за 4,357 с. Замер десяти потоков требует повторения с правильным именем таблицы и контролем ошибок. Сравнение со Spark, результаты манифестов/снимков и полный TPC-H в настоящий отчёт как выполненные эксперименты не включены, поскольку подтверждённых результатов пока нет.
