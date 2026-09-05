# Example 10 - ad-hoc analytics with PrestoDB over a Hive-partitioned Parquet table

This example builds a small data lake and then asks questions of it.

A Scala 3 program generates the online shop's clickstream, writes it as Apache Parquet files laid out in the Apache
Hive partition convention (`country=DE/dt=2023-11-14/clicks.parquet`), uploads those files to MinIO (an S3-compatible
object store), registers them with PrestoDB as an **external table**, and then runs analytical SQL over them through
JDBC (Java Database Connectivity, the standard Java interface to SQL databases).

The point of the example is not "you can run SQL over files" - it is **partitioning as a performance tool**. You will
see the same `count(*)` run twice, once against the whole table and once against a single partition, and you will see
`EXPLAIN` prove why the second one reads a fraction of the bytes.

Some vocabulary first, because three names get used interchangeably in the wild and they are not the same thing.

| Name | What it is |
| --- | --- |
| **Apache Hive** | The original SQL-on-Hadoop system. Today its lasting contribution is two things: the **metastore** (a catalogue service that remembers which tables exist and where their files are) and the **partition layout** (`column=value` directory names). |
| **PrestoDB** | A distributed SQL query engine created at Facebook in 2012. It does not store data; it reads other people's files and tables. It is what this example runs. |
| **Trino** | In 2018 the original PrestoDB creators left Facebook and forked their own project as PrestoSQL, renamed **Trino** in 2020. Facebook kept the PrestoDB name under the Linux Foundation. |

### PrestoDB and Trino, honestly

They share an ancestor and most of their SQL surface. A query you write here will usually run unchanged on Trino. The
differences that actually bite:

- **Connector names.** PrestoDB's Hive connector is `hive-hadoop2`; Trino's is `hive`. This PrestoDB stack uses
  `hive.s3.*` settings. Example 09 pins Trino 476 and enables its native client with `fs.native-s3.enabled=true`,
  then configures `s3.*` properties such as `s3.endpoint`. Use the settings for the pinned engine version:
  [Trino 476 S3 configuration](https://github.com/trinodb/trino/blob/476/docs/src/main/sphinx/object-storage/file-system-s3.md).
- **Metastore protocol.** PrestoDB 0.293 speaks the Thrift metastore protocol as it was before Apache Hive 4 renamed
  its methods. Point it at a Hive 4 metastore and every query fails with `Invalid method name: 'get_table'`. That is
  why this stack runs Hive 3.1.3. Trino's client tracks the newer protocol.
- **Java version and release cadence.** Trino releases roughly weekly and moves to new Java versions quickly;
  PrestoDB releases less often and stays on older ones longer.
- **Feature drift.** Trino has moved further on the newer table formats (Apache Iceberg, Delta Lake) and on fault
  tolerant execution; PrestoDB has invested in its native C++ worker, Velox.

This repository includes both engines so you can compare their connectors, configuration, query plans, and
operational behavior against concrete examples.

## The use case

The analytics team of the online shop wants two answers, today, without waiting for a data engineer to build a
pipeline:

1. **A funnel.** Of the shopping sessions that started on `/home`, how many went on to `/search`, then `/product`,
   then `/cart`, then `/checkout`? Where does the biggest drop happen?
2. **A per-country conversion report.** The shop runs five storefronts - Germany, Poland, Ukraine, France and Spain.
   What share of each market's shopping sessions ends in a checkout?

The clickstream is the same `de.common.domain.ClickEvent` every other example in this repository uses, produced by the
same seeded `de.common.gen.DataGenerator`. That means the numbers below are reproducible: re-running the loader
produces byte-for-byte the same files.

## How it works

### The pieces on disk

```
examples/10-presto-hive-analytics/
├── package.mill                        the Mill build definition
├── src/de/presto/hive/
│   ├── HivePartition.scala             what a partition is and how it becomes a directory name
│   ├── Clickstream.scala               shared-domain events -> table rows, and their partitions
│   ├── ParquetClickstreamWriter.scala  writes local Parquet files in the Hive layout
│   ├── ObjectStore.scala               uploads that directory tree to MinIO
│   ├── HiveSql.scala                   every SQL statement the example sends
│   ├── PrestoClient.scala              a thin JDBC wrapper that also captures query statistics
│   ├── Reports.scala                   turns query results into console tables
│   └── Main.scala                      the wiring: generate, write, upload, register, analyse
├── test/src/de/presto/hive/            unit tests; none of them needs Docker
└── docker/
    ├── docker-compose.yml              MinIO, PostgreSQL, Hive metastore, PrestoDB
    ├── hive/hive-site.xml              metastore configuration
    └── presto/                         PrestoDB server and catalog configuration
```

### Step 1: rows and partitions (`Clickstream.scala`, `HivePartition.scala`)

A `ClickEvent` carries a customer, a page, an optional product code and a timestamp - but no country, because a click
on a web page does not know one. In a real shop the storefront resolves the country before the event reaches the lake,
so `Clickstream.countryOf` does the same thing here, with a stable hash of the customer identifier. Stable matters: a
customer who appeared to hop between countries mid-session would corrupt the funnel.

`HivePartition` is the whole partitioning idea in one small type. It renders `country=DE/dt=2023-11-14` and it renders
the matching SQL predicate `country = 'DE' AND dt = '2023-11-14'`, from the same two fields, so the directory and the
query can never disagree. It computes the calendar day in UTC (Coordinated Universal Time), never in the local time
zone, so that the same event lands in the same partition no matter where the job runs.

### Step 2: writing Parquet (`ParquetClickstreamWriter.scala`)

Parquet is a columnar file format: values of one column are stored together, so a query that reads three of six
columns reads roughly half the file. The writer uses `parquet-avro` directly. Apache Spark is deliberately absent -
this example is about the storage layout and the query engine, and a Spark cluster would hide both behind an API.

Two decisions in this file are worth knowing about:

- **The partition columns are not in the files.** `country` and `dt` live in the directory name and nowhere else.
  Writing them into the file too would waste space and let the two copies disagree.
- **Checksums are switched off.** Hadoop's default local filesystem writes a hidden `.crc` file next to every file it
  creates. Uploaded to object storage, those sidecars would sit inside a partition directory, where the Hive connector
  treats every object as data. `RawLocalFileSystem` is the same filesystem without them.

The writer targets the local disk, and uploading is a separate step. That split is what lets the tests point the
writer at a temporary directory and read the results back with no containers running.

### Step 3: uploading (`ObjectStore.scala`)

An object store has no directories, only keys. But a key containing `clickstream/country=DE/dt=2023-11-14/clicks.parquet`
is treated by every Hive-compatible engine exactly as if those slashes were directories. The uploader therefore does
nothing clever: it walks the local tree and preserves the relative paths as object keys. It clears the prefix first,
so that re-running the loader replaces the data rather than doubling it.

### Step 4: registering the table (`HiveSql.scala`, `Main.scala`)

Three statements turn a pile of files into a table:

```sql
CREATE TABLE hive.shop.clickstream (
  customer_id varchar, page varchar, sku varchar, occurred_at bigint,
  country varchar, dt varchar
) WITH (
  format = 'PARQUET',
  external_location = 's3a://lake/clickstream',
  partitioned_by = ARRAY['country', 'dt']
);

CALL hive.system.sync_partition_metadata('shop', 'clickstream', 'FULL');

ANALYZE hive.shop.clickstream;
```

- `external_location` means PrestoDB only *describes* the files; dropping the table deletes nothing.
- The partition columns must come **last** in the column list, in partition order. That is a Hive rule, not a
  PrestoDB one.
- `sync_partition_metadata` is the step people forget. A freshly created external table has zero partitions
  registered and therefore reads as completely empty, however many files are in storage. This procedure walks the
  location, finds the `country=.../dt=...` directories and registers them.
- `ANALYZE` collects row counts, distinct-value counts and value ranges. Partition pruning does **not** need it -
  pruning only reads directory names. Statistics matter for everything the planner decides afterwards, and for the
  row-count estimates `EXPLAIN` prints.

### Step 5: proving that pruning happens (`PrestoClient.scala`)

The example runs the same count twice and prints both plans. In the plan without a partition predicate the scan is
unconstrained; in the pruned plan the scan node carries the partition constraint:

```
TableScan[... layout='Optional[shop.clickstream{domains={country=[ [["DE"]] ], dt=[ [["2023-11-15"]] ]}}]']
country:string:-13:PARTITION_KEY
dt:string:-14:PARTITION_KEY
```

`PARTITION_KEY` is PrestoDB saying "this column's value comes from the path". Because it does, the connector can
discard whole directories before opening a single file.

The plan is an argument; the bytes are the proof. The PrestoDB JDBC driver can report per-query statistics through a
progress monitor, and `PrestoClient` captures `getProcessedBytes` from it. The report then prints both figures and the
ratio between them - typically around a twenty-fold reduction here, because the data spans roughly twenty partitions.

### Step 6: the funnel (`HiveSql.funnel`)

The funnel is where window functions earn their keep. A window function computes a value for each row *from a set of
related rows*, without collapsing them the way `GROUP BY` does. The query does it in four stages:

1. `visits` reads the rows. The `dt IN (...)` predicate here is what prunes partitions.
2. `gapped` marks the first click of each session. `LAG(occurred_at) OVER (PARTITION BY customer_id ORDER BY
   occurred_at)` is "the previous click by this same customer"; a gap of more than thirty minutes starts a new
   session. There is no way to express this with plain aggregation.
3. `sessions` turns those markers into a session number with a running `SUM(...) OVER (... ROWS BETWEEN UNBOUNDED
   PRECEDING AND CURRENT ROW)`. The frame is spelled out because SQL's default frame is `RANGE`, which would lump
   together clicks that happen to share a timestamp.
4. `stage_times` collapses each session to the first time it touched each page, with `MIN(...) OVER (PARTITION BY
   customer_id, session_number)`.

The final `SELECT` counts sessions per step, requiring each step to have happened strictly *after* the previous one -
a visitor who lands on `/checkout` and then wanders to `/search` did not go through the funnel.

### Step 7: the conversion report (`HiveSql.conversionByCountry`)

The conversion report reuses the same three sessionization stages and then groups by `country`. Grouping by `country`
is free: it is a partition column, so PrestoDB already knows which value every row has from the directory it came
from, without reading anything from inside a file.

It counts **sessions**, not people, and that is a deliberate correction. Counting distinct customers in a shop with a
thousand regulars reports that a hundred percent of them converted, which is true and useless: given enough visits,
everybody buys something eventually. A conversion rate is a question about visits.

## Run it

Everything below is run from the repository root.

### 1. Start the stack

```bash
docker compose -f examples/10-presto-hive-analytics/docker/docker-compose.yml up -d
```

Four services come up in order, each waiting for the previous one to report healthy. First start pulls roughly six
gigabytes of images and takes a few minutes; after that it is under a minute.

| Service | Host port | What it is |
| --- | --- | --- |
| MinIO (S3 API) | `11000` | where the Parquet files live |
| MinIO console | `11001` | web user interface, `minioadmin` / `minioadmin` |
| PostgreSQL | `11032` | the Hive metastore's own database |
| Hive metastore | `11083` | Thrift catalogue service |
| PrestoDB | `11080` | JDBC endpoint and web user interface |

### 2. Run the tests (no Docker needed)

```bash
./mill examples.10-presto-hive-analytics.test
```

### 3. Load the data and run the analysis

```bash
./mill examples.10-presto-hive-analytics.run
```

Expect this output (the generator is seeded, so these are the actual numbers):

```
generated 600000 clickstream events
uploaded 20 objects to s3a://lake/clickstream
wrote 20 Parquet files covering 4 days and 5 markets
registered hive.shop.clickstream and collected statistics

=== EXPLAIN without a partition predicate ===
... TableScan[... layout='Optional[shop.clickstream{}]'] ...
        Estimates: {source: CostBasedSourceInfo, rows: 600,000 (5.15MB), ...}
        country:string:-13:PARTITION_KEY
            :: [["DE"], ["ES"], ["FR"], ["PL"], ["UA"]]
        dt:string:-14:PARTITION_KEY
            :: [["2023-11-14"], ["2023-11-15"], ["2023-11-16"], ["2023-11-17"]]

=== EXPLAIN with the partition predicate country=DE/dt=2023-11-16 ===
... TableScan[... layout='Optional[shop.clickstream{domains={country=[ [["DE"]] ], dt=[ [["2023-11-16"]] ]}}]'] ...
        Estimates: {source: CostBasedSourceInfo, rows: 34,611 (304.20kB), ...}
        country:string:-13:PARTITION_KEY
            :: [["DE"]]
        dt:string:-14:PARTITION_KEY
            :: [["2023-11-16"]]

=== bytes read for the same count ===
whole table:   600000 events
one partition: 34611 events
query          bytes read
-------------  ----------
whole table    320.00 KiB
one partition  16.00 KiB
20.0 x less data read

=== funnel: /home -> /search -> /product -> /cart -> /checkout ===
step       sessions  of entered  of previous
---------  --------  ----------  -----------
/home      15512     100.0%      100.0%
/search    7253      46.8%       46.8%
/product   2291      14.8%       31.6%
/cart      533       3.4%        23.3%
/checkout  95        0.6%        17.8%

=== conversion by country ===
country  sessions  purchases  conversion
-------  --------  ---------  ----------
UA       3519      3173       90.2%
PL       3412      3075       90.1%
ES       3443      3088       89.7%
DE       3501      3135       89.5%
FR       3475      3092       89.0%
```

Read the twenty-fold reduction in the middle block next to the two plans above it. The unpruned scan lists all five
countries and all four days as candidate partitions; the pruned one lists exactly one of each, and reads a twentieth
of the bytes to produce the same count.

Two things about these numbers are worth being honest about. Both come from the shape of the generated data, not from
anything PrestoDB did.

- **The conversion rates are absurdly high.** `DataGenerator` picks each page uniformly at random, so a session of
  thirty-odd clicks almost always contains a `/checkout` somewhere. A real shop converts a few percent. The funnel,
  which insists the pages be visited *in order*, is the more realistic of the two reports.
- **The five countries look identical.** Customers are assigned to storefronts by a hash, and the generator treats all
  customers the same, so there is no real per-market difference to find. The query is what matters here, not the
  finding.

### 4. Look around

Open the PrestoDB web user interface at <http://localhost:11080> to see each query, its plan, and how many bytes and
rows it read. Open the MinIO console at <http://localhost:11001> to browse the `lake` bucket and see the
`country=.../dt=.../` directory tree the loader created.

You can also query interactively from inside the container:

```bash
docker exec -it de-10-presto presto-cli --server localhost:8080 --catalog hive --schema shop
```

```sql
-- PrestoDB has no `SHOW PARTITIONS`. Every Hive table instead gets a companion
-- table named `<table>$partitions` that lists one row per registered partition.
-- The double quotes are required: `$` is not a normal identifier character.
SELECT * FROM "clickstream$partitions" ORDER BY country, dt;

SELECT country, count(*) FROM clickstream GROUP BY country;
```

## What to try next

- **Remove the pruning and watch the cost.** Run
  `SELECT count(*) FROM clickstream WHERE substr(dt, 1, 10) = '2023-11-15'`. Wrapping the partition column in a
  function hides it from the planner, so the plan loses its partition constraint and the query reads the whole table
  for the same answer. This is the single most common way real queries accidentally become slow.
- **Forget to sync.** Drop the table, recreate it, and query it *without* calling `sync_partition_metadata`. It
  returns zero rows even though every file is still there. Then call the procedure and query again.
- **Change what you partition by.** Set `EVENT_COUNT=2000000` and edit `HivePartition` to partition by
  `country` only. Fewer, larger partitions means less metadata but coarser pruning; partitioning by hour instead of
  day means the opposite, and at some point the metastore lookups cost more than the scan they save. Somewhere around
  a few hundred megabytes per partition is the usual sweet spot.
- **Skip the ANALYZE.** Drop and recreate the table without running `ANALYZE`, then compare the row-count estimates in
  the `EXPLAIN` output. Pruning still works; the estimates become guesses.
- **Point Trino at the same data.** The files, the layout and the metastore are all standard. Starting a Trino
  container against the same metastore and bucket is a good way to feel exactly how similar - and how different - the
  two engines are.

Environment variables let you point the program somewhere else without editing code: `PRESTO_URL`, `PRESTO_USER`,
`S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_PREFIX`, `EVENT_COUNT`, `GENERATOR_SEED` and
`GENERATOR_START_MILLIS`.

## Clean up

```bash
docker compose -f examples/10-presto-hive-analytics/docker/docker-compose.yml down -v
```

The `-v` removes the volumes as well, so the next run starts from an empty bucket and an empty metastore database.
