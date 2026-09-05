# 07 - A Delta Lake lakehouse on Apache Spark

This example builds a small **lakehouse**: a set of analytics tables that live as plain files in object
storage, but behave like database tables. It uses **Apache Spark** (a distributed engine for processing
large data sets) to do the computing and **Delta Lake** (a storage format that adds a transaction log on
top of Apache Parquet files) to make the tables trustworthy. The tables are laid out as the **medallion
architecture** - bronze, then silver, then gold - and the job then walks through the Delta Lake features
that a directory of plain Parquet files cannot offer: atomic transactions, `MERGE INTO` upserts, schema
enforcement and schema evolution, time travel, `OPTIMIZE`, `VACUUM`, and the transaction log itself.

Nothing here is a toy format: the same code, unchanged, writes to a local folder on your laptop and to
S3-compatible object storage in a Spark cluster.

## The use case

The shared online-shop domain of this repository (see `common/src/de/common/domain`) produces three feeds:
orders, payments and shipments. Analysts want two numbers from them - **daily revenue per country** and
**customer lifetime value** - and they want the numbers to be correct even though the feeds are messy.

The medallion architecture is the usual way to get there. Each layer has one job:

| Layer      | Contents                                          | Why it exists                                                                   |
| ---------- | ------------------------------------------------- | ------------------------------------------------------------------------------- |
| **bronze** | the raw feeds, exactly as they arrived            | it is the replayable record of what the source systems sent, duplicates included |
| **silver** | cleaned, de-duplicated, conformed tables          | one row per business key, validated values, tidy column names                    |
| **gold**   | business aggregates                               | the numbers a dashboard or a finance report reads                                |

The messiness is deliberate. The generated feeds contain retried orders that landed twice and a few rows
with an empty customer identifier, so the silver layer has something real to clean up.

Alongside the fact tables the job maintains a **customer dimension**: a lookup table of who each customer
is right now. It is kept up to date with `MERGE INTO` rather than being rewritten, which is what a
**slowly changing dimension** means in practice.

## How it works

The code is split so that the interesting part needs neither storage nor a cluster to test.

```
src/de/spark/lakehouse/
  core/                       pure logic - no file paths, no credentials, no Delta Lake
    BronzeRecords.scala       flattens the shared domain objects into table-shaped rows
    MedallionTransforms.scala every business rule, as DataFrame -> DataFrame functions
    LakehouseLayout.scala     where each table lives under one warehouse root
  job/                        wiring - everything that talks to the outside world
    JobConfig.scala           reads the environment, or falls back to a local folder
    SparkSessions.scala       builds the Spark session and switches Delta Lake on
    DeltaLakehouse.scala      every Delta Lake call the example makes, in one class
    SourceData.scala          stands in for the operational systems that feed bronze
    Main.scala                runs the eight steps and narrates what happened
resources/
  log4j2.properties         quietens Spark's INFO logging so the narration is readable
docker/
  docker-compose.yml        MinIO plus a one-master, one-worker Spark cluster
```

### The rule that makes this testable

`MedallionTransforms` contains only functions that take `DataFrame`s and return `DataFrame`s. They never
open a file, never read an environment variable and never mention Delta Lake. That is why
`MedallionTransformsSuite` can check the revenue calculation against six rows built in memory, in under a
second, with no container running.

The parts that *must* touch storage live in `DeltaLakehouse`, and `DeltaLakehouseSuite` exercises them
against a temporary directory. Delta Lake does not care whether a table path is a local folder or an S3
bucket, so those tests cover the object-storage behaviour too.

### The Delta Lake features, and where to find them

| Feature                | Where                                          | What it buys you                                                                                |
| ---------------------- | ---------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| ACID (atomic, consistent, isolated, durable) transactions | `DeltaLakehouse.append` / `overwrite`          | a write is all-or-nothing; readers never see a half-written table                                |
| `MERGE INTO`           | `DeltaLakehouse.mergeCustomerDimension`        | update-or-insert in one transaction, instead of read-join-rewrite                                 |
| Schema enforcement     | the plain `append` in step 5                   | a write that introduces an unexpected column is rejected, not silently absorbed                   |
| Schema evolution       | `appendWithSchemaEvolution`                    | the same change is accepted when you ask for it with `mergeSchema`                                |
| Time travel            | `readAtVersion` / `readAtTimestamp`            | read the table as it was at commit 3, or as it was this morning                                   |
| `OPTIMIZE`             | `DeltaLakehouse.optimize`                      | many small files are compacted into few large ones, so queries stop paying to open them           |
| `VACUUM`               | `DeltaLakehouse.vacuum`                        | data files no longer referenced by a retained version are deleted                                 |
| The transaction log    | `DeltaLakehouse.transactionLogFiles`           | the `_delta_log` directory that turns a pile of Parquet files into a table                        |

Two details are worth calling out because they bite people.

**Time zones.** `MERGE INTO` and time travel both compare timestamps, and a `java.sql.Timestamp` prints
itself in the time zone of the machine while Spark parses `timestampAsOf` in the *session* time zone. Hand
one to the other and you ask for a version that does not exist yet. `DeltaLakehouse.commitTimestampOf`
formats the timestamp inside Spark to keep the two in step, and the whole job runs with
`spark.sql.session.timeZone` pinned to Coordinated Universal Time (UTC).

**`VACUUM` retention.** The default is seven days, and lowering it can break a query that is still
running. Delta Lake therefore refuses a shorter retention unless a safety check is switched off
explicitly. The example switches it off for the demonstration and restores it immediately afterwards;
production should leave the default alone.

## Run it

Everything below is run from the repository root.

### The quick path: no infrastructure at all

The job defaults to a warehouse in a local folder, so it needs nothing running:

```bash
./mill examples.07-spark-delta-lakehouse.test    # the unit tests, no docker needed
./mill examples.07-spark-delta-lakehouse.run     # the whole pipeline, into out/lakehouse-07
```

The run prints eight sections. Expect roughly this shape (exact numbers depend on the generated batch):

```
Step 1 of 8 - bronze: land the raw feeds
orders    420 rows -> out/lakehouse-07/bronze/orders
...
Step 2 of 8 - silver: clean, de-duplicate and conform
orders    392 rows -> out/lakehouse-07/silver/orders
...
Step 5 of 8 - schema enforcement and schema evolution
schema enforcement rejected a write that introduced an unexpected column:
  A schema mismatch detected when writing to the Delta table. ...
...
Step 8 of 8 - what is actually on disk
out/lakehouse-07/bronze/orders/_delta_log contains:
  00000000000000000000.json
  00000000000000000001.json
  ...
```

Afterwards, look at what was written:

```bash
find out/lakehouse-07/bronze/orders -maxdepth 1
cat out/lakehouse-07/bronze/orders/_delta_log/00000000000000000000.json | head
```

That JavaScript Object Notation (JSON) file is the entire trick: a list of which Parquet files the table consisted of after commit
zero.

### The full path: object storage and a real Spark cluster

Start MinIO (an S3-compatible object store) and a one-master, one-worker Spark cluster:

```bash
docker compose -f examples/07-spark-delta-lakehouse/docker/docker-compose.yml up -d --wait
```

| Service      | Address                                            |
| ------------ | -------------------------------------------------- |
| MinIO console | <http://localhost:10701> (`minioadmin`/`minioadmin`) |
| MinIO S3 application programming interface | <http://localhost:10700>                            |
| Spark master  | <http://localhost:10780>                            |
| Spark worker  | <http://localhost:10781>                            |
| Cluster address for `spark-submit` | `spark://localhost:10707`         |

An init container has already created the `lakehouse` bucket.

Build the two jars the cluster needs - the example itself and the shared domain module - and submit the
job. The compose file mounts the repository at `/workspace` inside the containers, so the jars are visible
without copying them anywhere. The warehouse location and the object store credentials are passed as
environment variables, which is what `JobConfig` reads:

```bash
./mill examples.07-spark-delta-lakehouse.jar
./mill 'common[2.13.18].jar'

# Both jars are called out.jar, and Spark refuses to register two files with the
# same name, so give them distinct names first.
mkdir -p out/dist-07
cp out/common/2.13.18/jar.dest/out.jar               out/dist-07/de-common.jar
cp out/examples/07-spark-delta-lakehouse/jar.dest/out.jar out/dist-07/de-07-lakehouse.jar

docker exec \
  -e LAKEHOUSE_ROOT=s3a://lakehouse/warehouse \
  -e S3_ENDPOINT=http://minio:9000 \
  -e S3_ACCESS_KEY=minioadmin \
  -e S3_SECRET_KEY=minioadmin \
  de-07-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class de.spark.lakehouse.job.Main \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1 \
  --jars /workspace/out/dist-07/de-common.jar \
  /workspace/out/dist-07/de-07-lakehouse.jar
```

`spark.jars.ivy` matters because the Spark image runs as a user with no home directory, and the resolver
behind `--packages` needs somewhere writable to download into. `--packages` itself is how a Spark cluster
picks up libraries it does not ship with: Delta Lake itself, and the
Hadoop S3A filesystem that teaches Spark to read and write `s3a://` paths. The first submit spends a
minute downloading them; later submits inside the same container reuse the download.

Watch the job in the Spark master user interface at <http://localhost:10780>, then browse the resulting
files - including the `_delta_log` directories - in the MinIO console at <http://localhost:10701>.

You can also point the local `./mill run` at MinIO without the cluster, which is handy while developing:

```bash
LAKEHOUSE_ROOT=s3a://lakehouse/warehouse \
S3_ENDPOINT=http://localhost:10700 \
./mill examples.07-spark-delta-lakehouse.run
```

Note that this needs `hadoop-aws` on the classpath; it is not a dependency of the Mill module, because the
local default warehouse is a plain folder and pulling in the Amazon software development kit (SDK) for every developer would be a
large download for nothing. Use the `spark-submit` path above when you want object storage.

## What to try next

- **Break schema enforcement on purpose.** In `Main.demonstrateSchemaRules`, change `lit("mobile-app")` to
  `lit(42)` and rerun. Delta Lake now rejects the write for a different reason - a type conflict on an
  existing column is not something `mergeSchema` will paper over.
- **Watch a version disappear.** Run the job twice, note the version numbers printed in step 6, then run
  `VACUUM` with a retention of `0` a second time and try `readAtVersion(path, 0)` again. Time travel to a
  version whose data files were vacuumed fails - which is exactly why the default retention is seven days.
- **Change the merge condition.** Remove
  `"incoming.last_seen_epoch_millis >= current.last_seen_epoch_millis"` from
  `DeltaLakehouse.mergeCustomerDimension` and rerun `DeltaLakehouseSuite`. The test that replays an old
  batch now fails, showing what the guard was protecting.
- **Turn the dimension into type 2.** Instead of updating the row in place, add `valid_from` and
  `valid_to` columns and use `whenMatched().updateExpr(...)` to close the old row plus a
  `whenNotMatched().insertAll()` to open a new one. That is a slowly changing dimension of type 2, and it
  keeps the history in the table rather than only in the log.
- **Kill the worker.** `docker stop de-07-spark-worker` in the middle of a submit. The master will report
  the lost executor; restart it with `docker start de-07-spark-worker` and watch Spark re-run the lost
  tasks. Because every Delta write is a single commit, the table never ends up half written.

## Clean up

```bash
docker compose -f examples/07-spark-delta-lakehouse/docker/docker-compose.yml down -v
```

The local warehouse, if you ran the quick path, is just a folder:

```bash
rm -rf out/lakehouse-07
```
