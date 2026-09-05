# Data engineering by example

Sixteen realistic data engineering projects, each one runnable on a laptop with two
commands, built as a Mill monorepo with mixed Scala versions and one Java module:

- **Kafka** family: plain client, Streams, ksqlDB, and CMAK-backed operations
- **Streaming at scale**: Apache Flink and Apache Spark
- **Lakehouse + catalogs**: Delta Lake, Apache Trino, Presto, Apache Hive metadata, and
  object-storage-backed storage
- **Columnar/data formats**: Apache Parquet, Apache Arrow, and Polars interoperability
- **Graph + document streams**: Apache HugeGraph and Apache CouchDB CDC
- **Notebook + query surface**: Apache Zeppelin with Spark + Trino

The repository is meant to be used as reference material: when you need to
remember how Apache Kafka transactions are set up, how a Delta Lake `MERGE`
is written, or what partition pruning looks like in a query plan, open the
example, read its README, and run it.

Everything is one [Mill](https://mill-build.org) build. Nothing has to be
installed except a Java Development Kit (JDK) and Docker.

## Quick start

```bash
git clone git@github.com:w0rxbend/data-engineering.git
cd data-engineering

./mill __.test                                  # compile and test every module
./mill examples.11-parquet-arrow-toolkit.run    # run one example end to end; needs no Docker
```

The first `./mill` call downloads Mill itself and then the dependencies, which
takes a few minutes. After that it is fast.

A short portfolio page with the module map is maintained at
[examples/README.md](examples/README.md).

There is also a local helper for repetitive commands:

```bash
scripts/run-example.sh up 09-trino-lakehouse-sql
scripts/run-example.sh test 09-trino-lakehouse-sql
scripts/run-example.sh down 09-trino-lakehouse-sql
```

Every example has its own Docker stack:

```bash
docker compose -f examples/01-kafka-clients-exactly-once/docker/docker-compose.yml up -d --wait
# ... work through the example's README ...
docker compose -f examples/01-kafka-clients-exactly-once/docker/docker-compose.yml down -v
```

## The examples

| # | Example | Technology | Scala | Host ports |
|---|---------|-----------|-------|------------|
| 01 | [Exactly-once payment settlement](examples/01-kafka-clients-exactly-once/) | Apache Kafka producer/consumer, transactions | 3 | 10180, 10192 |
| 02 | [Card-testing fraud detection](examples/02-kafka-streams-fraud/) | Kafka Streams, windows, state stores | 3 | 10280, 10292 |
| 03 | [An operations dashboard in SQL](examples/03-ksqldb-orders/) | ksqlDB, push and pull queries | 3 | 10388, 10392 |
| 04 | [A custom archive partitioner](examples/04-kafka-connect-s3-partitioner/) | Kafka Connect plugin, MinIO | Java 17 | 10483, 10490-10492 |
| 05 | [Streaming Kafka into object storage](examples/05-flink-kafka-s3sink/) | Apache Flink, `FileSink`, checkpoints | 2.12 | 10581, 10590-10592 |
| 06 | [Delivery SLA monitoring](examples/06-flink-cep-shipment-sla/) | Flink CEP, event-time patterns | 2.12 | 10680, 10681, 10692 |
| 07 | [A medallion lakehouse](examples/07-spark-delta-lakehouse/) | Apache Spark, Delta Lake, MinIO | 2.13 | 10700, 10701, 10707, 10780, 10781 |
| 08 | [Streaming ingestion into Delta](examples/08-spark-streaming-kafka-delta/) | Spark Structured Streaming, Delta | 2.13 | 10880, 10890-10892 |
| 09 | [One query over two systems](examples/09-trino-lakehouse-sql/) | Trino, Delta and PostgreSQL connectors | 3 | 10900, 10901, 10932, 10980 |
| 10 | [Partition pruning made visible](examples/10-presto-hive-analytics/) | PrestoDB, Hive-partitioned Parquet | 3 | 11000, 11001, 11032, 11080, 11083 |
| 11 | [The formats themselves](examples/11-parquet-arrow-toolkit/) | Apache Parquet, Apache Arrow | 3 | 11100, 11101 |
| 12 | [Scala to Polars without copying](examples/12-polars-arrow-bridge/) | Polars, Arrow inter-process format | 3 | none |
| 13 | [Operating a Kafka cluster](examples/13-cmak-kafka-ops/) | CMAK, `AdminClient`, three brokers | 3 | 11301-11303, 11380, 11381 |
| 14 | [Finding fraud rings](examples/14-hugegraph-fraud-ring/) | Apache HugeGraph, Gremlin | 3 | 11400, 11401 |
| 15 | [Change data capture](examples/15-couchdb-changes-to-kafka/) | Apache CouchDB `_changes`, Kafka | 3 | 11580, 11592, 11598 |
| 16 | [Notebooks over the lakehouse](examples/16-zeppelin-notebooks/) | Apache Zeppelin, Spark, Trino | 3 | 11600, 11601, 11640, 11680, 11690 |

Each example folder contains a `README.md` with the full walkthrough: what it
showcases, how it works file by file, the exact commands to run it, what the
output should look like, experiments to try, and how to clean up.

That `examples/` README also includes a condensed portfolio matrix by use case and
technology in case you are selecting by target skill, not by example number.

### Suggested reading orders

* **New to streaming:** 01 → 02 → 03 → 05 → 06 → 08
* **Building a lakehouse:** 11 → 07 → 09 → 10 → 16
* **Running the platform:** 13 → 04 → 15

## How the repository is organised

```
build.mill                     versions and the traits every module shares
common/                        the shared online-shop domain, cross-compiled for Scala 2.12, 2.13 and 3
examples/<nn>-<name>/
    package.mill               the module definition
    src/                       main sources
    test/src/                  tests, none of which need Docker
    docker/docker-compose.yml  the stack for this example only
    README.md                  the walkthrough
```

### One domain everywhere

`common/` defines the vocabulary all sixteen examples speak: `Order`,
`OrderLine`, `Payment`, `Shipment`, `ClickEvent`, `Money`, and a seeded
`DataGenerator` that produces reproducible events. Two runs with the same seed
produce byte-identical data, which is what makes the examples testable.

The module deliberately has no third-party dependencies. It is compiled for
three Scala versions at once - Apache Flink publishes its Scala API for 2.12
only, Apache Spark for 2.13, everything else here is Scala 3 - and a shared
library would have to support all three. For the same reason it carries a
small hand-written JSON writer instead of a JSON library; examples that also
have to *parse* JSON pick their own.

### Three Scala versions, one build

`build.mill` pins them and defines the shared traits:

* `BaseScalaModule` - Java 17 target, common compiler options, formatting
* `BaseJavaModule` - the same for the one Java module (example 04)
* `MunitTests` - the MUnit test framework version

An example that needs a different Scala version simply says so:

```scala
object `package` extends BaseScalaModule {
  def scalaVersion = Versions.scala213
  def moduleDeps = Seq(build.common(Versions.scala213))
}
```

## Conventions

**Host ports.** Example `NN` may only publish host ports `1NN00`-`1NN99`.
Example 03 uses 10300-10399, example 14 uses 11400-11499. Every stack can
therefore run at the same time without colliding, and a port number tells you
which example owns it.

**Pinned images.** No Compose file uses the `latest` tag. Every image is
pinned to a version that was verified to exist and to start.

**Tests never need Docker.** Domain logic is kept apart from the wiring to
Kafka, Flink or Spark, so `./mill __.test` runs green on a machine with no
containers at all. The Docker stacks are for running the examples end to end.

**Formatting.** `./mill mill.scalalib.scalafmt/reformatAll` formats
everything; `checkFormatAll` verifies it.

## Prerequisites

* A JDK. Mill downloads the exact one each module needs (Temurin 17), so any
  reasonably recent JDK is enough to start it.
* Docker with the Compose plugin (`docker compose version`).
* Around 8 GB of free memory for the heavier stacks (07, 08, 09, 16) and
  roughly 20 GB of disk for images if you run all of them.

## Useful Mill commands

| Command | What it does |
|---------|--------------|
| `./mill resolve __` | list every module and task |
| `./mill examples.07-spark-delta-lakehouse.compile` | compile one module |
| `./mill __.test` | run every test in the repository |
| `./mill examples.05-flink-kafka-s3sink.assembly` | build a single runnable jar |
| `./mill mill.scalalib.scalafmt/reformatAll` | format all sources |
| `./mill shutdown` | stop the background Mill daemon |

Mill serialises builds behind a single lock, so a second `./mill` command
started while the first is still running waits its turn. When an example asks
you to run two programs at once, start the second one from a jar built with
`assembly`, or wait for the first command to finish.

## Licence

The Kafka Connect partitioner in example 04 is derived from prior work by Can
Elmas and keeps its original Apache-2.0 licence and attribution. Everything
else in this repository is provided as reference material for learning.
