# Data engineering by example

Sixteen runnable reference projects covering Kafka, Flink, Spark, lakehouse storage,
federated SQL, columnar formats, graph analytics, change data capture, and notebooks.
They share one [Mill](https://mill-build.org/) build and one small online-shop
domain, while keeping the Scala and Java versions required by each engine.

This is a learning and reference corpus, not a production platform template. Each
example has focused tests, an isolated Docker Compose stack, and a README that
explains the design and the operational trade-offs.

## Quick start: Docker only

The default path needs Git, Docker, and the Docker Compose plugin; it does not need
a host JDK or a host Mill installation. From the repository root:

```bash
# Show the modules through the pinned Temurin build container.
scripts/mill-docker.sh resolve examples._

# Run a self-contained example. It writes local Parquet/Arrow output and needs no service stack.
MILL_DOCKER=1 scripts/run-example.sh run 11

# Run the same repository checks used by CI through the containerized Mill runner.
MILL_DOCKER=1 scripts/check-repo.sh build
```

The build container runs as the host user and keeps resolver caches in
`.docker-cache/`, outside Mill's source inputs. It uses host networking so the JVM
can reach the `localhost:1NNxx` addresses advertised by the example stacks. On
Docker Desktop, enable host networking under **Settings → Resources → Network**.

To explore a service-backed example:

```bash
MILL_DOCKER=1 scripts/run-example.sh up 01
MILL_DOCKER=1 scripts/run-example.sh run 01 seed 12
scripts/run-example.sh status 01
scripts/run-example.sh down 01       # keeps data volumes
scripts/run-example.sh reset 01      # explicitly removes data volumes
```

`scripts/run-example.sh list` prints all 16 module names. Example 04 is a Java
Kafka Connect plugin: `up` builds its assembly before mounting it into Connect,
and it deliberately has no `run` command. Example 12 is a batch hand-off to native
Polars in Python: follow its README's generate → container → verify sequence;
`up 12` is intentionally refused because there is no long-running service.

## Catalogue

| # | Example | Focus | Language / JDK | Run shape | Host ports |
| --- | --- | --- | --- | --- | --- |
| 01 | [Exactly-once payment settlement](examples/01-kafka-clients-exactly-once/) | Kafka clients and transactions | Scala 3 / 21 | Compose + CLI | 10180, 10192 |
| 02 | [Card-testing fraud detection](examples/02-kafka-streams-fraud/) | Kafka Streams, windows, state stores | Scala 3 / 17 | Compose + CLI | 10280, 10292 |
| 03 | [Operations dashboard in SQL](examples/03-ksqldb-orders/) | ksqlDB push and pull queries | Scala 3 / 17 | Compose + CLI | 10388, 10392 |
| 04 | [Custom archive partitioner](examples/04-kafka-connect-s3-partitioner/) | Kafka Connect plugin and MinIO | Java / 17 | Assembly + Compose | 10483, 10490–10492 |
| 05 | [Kafka to object storage](examples/05-flink-kafka-s3sink/) | Flink `FileSink` and checkpoints | Scala 2.12 / 17 | Compose + job | 10581, 10590–10592 |
| 06 | [Delivery SLA monitoring](examples/06-flink-cep-shipment-sla/) | Flink CEP and event time | Scala 2.12 / 17 | Compose + job | 10680, 10681, 10692 |
| 07 | [Medallion lakehouse](examples/07-spark-delta-lakehouse/) | Spark and Delta Lake | Scala 2.13 / 17 | Compose + job | 10700, 10701, 10707, 10780, 10781 |
| 08 | [Streaming into Delta](examples/08-spark-streaming-kafka-delta/) | Structured Streaming, Kafka, Delta | Scala 2.13 / 17 | Compose + job | 10880, 10890–10892 |
| 09 | [One query over two systems](examples/09-trino-lakehouse-sql/) | Trino, Delta, PostgreSQL | Scala 3 / 17 | Compose + CLI | 10900, 10901, 10932, 10980 |
| 10 | [Partition pruning made visible](examples/10-presto-hive-analytics/) | PrestoDB, Hive, Parquet | Scala 3 / 17 | Compose + CLI | 11000, 11001, 11032, 11080, 11083 |
| 11 | [The formats themselves](examples/11-parquet-arrow-toolkit/) | Parquet and Arrow internals | Scala 3 / 17 | Local CLI; optional MinIO | 11100, 11101 |
| 12 | [Arrow bridge to Polars](examples/12-polars-arrow-bridge/) | Arrow IPC and native Polars | Scala 3 / 17 + Python | Three-step batch | none |
| 13 | [Operating Kafka](examples/13-cmak-kafka-ops/) | CMAK, `AdminClient`, three brokers | Scala 3 / 17 | Compose + CLI | 11301–11303, 11380, 11381 |
| 14 | [Finding fraud rings](examples/14-hugegraph-fraud-ring/) | HugeGraph and Gremlin | Scala 3 / 17 | Compose + CLI | 11400, 11401 |
| 15 | [CouchDB change data capture](examples/15-couchdb-changes-to-kafka/) | `_changes`, checkpoints, Kafka | Scala 3 / 21 | Compose + service | 11580, 11592, 11598 |
| 16 | [Notebooks over a lakehouse](examples/16-zeppelin-notebooks/) | Zeppelin, Spark, Trino | Scala 3 / 17 | Compose + seed CLI | 11600, 11601, 11640, 11680, 11690 |

The [examples catalogue](examples/README.md) adds learning paths and notes which
parts are local code, one-shot jobs, or long-running services.

## Runtime and resource expectations

Mill compiles every module to Java 17 bytecode and normally selects a pinned
Temurin 17 runtime. Examples 01 and 15 select Temurin 21 at runtime because Ox
uses virtual threads; their bytecode target remains Java 17. The root build
container itself uses a pinned Temurin 21 image so it can launch every module,
while Mill still selects each module's declared runtime.

The stacks are designed to be explored **one at a time**. As a practical starting
point, give Docker 4 CPUs, 8 GiB of memory, and roughly 20 GiB of free disk for
images and volumes. Spark/Delta, Trino/Presto, and especially Zeppelin can need
10–12 GiB during startup or larger experiments. Reserved port ranges let stacks
coexist, but starting all 16 together is not a supported resource target.

## Repository shape

```text
build.mill                     shared versions and module traits
common/                        cross-built domain, generator, and JSON writer
examples/<nn>-<name>/
  package.mill                 isolated dependencies and Scala/JVM choice
  src/                         application and adapter code
  test/src/                    tests that do not require Compose
  docker/docker-compose.yml    one example's external services
  README.md                    runnable walkthrough
docs/architecture.md           dependency boundaries and delivery semantics
docs/validation.md             local and CI verification layers
scripts/check-repo.sh          repository-wide quality entry point
```

`common/` is dependency-light and cross-compiled unchanged for Scala 2.12,
Scala 2.13, and Scala 3. Engine and connector versions stay inside the example
that needs them, preventing Spark's or Flink's dependency graph from leaking into
other modules. See [Architecture](docs/architecture.md) for the actual boundary
map rather than an abstract framework diagram.

## Common commands

Use `MILL_DOCKER=1` with `scripts/run-example.sh` or
`scripts/check-repo.sh` to select the Docker Mill runner.

| Command | Purpose |
| --- | --- |
| `scripts/run-example.sh list` | list all example modules |
| `scripts/run-example.sh config 09` | validate one example's Compose model |
| `scripts/run-example.sh test 09` | run one module's tests |
| `scripts/run-example.sh up 09` | start and wait for one service stack |
| `scripts/run-example.sh down 09` | stop it and preserve volumes |
| `scripts/run-example.sh reset 09` | stop it and remove volumes |
| `./mill --no-server -j 2 __.test` | test the complete mixed-version build locally |
| `./mill mill.scalalib.scalafmt/checkFormatAll` | verify Scala formatting |
| `scripts/check-repo.sh static` | shell, runner, and all Compose checks |
| `scripts/check-repo.sh build` | format, compile, and test with bounded Mill concurrency |
| `scripts/check-repo.sh all` | run both layers |

Scalafmt follows the repository's braceful style; Scala 3 modules also compile
with `-no-indent`. The commands use Mill's documented
[Scala formatting integration](https://mill-build.org/mill/scalalib/linting.html)
and Scalafmt's documented [per-path dialects](https://scalameta.org/scalafmt/docs/configuration.html#fileoverride).

## Validation and scope

Unit tests isolate policy from Kafka, databases, object stores, and query engines.
Compose parsing checks configuration only; it does not pull or start every image.
The exact validation layers, recorded results, and commands for deeper smoke
testing are in [Validation](docs/validation.md).

## Licence

The Kafka Connect partitioner in example 04 is derived from prior work by Can
Elmas and retains its Apache-2.0 licence and attribution. Everything else is
provided as reference material for learning.
