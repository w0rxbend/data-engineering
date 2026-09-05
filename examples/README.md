# Examples catalogue

The 16 modules form a local reference library, not one distributed application.
Each directory owns its dependencies, tests, Compose stack, host-port range, and
walkthrough. The shared online-shop vocabulary makes results comparable across
engines without forcing those engines onto one Scala version.

## Pick an example

| # | Example | Question it answers | Stack / execution | Runtime |
| --- | --- | --- | --- | --- |
| 01 | [Kafka exactly-once](01-kafka-clients-exactly-once/) | How are output records and consumed offsets committed atomically? | Kafka + JVM CLI | Scala 3, JDK 21 |
| 02 | [Kafka Streams fraud](02-kafka-streams-fraud/) | How do joins, hopping windows, suppression, and state queries fit together? | Kafka + JVM service | Scala 3, JDK 17 |
| 03 | [ksqlDB orders](03-ksqldb-orders/) | How can an operations dashboard be expressed as streaming SQL? | Kafka/ksqlDB + JVM driver | Scala 3, JDK 17 |
| 04 | [Kafka Connect partitioner](04-kafka-connect-s3-partitioner/) | How does a custom plugin create field-and-time lake paths? | Connect/Kafka/MinIO; mounted assembly | Java, JDK 17 |
| 05 | [Flink S3 sink](05-flink-kafka-s3sink/) | How do event-time windows and checkpointed files reach object storage? | Flink/Kafka/MinIO job | Scala 2.12, JDK 17 |
| 06 | [Flink CEP SLA](06-flink-cep-shipment-sla/) | How are missing shipment milestones detected in event time? | Flink/Kafka job | Scala 2.12, JDK 17 |
| 07 | [Spark Delta lakehouse](07-spark-delta-lakehouse/) | How do bronze, silver, and gold tables evolve through `MERGE`? | Spark/Delta/MinIO job | Scala 2.13, JDK 17 |
| 08 | [Spark streaming Delta](08-spark-streaming-kafka-delta/) | How do checkpoints, watermarks, and idempotent upserts interact? | Spark/Kafka/Delta job | Scala 2.13, JDK 17 |
| 09 | [Trino federation](09-trino-lakehouse-sql/) | How does one query join Delta data to PostgreSQL? | Trino/MinIO/PostgreSQL + JVM CLI | Scala 3, JDK 17 |
| 10 | [Presto Hive analytics](10-presto-hive-analytics/) | What do partition pruning and scan statistics save? | Presto/Hive/MinIO/PostgreSQL + JVM CLI | Scala 3, JDK 17 |
| 11 | [Parquet and Arrow](11-parquet-arrow-toolkit/) | What is physically read for projection, filtering, and Arrow transfer? | Local JVM; MinIO optional | Scala 3, JDK 17 |
| 12 | [Polars Arrow bridge](12-polars-arrow-bridge/) | When is a typed JVM-to-native analytics hand-off worthwhile? | JVM generate/read + one-shot Python container | Scala 3, JDK 17 + native Polars |
| 13 | [Kafka operations](13-cmak-kafka-ops/) | How do replication plans, lag, and cluster health look through `AdminClient`? | Three Kafka brokers/CMAK + JVM CLI | Scala 3, JDK 17 |
| 14 | [HugeGraph fraud rings](14-hugegraph-fraud-ring/) | How are a property graph and bounded traversals used for fraud signals? | HugeGraph/Hubble + JVM CLI | Scala 3, JDK 17 |
| 15 | [CouchDB CDC](15-couchdb-changes-to-kafka/) | How does publish-before-checkpoint create at-least-once delivery? | CouchDB/Kafka + JVM service | Scala 3, JDK 21 |
| 16 | [Zeppelin notebooks](16-zeppelin-notebooks/) | How are Spark and Trino exposed through a reproducible notebook surface? | Zeppelin/Spark/Trino/Hive/MinIO + seed CLI | Scala 3, JDK 17 |

## Suggested paths

- Streaming foundations: 01 → 02 → 03 → 05 → 06 → 08
- Lakehouse and query engines: 11 → 07 → 09 → 10 → 16
- Operations and integration boundaries: 13 → 04 → 15
- Cross-runtime columnar data: 11 → 12

## Run from the repository root

List and inspect modules:

```bash
scripts/run-example.sh list
scripts/run-example.sh config 09
```

Start, exercise, and stop a service-backed example:

```bash
MILL_DOCKER=1 scripts/run-example.sh up 09
MILL_DOCKER=1 scripts/run-example.sh run 09
scripts/run-example.sh down 09
```

`down` preserves named volumes; use `reset` when a clean state is part of the
experiment. Without `MILL_DOCKER=1`, Mill runs on the host and selects the JDK
declared by the module. With it, the root Compose runner supplies the build JVM,
runs as your uid/gid, and mounts persistent caches outside the source tree.

Two modules intentionally differ:

- 04 produces a Java plugin jar for Kafka Connect. `up 04` assembles it first;
  `run 04` is rejected because a plugin has no standalone main program.
- 12 is a finite interoperability pipeline. Generate Arrow files with the Scala
  module, run native Polars with the exact Compose command in its README, then run
  Scala again to compare results. `up 12` is rejected because Compose service
  readiness is the wrong lifecycle for a batch container.

For a self-contained first run:

```bash
MILL_DOCKER=1 scripts/run-example.sh test 11
MILL_DOCKER=1 scripts/run-example.sh run 11
```

## Directory contract

```text
examples/NN-name/
  package.mill              Mill module, dependencies, language and runtime
  src/                      production source
  test/src/                 container-free test suites
  resources/                optional SQL, fixtures, or notebook assets
  docker/docker-compose.yml isolated external services or batch container
  README.md                 exact walkthrough and expected observations
```

`NN` also reserves host ports `1NN00`–`1NN99`; example 09 therefore publishes
only `109xx` ports. Example 12 publishes none. Port isolation avoids accidental
collisions, but memory—not ports—is the reason to run one stack at a time.

See the root [README](../README.md) for prerequisites and the complete port
table, [architecture](../docs/architecture.md) for boundary and delivery maps,
and [validation](../docs/validation.md) for checks and their evidence limits.
