# Examples catalogue

This folder is a local-first reference library of data engineering use-cases.  
Each module has its own `docker/docker-compose.yml`, source, tests, and a dedicated
`README.md` with:

- what the example demonstrates,
- run steps for local containers,
- what output to expect,
- what to try next,
- cleanup instructions.

## Technology portfolio (16 examples)

| # | Example | Technology | Domain model | Scala/JVM |
|---|---|---|---|---|
| 01 | [Kafka exactly-once](01-kafka-clients-exactly-once/) | Apache Kafka client API, transactions, offsets | Order settlement | Scala 3 / JDK 21 |
| 02 | [Kafka Streams fraud detection](02-kafka-streams-fraud/) | Kafka Streams, DSL topology, state store | Fraud ring candidate filtering | Scala 3 |
| 03 | [ksqlDB operations dashboard](03-ksqldb-orders/) | SQL streaming, push and pull queries | Real-time order analytics | Scala 3 |
| 04 | [Kafka Connect partitioner plugin](04-kafka-connect-s3-partitioner/) | Kafka Connect, Connect plugin model | Raw order lake layout | Java 17 |
| 05 | [Flink Kafka sink to S3](05-flink-kafka-s3sink/) | Flink DataStream API, FileSink, checkpoints, windows | Customer order windows | Scala 2.12 |
| 06 | [Flink CEP SLA monitoring](06-flink-cep-shipment-sla/) | Flink CEP, event-time patterns, alerts | Shipment SLA breaches | Scala 2.12 |
| 07 | [Spark Delta lakehouse](07-spark-delta-lakehouse/) | Medallion architecture with Delta Lake | Bronze/Silver/Gold order tables | Scala 2.13 |
| 08 | [Spark streaming into Delta](08-spark-streaming-kafka-delta/) | Structured Streaming, upserts, watermarking | Streaming order aggregation | Scala 2.13 |
| 09 | [Trino lakehouse federation](09-trino-lakehouse-sql/) | SQL federation, cross-catalog joins, query plans | Clickstream + PostgreSQL | Scala 3 |
| 10 | [Presto + Hive + Parquet](10-presto-hive-analytics/) | SQL, partition pruning, table statistics | Clickstream partitioning | Scala 3 |
| 11 | [Parquet + Arrow toolkit](11-parquet-arrow-toolkit/) | Columnar file internals, Arrow vectors | Order/archive data formats | Scala 3 |
| 12 | [Arrow bridge to Polars](12-polars-arrow-bridge/) | Python bridge, Arrow IPC, interoperability | Revenue analytics in Python | Scala 3 |
| 13 | [CMAK and Kafka Ops API](13-cmak-kafka-ops/) | Kafka operations, AdminClient, cluster health | Order pipeline operations | Scala 3 |
| 14 | [HugeGraph fraud rings](14-hugegraph-fraud-ring/) | Graph storage + query, Gremlin REST | Shop relationship fraud patterns | Scala 3 |
| 15 | [CouchDB CDC to Kafka](15-couchdb-changes-to-kafka/) | Change feed, idempotent producer, checkpoints | Catalogue synchronization | Scala 3 / JDK 21 |
| 16 | [Zeppelin notebook front end](16-zeppelin-notebooks/) | Notebook-driven analytics, interpreters | Lakehouse query notebook layer | Scala 3 |

## Repository structure

```
examples/
  nn-tech-name/
    package.mill              module definition for Mill
    src/                     production code
    test/src/                 test suite
    resources/                test fixtures and example assets
    docker/docker-compose.yml per-use-case container stack
    README.md                 runbook for this use-case
```

The prefix `nn-` (for example `09-trino-lakehouse-sql`) determines:

- the example number in the root README,
- the Mill module name (`examples.09-trino-lakehouse-sql`),
- and the reserved host port range (`09xx`).

## Fast local workflow

All modules are built from the repository root.

```bash
# compile and run one module
./mill examples.09-trino-lakehouse-sql.compile

# run one module (example 09 in this case)
./mill examples.09-trino-lakehouse-sql.run

# run tests for all modules (unit tests never need Docker)
./mill __.test
```

To run the container stack for a specific example, open that module's README and use its
`docker compose` commands. As a shortcut, all stacks live in:

```bash
docker compose -f examples/<nn>-<name>/docker/docker-compose.yml up -d --wait
```

and clean up with:

```bash
docker compose -f examples/<nn>-<name>/docker/docker-compose.yml down -v
```

For repetitive local workflows there is also:

```bash
scripts/run-example.sh up 09-trino-lakehouse-sql    # stack up
scripts/run-example.sh test 09-trino-lakehouse-sql   # run example tests
scripts/run-example.sh down 09-trino-lakehouse-sql   # stack down + cleanup
```

### What this repo is trying to be

Not a production template. It is a **reference corpus** for engineers who need
repeatable, realistic end-to-end flows in local environments:

- message streaming semantics,
- stream processing and SQL-on-streams,
- lakehouse storage formats and engines,
- graph + document change-capture systems,
- and notebook / operations layers.

If an example is missing a stack in your environment, follow the same `docker-compose`
pattern already used in these modules and adjust the compose file for your local
runtime constraints.
