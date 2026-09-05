# Architecture

This repository optimizes for comparison: the same shop concepts travel through
different data systems, while each example remains independently buildable and
runnable. It borrows ports-and-adapters and domain-driven design where those ideas
make a boundary testable; it does not impose a framework or a layer for its own sake.

## Dependency direction

```text
common domain values
        ↓
example policy / transformations  ←  unit tests and in-memory fakes
        ↓                                  ↑
policy-owned ports  ←──────────── constructor or parameter injection
        ↑
Kafka, JDBC, HTTP, Spark/Flink, files, Compose adapters
        ↑
Main / job builder (composition root)
```

Dependencies point from delivery code toward policy. External systems never become
part of the shared domain model. Some small examples use functions instead of named
ports; that is intentional when the substitution seam is already obvious.

## What the boundaries are in this codebase

| Area | Domain or policy | Boundary | Adapter / composition |
| --- | --- | --- | --- |
| Shared shop language | `common/src/de/common/domain` (`Order`, `Money`, `Payment`, `Shipment`, identifiers) | Plain values; no engine imports | `gen` supplies deterministic inputs; `json` writes the published event shape |
| 01 Kafka transactions | settlement decisions and `TransactionalSettlement` | `SettlementTransaction` | `KafkaSettlementTransaction`, `KafkaClients`, and `Main` |
| 02 Kafka Streams | `FraudRules`, alert and tally values, topology description | Kafka Serdes and named topics/stores | `Main`, producer, interactive store queries |
| 03 SQL streaming | SQL statement catalogue and response protocol | ksqlDB request/response values | `KsqlDbClient` and `Main` |
| 04 Connect plugin | `PartitionFieldExtractor` path rules | Confluent `TimeBasedPartitioner` API | `FieldAndTimeBasedPartitioner`; Connect constructs the plugin |
| 05–06 Flink | packages named `core` hold window, bucket, timeline, and SLA policy | serializable records and Flink functions in `job` | source/sink builders and `Main` assemble the dataflow |
| 07 Spark batch | `core` medallion transformations and layout | Spark `DataFrame` transformations | `job` owns sessions, Delta paths, I/O, and `Main` |
| 08 Spark streaming | parsing, deduplication, and window transformations | `foreachBatch` callback plus checkpoint paths | `StreamingJob`, Kafka source, Delta upsert, `Main` |
| 09 Trino | SQL parsing, result tables, rendering | `TrinoSession` and `ResultCursor` | private JDBC adapters; `Main` owns connection lifetime |
| 10–12 formats | partition/schema/aggregation/report policy | paths, Arrow schemas, and object-store configuration | Parquet/Arrow/JDBC/S3 code and each `Main`; example 12 crosses to Python through Arrow IPC |
| 13 Kafka operations | replication, lag, and report calculations | plain snapshots returned from admin calls | `KafkaOps`, traffic generator, and `Main` |
| 14 HugeGraph | account plans, property graph, schema, Gremlin query values, ring detection | JSON payloads and `SyncBackend` | `HugeGraphClient` and `Main` |
| 15 CouchDB CDC | feed parsing, mapping, checkpoint arithmetic, `ChangeProcessor` | `ChangeSink`, `CheckpointStore`, `ConnectorLog` | CouchDB/Kafka implementations; `ConnectorService` wires them |
| 16 Zeppelin | notebook/interpreter values and validation | JSON resources and `TrinoSession` use | resource loader, JDBC seed path, Compose-mounted Zeppelin assets |

The packages are the primary map. A `core` package is used where an engine-heavy
module benefits from a visible split; smaller examples stay flat and use precise
type and file names. There is no generic repository, service, or DTO hierarchy.

## Construction and dependency injection

Construction happens at executable edges:

- Example 01 passes a `SettlementTransaction`, a clock function, and a reporter
  into the processing loop. Tests use `RecordingTransaction`.
- Example 09 passes a `TrinoSession` into `ScriptRunner`; the JDBC implementation
  is private and scoped by `TrinoSession.connected`.
- Example 14 receives an sttp `SyncBackend` in `HugeGraphClient`, keeping transport
  lifecycle separate from graph and query policy.
- Example 15 constructor-injects `ChangeSink`, `CheckpointStore`, and `ConnectorLog`
  into `ChangeProcessor`; `ConnectorService` is the composition root.
- Spark and Flink build functions accept sessions, environments, configuration,
  or callback factories rather than opening infrastructure in pure transforms.

These are ordinary constructors and parameters, not a dependency-injection
container. A new interface is warranted when it protects policy from a volatile
system or enables a useful fake, not merely because two classes call each other.

## Version and dependency isolation

`common` is a Mill `Cross` module compiled for Scala 2.12, 2.13, and 3. It uses
the source subset understood by all three and has no third-party dependencies.
Every Scala example selects exactly one matching `common` artifact:

- Flink examples 05–06 use Scala 2.12.
- Spark examples 07–08 use Scala 2.13.
- The other Scala modules use Scala 3.
- Example 04 is a Java module and does not pull Scala into Kafka Connect.

Engine and connector dependencies are declared in each example's `package.mill`.
Cluster-provided libraries use `compileMvnDeps` when packaging them would create
classloader conflicts. All modules emit Java 17-compatible bytecode; examples 01
and 15 run on Java 21 for virtual threads. Scala 3 is kept braceful and is compiled
with `-no-indent`; no such flag is sent to Scala 2.

## Delivery guarantees are local contracts

There is no honest repository-wide “exactly once” switch. Guarantees belong to a
specific source → state → sink path and stop at its external side effects.

| Example / path | Implemented guarantee | Boundary of the claim |
| --- | --- | --- |
| 01 Kafka order → payment record | Kafka transaction commits payment records and consumed offsets atomically; consumers use committed isolation | Exactly-once Kafka record processing, not an external payment-provider charge |
| 05 Kafka → Flink → files | Flink checkpoints source/operator state; `FileSink` publishes completed files on checkpoint | Requires durable checkpoint storage and a compatible filesystem in a real deployment |
| 06 Flink CEP → Kafka alerts | Explicit at-least-once Kafka sink | An alert may repeat after recovery; consumers should use the order key or otherwise deduplicate |
| 08 Kafka → Delta orders | Checkpointed micro-batches plus key-based Delta `MERGE` make replay converge on one order row | The result table is idempotent by order id; this is not a distributed transaction with Kafka |
| 08 closed-window revenue | Append mode emits after the watermark closes a window | Late data beyond the configured watermark is outside the result contract |
| 15 CouchDB → Kafka | Publish happens before checkpoint, so crashes replay rather than lose a change | At-least-once; stable document keys and Kafka producer idempotence make duplicates manageable, not globally impossible |

The remaining examples demonstrate queries, storage layouts, operations, or
interoperability and do not claim an end-to-end delivery guarantee.

## Adding an example

Keep the change local:

1. Reuse `common` only for shop concepts that mean the same thing. Translate an
   external schema at the example boundary instead of putting vendor fields into
   `Order` or `Payment`.
2. Put deterministic decisions in plain functions or values. Introduce a narrow
   port only when a real external mechanism needs substitution.
3. Wire concrete clients, sessions, files, and engines in `Main` or a clearly
   named job builder.
4. Pin dependencies in that example's `package.mill`, select the required Scala
   line, and keep cluster-provided jars out of assemblies.
5. Test policy without Docker; test adapters at their boundary; use Compose for
   the runnable system demonstration.
6. Document the exact delivery semantics and the point where the claim ends.
