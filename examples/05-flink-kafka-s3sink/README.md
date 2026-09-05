# 05 — Apache Flink: Kafka to S3-compatible object storage

This example runs an **Apache Flink** streaming job that reads `Order` events from an **Apache Kafka**
topic, groups every customer's orders into fixed one-hour **event-time windows**, and writes one JSON
summary file per customer and window into **S3-compatible object storage**. It shows the three ideas
that make a streaming job to a data lake trustworthy: event time and watermarks, keyed state with
timers, and exactly-once file output driven by checkpoints.

Apache Flink is a stream processor: a program that reads records one at a time, keeps state about
what it has seen, and produces results continuously rather than in nightly batches. "S3-compatible
storage" means any service that speaks the Amazon Simple Storage Service application programming
interface; the local stack uses **MinIO**, which speaks exactly that interface on your own machine.

> This example replaces an older stand-alone sbt project that lived at `flink-kafka-s3sink-job/`.

## The use case

The online shop wants a *customer 360* data lake: for every customer, an hour-by-hour record of what
they bought, laid out so that analysts can query a single day without scanning years of history.

The shop's checkout publishes an `Order` event to Kafka the moment an order is accepted. This job
turns that firehose into one small file per customer per hour:

```json
{"customerId":"cust-0000","windowStart":1700956800000,"windowEnd":1700960400000,
 "orderCount":1,"orderIds":["order-0843432"],"totalCents":11832,"currency":"EUR"}
```

stored at

```
s3://orders/customer-batches/customer_id=cust-0000/dt=2023-11-26/hour=00/part-a9487644-...-0
```

The `Order`, `CustomerId` and `Money` types come from the shared `common` module, so the events this
job reads are byte-for-byte the events every other example in this repository produces.

## How it works

The code is split so that every business decision is a plain function you can call from a test, and
everything that touches Flink is mechanical wiring.

### The pure core — `src/de/flink/s3sink/core/` (no Flink imports at all)

| File | What it does |
| --- | --- |
| `EventTimeWindows.scala` | Arithmetic for tumbling windows: given a timestamp and a window size, which window does it belong to? Windows are aligned to 1970-01-01T00:00:00Z, so every parallel worker agrees on the boundaries with no coordination. |
| `CustomerOrderBatch.scala` | The record that gets written (`CustomerOrderBatch`) and the running total kept while a window is open (`BatchAccumulator`). Folding an order into the accumulator is a pure function; mixing two currencies in one batch is refused with an explicit message. |
| `BatchWindowing.scala` | The windowing decision itself: tag each buffered order with its window, and "close window X" means take out exactly that window's orders and leave the rest buffered. |
| `BucketPath.scala` | Turns a finished batch into the directory it is stored under. |
| `OrderJson.scala` | Decodes the shared `Order` wire format (with the **circe** JSON library) and encodes the batch record. Decoding returns `Either`, never an exception. |
| `JobConfig.scala` | Every setting — brokers, topic, consumer group, output location, checkpoint interval, window size, lateness allowance — resolved from defaults, then environment variables, then command-line flags. |

### The Flink wiring — `src/de/flink/s3sink/job/`

| File | What it does |
| --- | --- |
| `OrderRecords.scala` | The flat tuples that travel between operators, plus their type descriptions. |
| `OrderDeserializationSchema.scala` | Kafka bytes to a tuple. A record that will not parse is logged and **dropped**. |
| `OrderSelectors.scala` | The key selector (which customer a record belongs to) and the timestamp assigner (which field is the event time). |
| `CustomerBatchProcessFunction.scala` | The `KeyedProcessFunction` that buffers orders in Flink state and registers an event-time timer per window. |
| `BatchFileSink.scala` | Builds the `FileSink` that writes the files. |
| `OrderBatchPipeline.scala` | Assembles source, window and sink into one graph. |
| `Main.scala` | The composition root: reads the environment, creates the execution environment, submits the job. |
| `OrderProducer.scala` | A helper that fills the topic with generated orders. Not part of the job. |

### Event time and watermarks

Every `Order` carries `placedAtEpochMillis`, the moment the shop accepted it. That is the job's
**event time**. The alternative, *processing time*, is the moment Flink happened to read the record —
which depends on network delays and on how often the job was restarted, so the same input would
produce different output on every run. Using event time means a replay of last week's topic produces
exactly the same files as reading it live.

Records do not arrive in event-time order: Kafka partitions are read independently, and a slow
producer can lag behind a fast one. Flink deals with this using a **watermark**, a marker that flows
through the pipeline meaning *"event time has definitely reached T; nothing older than T is still
coming"*. `OrderBatchPipeline.watermarkStrategy` builds one with
`forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS_MS)`, which is the promise "an event may be at most
this many milliseconds out of order". Larger values tolerate more disorder but hold windows open
longer; anything later than the allowance is a *late* record and is not counted.

`withIdleness` matters too: a topic partition that receives no traffic would otherwise hold the
watermark back for the whole job, and no window would ever close.

### Keyed state and the timer

`CustomerBatchProcessFunction` runs once per customer (the stream is `keyBy`-ed on the customer
identifier, so Flink guarantees that all of one customer's records go to the same worker and see the
same state). For each record it:

1. works out which window the order belongs to,
2. appends the raw order to a `ListState`, tagged with that window,
3. registers an **event-time timer** for the last millisecond of the window.

Flink fires that timer when the *watermark* passes the timestamp — not when the wall clock does.
`onTimer` then hands the whole buffer to the pure `BatchWindowing.close`, emits the resulting record,
and writes back only the orders whose windows are still open, so state cannot grow without bound.

The buffer is deliberately window-aware. Flink advances the watermark only every couple of hundred
milliseconds, so a fast source easily delivers the first order of the *next* window before the
previous window's timer has fired. A naive single accumulator would silently merge the two.

### Checkpointing and exactly-once output

A **checkpoint** is a consistent snapshot of everything the job remembers: the Kafka read positions,
the buffered orders, the pending timers. Flink takes one every `CHECKPOINT_INTERVAL_MS`. If a machine
dies, the job restarts from the last snapshot — the buffered orders come back and the Kafka offsets
rewind to match, so no order is lost and none is counted twice.

The output side is what turns that into **exactly-once files**. `FileSink` (the modern replacement
for the deprecated `StreamingFileSink`) is a two-phase sink:

- While a file is being written it is *in progress* and carries a hidden name: `.part-…inprogress.…`
  on a normal filesystem, `_part-…_tmp_…` on object storage, where Flink stages the data as a
  separate temporary object. Either way it does not match `part-…`, so readers ignore it.
- The sink uses `OnCheckpointRollingPolicy`, so on every checkpoint the open file is closed and
  becomes *pending*.
- When the checkpoint is confirmed complete, the pending file is renamed to its final `part-…` name.

A file therefore becomes visible at exactly the moment the snapshot that produced it became durable.
A restart re-does the work only for records after that snapshot, and the half-written file from the
crashed attempt is never promoted. The practical consequence: **the checkpoint interval is also the
delay before finished batches appear in the bucket**, and it decides how many files you get.

### The bucket layout

```
s3://orders/customer-batches/customer_id=<customer>/dt=<YYYY-MM-DD>/hour=<HH>/part-<uuid>-<n>
```

Each directory name is a `column=value` pair. This is **Hive-style partitioning**, and query engines
such as Trino, Apache Spark and Apache Hive read those names as real columns. A query filtered to one
day opens only the files under that `dt=` directory instead of scanning the whole bucket — an
optimisation called *partition pruning*.

The date comes from the **window start**, formatted in UTC (Coordinated Universal Time), never from
the wall clock. That is what makes a replay idempotent: yesterday's data lands in yesterday's
directories no matter when the job runs. `BucketPath` also replaces any character that would break
the `column=value/` structure.

### What changed from the old sbt project

- Error handling no longer throws `RuntimeException("…")` with profanity in the message. A record
  that cannot be parsed is logged and skipped, which is the only safe behaviour on a shared topic:
  throwing would make Flink restart, read the same bad record and crash again, forever.
- Brokers, topic and bucket are configuration, not constants compiled into the jar.
- The deprecated `StreamingFileSink` is replaced by `FileSink`.
- Processing-time timers rounded to the next hour are replaced by event-time timers on real windows.
- The ad-hoc `DataWrapper`/`DataInterimModel` types are replaced by the shared `Order` domain.
- The business logic is pure and covered by unit tests that need neither Docker nor a cluster.

## Run it

All commands are run from the repository root.

### 1. Start the local stack

```bash
docker compose -f examples/05-flink-kafka-s3sink/docker/docker-compose.yml up -d
```

This starts Kafka (in KRaft mode, so no ZooKeeper), MinIO, a one-shot container that creates the
`orders` bucket, a Flink JobManager and a Flink TaskManager. `depends_on` conditions make `up` wait
until each piece is healthy, so there is nothing to poll by hand.

| Service | Host port | URL / address |
| --- | --- | --- |
| Flink web user interface | 10581 | <http://localhost:10581> |
| MinIO S3 API | 10590 | <http://localhost:10590> |
| MinIO web console | 10591 | <http://localhost:10591> (`minioadmin` / `minioadmin`) |
| Kafka (from your machine) | 10592 | `localhost:10592` |

Inside the Docker network the services use their standard ports: `kafka:9092` and `minio:9000`.

### 2. Fill the topic with orders

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:10592 KAFKA_TOPIC=orders \
  ./mill examples.05-flink-kafka-s3sink.runMain de.flink.s3sink.job.OrderProducer
```

Expected output:

```
Published 500 Order events to topic 'orders' at localhost:10592
```

The generator produces events roughly half a second apart. `EVENT_TIME_SPEEDUP` (default `12000`)
stretches that generated timeline so 500 records cover several hundred one-hour windows, which is
what makes the demo produce files immediately instead of after an hour.

### 3. Build the job jar and submit it

`assembly` packages the compiled code together with every dependency the Flink cluster does not
already provide (the Kafka connector and circe). Flink itself is compiled against but **not** bundled.

```bash
./mill show examples.05-flink-kafka-s3sink.assembly

docker cp out/examples/05-flink-kafka-s3sink/assembly.dest/out.jar de-05-jobmanager:/tmp/job.jar

docker exec de-05-jobmanager /opt/flink/bin/flink run -d \
  -c de.flink.s3sink.job.Main /tmp/job.jar \
  --kafka-bootstrap-servers kafka:9092 \
  --kafka-topic orders \
  --output-uri s3://orders/customer-batches \
  --checkpoint-interval-ms 10000
```

Expected output ends with:

```
Job has been submitted with JobID 71e179dc32ebe5063046d295f577de0f
```

Open <http://localhost:10581> to watch it. The job graph shows three tasks
(`orders-kafka-source`, `customer-order-window`, `customer-batch-file-sink`), and the *Checkpoints*
tab fills up every ten seconds.

Every flag has an environment-variable twin: `--kafka-topic orders` and `KAFKA_TOPIC=orders` mean the
same thing, and `--flag=value` works as well as `--flag value`.

| Setting | Flag | Environment variable | Default |
| --- | --- | --- | --- |
| Kafka brokers | `--kafka-bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| Topic | `--kafka-topic` | `KAFKA_TOPIC` | `orders` |
| Consumer group | `--kafka-group-id` | `KAFKA_GROUP_ID` | `flink-s3-sink` |
| Output location | `--output-uri` | `OUTPUT_URI` | `s3://orders/customer-batches` |
| Checkpoint interval | `--checkpoint-interval-ms` | `CHECKPOINT_INTERVAL_MS` | `15000` |
| Window size | `--window-size-ms` | `WINDOW_SIZE_MS` | `3600000` |
| Lateness allowance | `--max-out-of-orderness-ms` | `MAX_OUT_OF_ORDERNESS_MS` | `5000` |

### 4. Look at the files

Give it a minute — the job first has to start up and read the whole topic, and a part file only
becomes visible when the checkpoint that produced it completes — then list the bucket:

```bash
docker run --rm --entrypoint /bin/sh --network de-05-flink-s3 \
  minio/mc:RELEASE.2025-04-16T18-13-26Z -c \
  "mc alias set l http://minio:9000 minioadmin minioadmin >/dev/null && \
   mc ls --recursive l/orders/customer-batches | head"
```

Expected output, one line per finished window:

```
[...] 161B STANDARD customer_id=cust-0000/dt=2023-11-26/hour=00/part-a9487644-...-0
[...] 161B STANDARD customer_id=cust-0001/dt=2023-12-02/hour=05/part-4eaeffd6-...-0
```

Read one of them:

```bash
docker run --rm --entrypoint /bin/sh --network de-05-flink-s3 \
  minio/mc:RELEASE.2025-04-16T18-13-26Z -c \
  "mc alias set l http://minio:9000 minioadmin minioadmin >/dev/null && \
   mc cat l/orders/customer-batches/customer_id=cust-0000/dt=2023-11-26/hour=00/part-a9487644-...-0"
```

You can also browse the bucket in the MinIO console at <http://localhost:10591>.

### 5. Run the tests

The unit tests cover the window arithmetic, the bucket paths, the JSON round trip, the configuration
resolution and the process function itself (through Flink's single-operator test harness, with a
watermark the test advances by hand). They need no Docker and no cluster:

```bash
./mill examples.05-flink-kafka-s3sink.test
```

## What to try next

- **Shrink the window.** Resubmit with `--window-size-ms 60000` and watch the `hour=` directories fill
  with many more, much smaller files. This is the file-size problem every data lake eventually hits.
- **Change the checkpoint interval.** `--checkpoint-interval-ms 60000` makes finished batches take a
  minute to appear, because a part file is only promoted when its checkpoint completes.
- **Kill the TaskManager mid-run.** `docker kill de-05-taskmanager`, then
  `docker compose -f examples/05-flink-kafka-s3sink/docker/docker-compose.yml up -d taskmanager`.
  The job restarts from the last checkpoint. Count the records in the bucket before and after: the
  totals are unchanged, because the in-progress files from the killed attempt were never promoted.
- **Feed late data.** Run the producer again with `EVENT_TIME_SPEEDUP=1`, so new orders carry
  timestamps far behind the current watermark, and watch those windows produce nothing — they are
  already closed. Then raise `--max-out-of-orderness-ms` and try again.
- **Send a broken record** with `kafka-console-producer.sh` and confirm in the TaskManager log
  (`docker logs de-05-taskmanager`) that it is skipped rather than crashing the job.

Note that the very last window of a finite input never closes: no further events means the watermark
stops advancing, so its timer never fires. That is expected behaviour for an unbounded stream, and it
is why the producer generates orders spanning many windows.

## Clean up

```bash
docker compose -f examples/05-flink-kafka-s3sink/docker/docker-compose.yml down -v
```

`-v` also deletes the MinIO volume, so the next run starts from an empty bucket.
