# 08 - Apache Spark Structured Streaming from Kafka into Delta Lake

This example takes a live stream of shop orders out of Apache Kafka and keeps two Delta Lake tables
up to date: one row per order, and one row per five-minute slice of revenue. Apache Spark is a
distributed data processing engine; *Structured Streaming* is the part of it that treats a
never-ending stream as a table that keeps growing, so the same code you would write for a one-off
batch job also works on a stream. Delta Lake is a table format - a folder of Apache Parquet files
plus a transaction log - that adds database features such as `MERGE`, atomic commits and time travel
on top of plain object storage.

Along the way the example shows the handful of concepts that separate a streaming job that works
from one that quietly loses or double-counts data: an explicit schema, event time and watermarks,
the three output modes and why picking the wrong one breaks the job, checkpoints, `foreachBatch`
with a Delta `MERGE`, and the progress metrics that tell you whether the job is keeping up.

> **New to Delta Lake?** Example 07 (`examples/07-spark-delta-lakehouse`) introduces the table
> format from the batch side - what the transaction log contains, how time travel works, why the
> files are Parquet. This example assumes that ground and concentrates on the streaming half.
> Read 07 first if any of those words are new.

## The use case

The online shop publishes every accepted order to the Kafka topic `orders`. Two groups of people
want that data, and they want it in different shapes:

- **Operations** needs a table with one current row per order, so a dashboard can answer "what is
  the status of order-4711?" and "how many orders came in today?". Orders can be *republished* -
  the shop corrects a wrong address or a wrong price and sends the order again under the same id -
  so this table must be keyed by order id, not append-only.
- **Finance** needs revenue per country per five minutes, bucketed by *when the order was placed*,
  not when the data happened to arrive. A mobile client that was offline for two minutes must still
  count towards the window it belongs to.

Both are served from the same stream, by two queries reading one parsed `DataFrame`.

## How it works

### `src/de/spark/streaming/OrderSchema.scala`

The exact shape of the order JSON (JavaScript Object Notation) that `de.common.json.Codecs` writes.

Spark can *infer* a schema from files by scanning them, but a stream cannot be scanned before it is
read, so the schema of a streaming source has to be declared up front. Writing it out has a second
payoff that outlives streaming: if a producer starts sending an extra field tomorrow, this job keeps
producing exactly the same columns instead of silently changing the shape of a table other people
query.

### `src/de/spark/streaming/OrderStreams.scala` - the pure core

Three functions, each `DataFrame => DataFrame`, none of which knows that Kafka or Delta exist.

`parseOrders` does the standard Kafka-to-anything dance. A record read from Kafka arrives as a row
with fixed columns (`key`, `value`, `topic`, `partition`, `offset`, `timestamp`); the payload is the
binary `value` column. So: cast `value` to a string, run `from_json` against the schema, and drop
what did not parse. That last step matters - `from_json` defaults to *permissive* mode, meaning a
message that does not fit the schema becomes `null` instead of killing the query - and it has a
subtlety worth knowing. Permissive mode fails in two different shapes: text that is not JSON at all
produces a `null` struct, while valid JSON that just lacks the expected fields produces a struct
whose fields are all `null`. A filter on the struct itself would let the second shape through and
write a row with a `null` order id, so the code filters on the fields the job cannot work without.
A production job would route those records to a separate "dead letter" table rather than discard
them.

The order total is computed with the `aggregate` higher-order function of SQL (Structured Query
Language), which folds over the `lines` array *inside* the row:

```
aggregate(order.lines, 0L, (acc, line) -> acc + line.unitPrice.cents * line.quantity)
```

Doing the fold in SQL rather than in Scala keeps the work inside Spark's optimizer and avoids
turning every row into a JVM (Java virtual machine) object first.

Finally `timestamp_millis` turns the `placedAt` number into a real timestamp column. That column is
the **event time**: when the order happened according to the shop. It is not the time Spark read the
record, and that distinction is the reason the next function needs a watermark.

`revenuePerWindow` groups orders into fixed, non-overlapping five-minute buckets of event time. An
order stamped 12:07 lands in the 12:05-12:10 bucket no matter when it arrived, which is what makes
the result reproducible: replay the topic tomorrow and the numbers are identical.

`withWatermark("placedAt", "10 minutes")` is the memory bound. It tells Spark: once you have seen an
order stamped 12:40, assume nothing older than 12:30 will still show up. Every window that ended
before 12:30 can then be finalised and its state thrown away. Without a watermark Spark would have
to keep the running total of every window it has ever seen, forever, and the job's memory would grow
until it died. The price is real: an order that arrives *later* than the watermark allows is
dropped, and the already-published window is never corrected. Ten minutes versus one hour is a
direct trade between completeness and cost, and it is a business decision, not a technical one.

`deduplicateByOrderId` collapses several rows carrying the same order id down to the newest one. It
exists because Kafka delivers at least once and the shop republishes corrections, so one micro-batch
can genuinely contain `order-1` twice - and a Delta `MERGE` refuses to run when a source row matches
more than one target row.

### `src/de/spark/streaming/DeltaOrderUpsert.scala` - exactly-once writing

A Spark streaming sink can normally only *append* rows. `foreachBatch` is the escape hatch: it hands
you each micro-batch as an ordinary batch `DataFrame`, so the full batch API (application
programming interface) - including Delta's `MERGE` - becomes available.

```scala
DeltaTable.forPath(spark, tablePath).as("target")
  .merge(deduplicated.as("source"), "target.orderId = source.orderId")
  .whenMatched().updateAll()
  .whenNotMatched().insertAll()
  .execute()
```

`foreachBatch` also passes a `batchId`, and that id is *stable across retries*: if the job dies in
the middle of batch 37 and restarts, Spark replays the very same rows as batch 37. Delta records the
last committed batch id in its transaction log and ignores a replay of a batch it already committed.
That pair - stable batch ids plus a transaction log - is what turns "at-least-once from Kafka" into
"exactly-once in the table".

The first micro-batch has no table to merge into, so the code writes zero rows first. That creates
the transaction log and the schema without inserting anything.

One small but important detail: the micro-batch is `cache()`d before anything reads it. It is
scanned several times - once by the deduplication, then twice more by `MERGE`, which reads its
source to find the matching rows and again to write them - and an uncached `DataFrame` is
*recomputed* for every scan, which here means re-reading the same records from Kafka. Caching turns
that back into a single read, and the progress metrics show it plainly: without the cache the
`orders-to-delta` query reports 1000 input rows for 500 published orders.

### `src/de/spark/streaming/StreamingJob.scala` - the wiring

The only file that knows a broker or a table path exists.

`readOrdersTopic` opens the Kafka source. `startingOffsets` decides where a **brand new** query
begins: `"earliest"` replays the whole topic, `"latest"` picks up only what arrives from now on. As
soon as a checkpoint directory exists the setting is ignored, because the checkpoint then decides.
That is the behaviour you want - it is what lets you stop the job, deploy a new version and restart
without re-reading or skipping data - but it explains a classic confusion: switching an existing job
to `"earliest"` appears to do nothing. Delete the checkpoint to actually replay.

`startOrdersUpsert` writes through `foreachBatch`. Its output mode is not a choice: a query without
aggregation always produces `append`, each input row emitted once. Delta then decides per row
whether that becomes an insert or an update.

`startRevenueAggregation` writes the windowed table, and here the **output mode** is the decision
people get wrong most often:

| Mode | What it emits | Cost | Works for this query? |
| --- | --- | --- | --- |
| `complete` | the entire result table, rewritten every batch | grows forever in a long-running stream | only for a demo |
| `update` | every window whose value changed, including windows still open | sink must be able to overwrite by key | yes, with a `MERGE` |
| `append` | each window once, only after the watermark passes its end | nothing is ever rewritten | yes, and the sink can be plain |

This example uses `append`, because the revenue table is meant to be a durable record of *closed*
windows and a plain Delta append sink can accept it. The cost is latency: a window shows up one
watermark after it ends. Switching to `update` would give a dashboard a window's revenue rising live
as orders trickle in - and would force the revenue table through a `MERGE` too, exactly like the
orders table.

Each query gets its **own** checkpoint directory. Two queries must never share one; the checkpoint
holds the committed Kafka offsets and the aggregation state of exactly one query.

### `src/de/spark/streaming/ProgressReporter.scala` - is the job keeping up?

Spark publishes a `StreamingQueryProgress` object after every micro-batch. Three of its numbers
answer nearly every operational question:

- `inputRowsPerSecond` - how fast records arrive from Kafka;
- `processedRowsPerSecond` - how fast the job consumes them. When this stays *below* the input rate
  the job is falling behind and the Kafka lag grows without bound;
- `numInputRows` per batch - a sudden jump usually means the job was stalled and is now working
  through a backlog.

The listener prints one line per batch, so the information survives the job exiting. The same data
is in the Spark web user interface under "Structured Streaming".

### The rest

- `JobConfig.scala` - every knob in one immutable case class, overridable from environment
  variables.
- `SparkSessions.scala` - builds the local Spark session. Delta plugs itself in through two
  settings, an *extension* (teaches the SQL planner about `MERGE`) and a *catalog*; forgetting
  either produces a confusing "MERGE is not supported" error even with the Delta jar present. It
  also holds the two settings that make Hadoop's S3 client talk to MinIO instead of Amazon.
- `OrderPublisher.scala` - fills the topic using the shared, seeded `de.common.gen.DataGenerator`,
  keyed by order id so all versions of one order land in the same partition.
- `Main.scala` - dispatches `produce` and `stream`.

### The tests

`./mill examples.08-spark-streaming-kafka-delta.test` runs with **no docker at all**. It starts a
single-process Spark session and feeds the pure functions from memory:

- `OrderStreamsSuite` checks parsing, the malformed-record drop, window bucketing and deduplication
  on batch data, and then uses a `MemoryStream` to prove the append-mode behaviour end to end: an
  open window emits nothing, a closed one emits exactly once, and a straggler older than the
  watermark is ignored rather than reopening it.
- `DeltaOrderUpsertSuite` writes a real Delta table into a temporary directory and checks that a
  republished order replaces its previous row instead of adding one.

A Delta table is a folder, not a server, which is why the second suite needs nothing running.

## Run it

Everything below is run from the repository root.

**1. Start the stack.**

```bash
docker compose -f examples/08-spark-streaming-kafka-delta/docker/docker-compose.yml up -d
```

Wait for the containers to report healthy:

```bash
docker compose -f examples/08-spark-streaming-kafka-delta/docker/docker-compose.yml ps
```

Ports published on your machine (all inside this example's reserved range 10800-10899):

| Address | What it is |
| --- | --- |
| `localhost:10892` | Kafka broker |
| <http://localhost:10880> | Kafka web user interface - browse the `orders` topic |
| <http://localhost:10890> | MinIO S3 (Amazon Simple Storage Service) API |
| <http://localhost:10891> | MinIO console (user `minioadmin`, password `minioadmin`) |

**2. Publish some orders.**

```bash
./mill examples.08-spark-streaming-kafka-delta.run produce 500
```

Expected output ends with `published 500 orders to topic 'orders'`. You can now see them in the
Kafka user interface at <http://localhost:10880> under Topics -> orders -> Messages.

**3. Run the streaming job.**

```bash
./mill examples.08-spark-streaming-kafka-delta.run stream
```

It reads the topic from the earliest offset, writes both Delta tables under `out/08-spark-streaming/`
and stops after two minutes. Expect lines like:

```
reading topic 'orders' from localhost:10892
orders table  -> out/08-spark-streaming/orders
revenue table -> out/08-spark-streaming/revenue-by-window
query 'orders-to-delta' started (id ...)
query 'revenue-by-window' started (id ...)
batch 0 | query 'revenue-by-window' | rows 500 | in/s 0.0 | out/s 153.7 | took 3253ms
batch 0 | query 'orders-to-delta' | rows 500 | in/s 0.0 | out/s 60.8 | took 8233ms
batch 1 | query 'revenue-by-window' | rows 0 | in/s 0.0 | out/s 0.0 | took 3530ms
```

`in/s 0.0` on batch 0 is normal: there is no previous batch to measure an arrival rate against yet.
The exact numbers differ on every machine.

**4. Look at the result.**

The tables are Delta folders on disk. Read them back with the same job's Spark session, for example
through the Scala console of your choice, or point example 09 (Trino) at them. The quickest check is
that the folders exist and contain a `_delta_log` directory:

```bash
ls out/08-spark-streaming/orders out/08-spark-streaming/orders/_delta_log
```

**5. Write to object storage instead (optional).**

Point the table paths at MinIO to see the identical job write to S3-compatible storage:

```bash
S3_ENDPOINT=http://localhost:10890 \
ORDERS_TABLE_PATH=s3a://lakehouse/orders \
REVENUE_TABLE_PATH=s3a://lakehouse/revenue-by-window \
CHECKPOINT_ROOT=s3a://lakehouse/checkpoints \
./mill examples.08-spark-streaming-kafka-delta.run stream
```

The files then appear in the `lakehouse` bucket in the MinIO console at <http://localhost:10891>.

## What to try next

- **Watch the restart.** Run `stream`, let it finish, publish 200 more orders, then run `stream`
  again. The second run reports far fewer input rows in batch 0: it resumed from the checkpoint
  instead of re-reading the topic. Now delete `out/08-spark-streaming/checkpoints` and run it once
  more - it replays everything, and the orders table is *unchanged*, because `MERGE` is idempotent.
  That is exactly-once in action.
- **Break the output mode.** Change `outputMode("append")` to `"complete"` in
  `StreamingJob.startRevenueAggregation`. The query still runs, but every batch rewrites the whole
  revenue table. Then try `"update"` - Spark refuses, because a plain Delta append sink cannot
  overwrite rows. The error message is the shortest possible explanation of why mode and sink have
  to agree.
- **Shrink the watermark.** Set `watermarkDelay` to `"0 seconds"` in `JobConfig` and re-run. Windows
  close almost immediately, and orders that arrive even slightly out of order start disappearing
  from the totals. Then raise it to `"1 hour"` and watch the state - visible as
  `stateOperators.numRowsTotal` in the progress output - grow instead.
- **Change the window.** Five minutes to one minute produces five times as many rows and five times
  as many files. This is where a lakehouse's small-file problem starts, and why example 07 talks
  about compaction.
- **Kill the broker mid-run.** `docker compose ... stop kafka` while the job is streaming. The query
  fails; restart the broker and the job, and it picks up from the last committed offset.

## Clean up

```bash
docker compose -f examples/08-spark-streaming-kafka-delta/docker/docker-compose.yml down -v
```

The `-v` also removes the MinIO volume. The local Delta tables under `out/08-spark-streaming/` are
plain folders; delete them with `rm -rf out/08-spark-streaming` when you are done.
