# 06 — Apache Flink CEP: delivery SLA monitoring

This example runs an **Apache Flink** job that watches the shipment milestones of every order and
raises an alert the moment a delivery promise is broken. It is built with **Flink CEP**, the
*complex event processing* library that ships with Flink.

Three terms first, because everything below leans on them:

- **Apache Flink** is a stream processor: a program that reads records one at a time, keeps state
  about what it has seen, and produces results continuously rather than in a nightly batch.
- **Complex event processing (CEP)** means looking for a *sequence* of events rather than
  reacting to a single one. "A dispatch scan, and then a delivery scan, and no more than two days
  between them" is a sequence; matching it is what this library does.
- A **service-level agreement (SLA)** is a promise about time. The shop promises "we hand your
  parcel to the carrier within four hours, and it is delivered within two days". A **breach** is
  that promise being broken.

The interesting half of CEP is not the matching. It is the *non*-matching: a sequence that starts
and then never finishes is exactly what an SLA breach looks like, and Flink CEP reports those as
**timed-out partial matches**.

> Example 05 uses Flink to write a data lake and covers the sink side — event-time windows,
> checkpoints and exactly-once file output. This example never touches object storage; it is about
> the pattern API and about detecting the events that *fail to arrive*.

## The use case

Every order in the shared online-shop domain produces up to three shipment milestones:

```
Created  ──────►  Dispatched  ──────►  Delivered
   the warehouse      the carrier          the customer's
   accepted it        picked it up         doorstep
```

The operations team needs to know two things, per order, as they happen:

1. **The promise was kept.** `Created` was followed by `Dispatched` in time, `Dispatched` by
   `Delivered` in time. Useful for the daily SLA percentage.
2. **The promise was broken.** Either the parcel is still in the warehouse hours after the order was
   accepted, or it left the warehouse days ago and was never scanned as delivered. Each of these is
   somebody's afternoon: a pallet that was not loaded, a parcel lost in a hub.

Nothing tells the system that a parcel is stuck. The absence of the next event is the signal, and a
deadline is the only way to notice an absence.

## How it works

The code is split so that every business decision is a plain function you can call from a test, and
the Flink-specific classes contain nothing but wiring.

### The pure core — `src/de/flink/cep/sla/core/`

| File | What it holds |
| --- | --- |
| `SlaPolicy.scala` | The promise itself: dispatch within *n* milliseconds, deliver within *m*. Also the deadline arithmetic. |
| `SlaAlert.scala` | The four possible outcomes and the four functions that build an alert out of the matched shipment events. Every sentence an operator reads is produced here. |
| `ShipmentSlaPatterns.scala` | The two CEP patterns, written in Flink's pattern language. |
| `ShipmentJson.scala` | Reads the shared `Shipment` wire format, writes the alert JSON. Decoding returns a value on failure instead of throwing. |
| `ShipmentRecords.scala` | The flat tuples that cross operator boundaries, and the conversions to and from the shared domain model. |
| `ShipmentTimeline.scala` | Shapes the demonstration data: which milestones go missing, and how far apart the orders sit in event time. |
| `JobConfig.scala` | Every setting, resolved from defaults, then environment variables, then command-line flags. |

The `Order`, `OrderId`, `Shipment` and `ShipmentStatus` types come from the shared `common` module,
so the events this job reads are byte-for-byte the events every other example in this repository
produces.

### The patterns

```scala
Pattern
  .begin[Event](CreatedStep).where(hasStatus(Created))
  .next(DispatchedStep).where(hasStatus(Dispatched))
  .within(Duration.ofMillis(policy.dispatchWithinMillis))
```

- `begin(name)` starts a pattern and names its first step. The names are how a match is read back:
  Flink hands a match over as `Map[stepName, List[event]]`.
- `next(name)` demands **strict contiguity** — the very next event for that order must match. Any
  other milestone in between destroys the partial match. Between creation and dispatch there is no
  legitimate other milestone, so this is the honest operator here: an order that jumps straight from
  `Created` to `Delivered`, a missing warehouse scan, should not quietly count as dispatched.
- `followedBy(name)`, used by the delivery pattern, is **relaxed**: unrelated events in between are
  skipped. While a parcel is with the carrier, a re-scan or a partial shipment may legitimately turn
  up and must not cancel the watch.
- `within(duration)` bounds the whole sequence **in event time**, and is what turns this from a
  matcher into an SLA monitor.

### Two outputs, two callbacks — `src/de/flink/cep/sla/job/SlaMatchFunctions.scala`

A `PatternProcessFunction` has one callback for a completed match. Implementing
`TimedOutPartialMatchHandler` as well adds a second one:

| Callback | When Flink calls it | Where the alert goes |
| --- | --- | --- |
| `processMatch` | the whole sequence completed inside the window | the operator's main output, through the `Collector` |
| `processTimedOutMatch` | the watermark passed the end of the window while the sequence was still incomplete | a **side output**, because a timed-out match has no `Collector` |

A *side output* is an extra outlet of an operator, addressed by an `OutputTag`. The tag used for
writing and the tag used for reading must be the same value, which is why both patterns share the
one tag defined in `SlaOutputTags.scala`.

### Event time, watermarks and why a breach happens at all

Flink measures time in two ways. *Processing time* is the clock on the wall of the machine running
the job. *Event time* is the timestamp inside the record — when the parcel was actually scanned. This
job uses event time only, so replaying a topic produces exactly the same alerts today as it will
next year, and a carrier feed that is an hour behind does not raise false alarms.

Since event time comes from the data, Flink needs to be told when it may consider a moment complete.
That signal is the **watermark**: "everything with a timestamp older than this has arrived". The job
builds it with `forBoundedOutOfOrderness`, which places the watermark a fixed distance behind the
newest timestamp seen; that distance (`--max-out-of-orderness-ms`, five seconds by default) is the
disorder the job tolerates — a delivery scan overtaking its dispatch scan, say.

**The watermark is what fires a breach.** A partial match is declared timed out exactly when the
watermark passes the end of its `within` window. A consequence worth internalising: if no new events
arrive, the watermark does not move, and a stuck parcel is never reported. Absence of data is not
evidence; later data is. That is why the producer publishes the milestones of all orders interleaved
in event-time order, and why the last few orders of a batch stay pending until you publish more.

### Keyed patterns

`detect` keys the stream by order identifier before applying either pattern. Flink then runs one
independent state machine per key, so the `Dispatched` event of one order can never complete the
sequence that another order's `Created` event started. Without the `keyBy`, the whole topic would be
matched as a single interleaved sequence, which is meaningless here.

### The wiring — `src/de/flink/cep/sla/job/`

| File | What it does |
| --- | --- |
| `Main.scala` | Composition root: reads the environment, creates the execution environment, starts the job. |
| `ShipmentSlaPipeline.scala` | Kafka source, watermark strategy, both pattern matchers, both Kafka sinks. `detect` is the part the mini-cluster test reuses. |
| `SlaMatchFunctions.scala` | The two callbacks described above. |
| `ShipmentSelectors.scala` | Key selector and timestamp assigner, as named classes rather than lambdas. |
| `ShipmentDeserializationSchema.scala` | Kafka bytes to record; an unparsable record is logged and dropped instead of crashing the job in a restart loop. |
| `AlertKafkaSink.scala` | Writes alerts back to Kafka, keyed by order identifier, at-least-once. |
| `ShipmentProducer.scala` | A helper that fills the input topic. Not part of the job. |

Two details that look odd and are deliberate:

- **Flat tuples cross operator boundaries.** Flink has to serialise every record between operators.
  It has fast serialisers for its own tuples and Java primitives, but falls back to the
  general-purpose Kryo library for Scala case classes with collections or sealed traits. The domain
  model stays pure Scala *inside* the functions; only tuples travel.
- **Conditions and selectors are named classes, not lambdas.** A Scala 2.12 lambda is serialised as a
  `SerializedLambda` that has to be reconstructed with the exact Scala runtime helper that produced
  it. When the job jar and the Flink distribution each carry their own copy of the Scala library,
  that reconstruction fails with a puzzling `InvalidObjectException`. A plain class is serialised by
  name and has no such problem.

Scala 2.12 is not a stylistic choice either: the Flink Scala helpers and connectors are published
for 2.12 only, and a Flink cluster puts a Scala 2.12 runtime on every job's classpath.

## Run it

All commands are run from the repository root.

### 1. Start the local stack

```bash
docker compose -f examples/06-flink-cep-shipment-sla/docker/docker-compose.yml up -d
```

This starts Kafka (in KRaft mode, so no ZooKeeper), a one-shot container that creates the three
topics, a Kafka web user interface, a Flink JobManager and a Flink TaskManager. The `depends_on`
conditions make `up` wait until each piece is healthy, so there is nothing to poll by hand.

| Service | Host port | URL / address |
| --- | --- | --- |
| Kafka web user interface | 10680 | <http://localhost:10680> |
| Flink web user interface | 10681 | <http://localhost:10681> |
| Kafka (from your machine) | 10692 | `localhost:10692` |

Inside the Docker network the services use their standard ports: `kafka:9092`, `jobmanager:8081`.

### 2. Fill the topic with shipment milestones

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:10692 \
  ./mill examples.06-flink-cep-shipment-sla.runMain de.flink.cep.sla.job.ShipmentProducer
```

Expected output:

```
Published 501 shipment events (Created=200, Delivered=129, Dispatched=172) to topic 'shipments' at localhost:10692
```

The faults are arithmetic, not random, so the numbers are the same on every run: every seventh order
reports only `Created` (never dispatched), and every fourth of the remaining ones stops after
`Dispatched` (never delivered).

The shared generator places its orders about half a second apart, while a delivery promise is
measured in days. `EVENT_TIME_SPEEDUP` (default `20000`) multiplies the distance between orders so
that a couple of hundred orders span weeks of event time — which is what makes deadlines actually
pass. Only the timestamps in the data change; the job still runs as fast as your machine allows.

### 3. Build the job jar and submit it

`assembly` packages the compiled code together with every dependency the Flink cluster does not
already provide — the CEP library, the Kafka connector and circe. Flink itself is compiled against
but **not** bundled.

```bash
./mill show examples.06-flink-cep-shipment-sla.assembly

docker cp out/examples/06-flink-cep-shipment-sla/assembly.dest/out.jar de-06-jobmanager:/tmp/job.jar

docker exec de-06-jobmanager /opt/flink/bin/flink run -d \
  -c de.flink.cep.sla.job.Main /tmp/job.jar \
  --kafka-bootstrap-servers kafka:9092 \
  --shipment-topic shipments \
  --dispatch-within-ms 14400000 \
  --deliver-within-ms 172800000
```

Expected output ends with:

```
Job has been submitted with JobID 6f1c0a3d8b2e4f5a9c7d1e0b3a4f5c6d
```

Open <http://localhost:10681> to watch it. The job graph shows the source, the two pattern matchers
and the two sinks; the *Checkpoints* tab fills up every fifteen seconds.

Every flag has an environment-variable twin: `--shipment-topic shipments` and
`SHIPMENT_TOPIC=shipments` mean the same thing, and `--flag=value` works as well as `--flag value`.

| Setting | Flag | Environment variable | Default |
| --- | --- | --- | --- |
| Kafka brokers | `--kafka-bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| Input topic | `--shipment-topic` | `SHIPMENT_TOPIC` | `shipments` |
| Breach topic | `--breach-topic` | `BREACH_TOPIC` | `shipment-sla-breaches` |
| Kept-promise topic | `--completion-topic` | `COMPLETION_TOPIC` | `shipment-sla-completions` |
| Consumer group | `--kafka-group-id` | `KAFKA_GROUP_ID` | `flink-cep-shipment-sla` |
| Dispatch promise | `--dispatch-within-ms` | `DISPATCH_WITHIN_MS` | `14400000` (4 hours) |
| Delivery promise | `--deliver-within-ms` | `DELIVER_WITHIN_MS` | `172800000` (2 days) |
| Lateness allowance | `--max-out-of-orderness-ms` | `MAX_OUT_OF_ORDERNESS_MS` | `5000` |
| Checkpoint interval | `--checkpoint-interval-ms` | `CHECKPOINT_INTERVAL_MS` | `15000` |

### 4. Read the alerts

Give the job a few seconds to start up and read the topic, then look at the breaches:

```bash
docker exec de-06-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic shipment-sla-breaches \
  --from-beginning --timeout-ms 15000
```

Expected output, one JSON object per breached order:

```json
{"orderId":"order-0821675","outcome":"NotDispatchedInTime","breach":true,"lastStatus":"Created",
 "lastObservedAt":1700162660277,"deadline":1700162660277,"latenessMs":0,
 "message":"Order order-0821675 was still in the warehouse 4.0 hours after it was created."}
{"orderId":"order-0744518","outcome":"NotDeliveredInTime","breach":true,"lastStatus":"Dispatched",
 "lastObservedAt":1700521199834,"deadline":1700521199834,"latenessMs":0,
 "message":"Order order-0744518 was still undelivered 48.0 hours after dispatch."}
```

A batch of 200 orders produces roughly seventy breach alerts.

The same command against `shipment-sla-completions` shows the kept promises
(`"outcome":"DispatchedInTime"` and `"outcome":"DeliveredInTime"`).

Both topics are also browsable in the Kafka web user interface at <http://localhost:10680> —
*Topics* → `shipment-sla-breaches` → *Messages*.

### 5. Run the tests

The unit tests cover the deadline arithmetic, the alert texts, the JSON round trip, the
configuration resolution and the fault rules; one more suite runs the *real* pipeline on a local
mini cluster that Flink starts inside the test's own Java virtual machine. None of them needs Docker
or a cluster:

```bash
./mill examples.06-flink-cep-shipment-sla.test
```

## What to try next

- **Tighten the promise until the happy path disappears.** Resubmit with
  `--deliver-within-ms 3600000` (one hour). The generator gives parcels one to two days of travel
  time, so `shipment-sla-completions` loses every `DeliveredInTime` statement and the breach topic
  fills up instead. Loosen it to `--deliver-within-ms 864000000` (ten days) and the reverse happens.
- **Watch a breach wait for the watermark.** Consume `shipment-sla-breaches` while it is idle, then
  run the producer again with `ORDER_COUNT=50`. The new events push the watermark forward, and the
  breaches left pending from the previous batch appear immediately. This is the single most
  surprising property of event-time processing, and it is worth seeing once.
- **Swap `next` for `followedBy` in `dispatchPattern`.** Rebuild, resubmit, and compare the breach
  counts. Orders whose warehouse scan was skipped stop being reported as "never dispatched" — the
  relaxed operator happily skips over the unexpected event.
- **Kill the task manager** with `docker stop de-06-taskmanager` while the job runs, then start it
  again. Flink restarts the job from the last checkpoint and the half-finished pattern matches are
  restored with it, because the partial matches are part of the operator's state.
- **Add a third promise.** A `notFollowedBy` step, for instance, to alert on a `Delivered` scan that
  arrives after the customer already cancelled.

## Clean up

```bash
docker compose -f examples/06-flink-cep-shipment-sla/docker/docker-compose.yml down -v
```
