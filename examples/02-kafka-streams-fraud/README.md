# Example 02 — Card-testing fraud detection with Kafka Streams

This example detects credit-card fraud in a live event stream using **Kafka Streams**, the stream
processing library that ships inside Apache Kafka itself. There is no cluster to install and no
job to submit: a Kafka Streams application is an ordinary program that happens to read from and
write to Kafka topics, so it starts with `java -jar` and scales by starting a second copy.

Along the way it shows the pieces you need for almost any real streaming job: a **KStream** and a
**KTable**, a **stream-stream join** with a join window, a **hopping windowed aggregation** backed
by a **state store**, **suppression** of intermediate results, **custom Serdes** for a domain
model, and an **interactive query** that reads the running application's state directly. The whole
topology is unit tested with `TopologyTestDriver`, so `./mill __.test` passes without Docker
running.

## The use case

"Card testing" is what a fraudster does with a list of stolen card numbers. They do not know which
of the numbers still work, so they place a rapid series of small orders in an online shop and watch
which charges go through. The signature of the attack is therefore not one big suspicious order,
but **many declined payments from the same customer within a couple of minutes**.

The shop's events arrive on two separate topics, and neither of them can answer the question alone:

| Topic           | Key      | What it says                                                 |
| --------------- | -------- | ------------------------------------------------------------ |
| `orders`        | order id | who placed the order (`customerId`), what was in it, when     |
| `payments`      | order id | whether the card was `Authorized`, `Captured` or `Declined`   |
| `customer-risk` | customer | the customer's standing risk tier, newest value wins          |
| `fraud-alerts`  | customer | **output**: a customer who crossed the decline threshold      |

A payment knows the outcome but not the customer. An order knows the customer but not the outcome.
Joining them is the whole point of the first step.

## How it works

All the domain types (`Order`, `Payment`, `Money`, `CustomerId`, …) come from the shared
`common` module (`de.common.domain`), so the messages on these topics are byte-for-byte the same as
in every other example in this repository.

### The files

**`src/de/kafkastreams/fraud/Alerts.scala`** — the vocabulary this example adds, with no Kafka
types in it at all:

- `PaidOrder` — one order joined to its payment, the record that finally knows both *who* and
  *what happened*.
- `DeclineTally` — the running `(count, totalCents)` for one customer inside one window.
- `CustomerRisk` — a customer's standing risk tier.
- `FraudAlert` — the message written to `fraud-alerts`.
- `FraudRules.alertFor` — the actual business rule: "at or above the threshold, raise an alert".
  It is a plain function, which is why `FraudRulesSuite` can test it in microseconds.

**`src/de/kafkastreams/fraud/EventJson.scala`** — turning values into JSON bytes and back.
Writing reuses `de.common.json.Codecs` so every example agrees on the wire format; reading uses
`ujson`, uPickle's small JSON parser. (The shared module cannot depend on a JSON library, because
it is compiled for Scala 2.12, 2.13 and 3 at the same time and no single library version covers all
three.)

**`src/de/kafkastreams/fraud/JsonSerdes.scala`** — the **Serdes**. A Serde ("serializer /
deserializer") is the pair of functions Kafka Streams calls to put a value on a topic or into a
state store and to read it back. Kafka ships Serdes for primitives such as `String` and `Long`
only, so each domain type needs one. `JsonSerde` builds one from an encode function and a decode
function, and handles the `null` case that Kafka uses to mean "deleted".

**`src/de/kafkastreams/fraud/FraudTopology.scala`** — the processing graph, and the file worth
reading twice:

1. **Read the inputs.** `orders` and `payments` become **KStreams**: unbounded, append-only logs
   where every record is an independent fact. `customer-risk` becomes a **KTable**: the same kind
   of log read as a changelog, where a record *replaces* the previous one for its key, so the table
   always holds the current tier and nothing else. Same data on disk, two different readings.

2. **Stream-stream join.** `orders.join(payments, …, JoinWindows.ofTimeDifferenceAndGrace(…))`
   pairs each order with the payment carrying the same order id whose timestamp lies within the
   join window. Both sides are buffered in a state store for the length of the window, which is why
   a stream-stream join window always has to be bounded — otherwise the buffer would grow forever.
   The result is a stream of `PaidOrder`.

3. **Filter and re-key.** Only declines matter. `groupBy` then re-keys the records by customer id.
   Re-keying is what makes "count per customer" possible: Kafka partitions by key, so after the
   re-key every decline of one customer lands in the same partition and therefore in the same
   state store. Kafka Streams inserts an internal *repartition topic* to do this, automatically.

4. **Hopping windowed aggregation.** `windowedBy(TimeWindows.ofSizeAndGrace(size, grace)
   .advanceBy(step))` cuts time into overlapping windows: with a 2-minute size and a 1-minute step,
   a new window opens every minute and each instant belongs to two of them. Overlap matters,
   because a burst that straddles the boundary of a non-overlapping (tumbling) window would be
   split in half and might slip under the threshold. `aggregate` then folds each decline into a
   `DeclineTally` held in a **state store** named `declines-per-customer`.

5. **Suppression.** Without it, downstream sees every intermediate value — the count 1, then 2,
   then 3 — and would alert three times on one burst.
   `suppress(Suppressed.untilWindowCloses(…))` holds the result back until the window plus its
   grace period has passed and emits exactly one final value per window.

6. **Apply the rule.** `flatMap` calls `FraudRules.alertFor`, emitting zero or one `FraudAlert`.

7. **Stream-table join.** `alerts.leftJoin(riskTable, …)` looks up the customer's standing risk
   tier. `leftJoin` rather than `join`, so a customer who has never been seen before still gets an
   alert, tagged `unknown`. This is the everyday use of a KTable: enrichment from a lookup table
   that is itself kept up to date by a stream.

8. **Write the output** to `fraud-alerts`, keyed by customer.

Two non-obvious decisions:

- **Windowing uses the Kafka record timestamp**, not a field inside the JSON. That keeps the
  topology free of a custom `TimestampExtractor`, and it is the producer's job to stamp each record
  with the event time — which `SeedProducer` does explicitly.
- **The state-store cache is switched off** (`STATESTORE_CACHE_MAX_BYTES_CONFIG = 0`). Suppression
  already decides when results are emitted; leaving the cache on would only add a second, less
  predictable delay on top.

**`src/de/kafkastreams/fraud/StoreQueries.scala`** — the **interactive query**. A Kafka Streams
state store is not a black box: the running instance can be asked what it currently holds, which is
how a service answers "how many declines does customer X have right now?" without a database in
between. `recentDeclines` opens the named store read-only and reads the recent windows;
`render` formats the worst offenders for the console.

**`src/de/kafkastreams/fraud/DemoScenario.scala`** — the sample traffic, built as plain values.
`backgroundTraffic` is ordinary seeded shop traffic from the shared `DataGenerator` (roughly one
payment in ten is declined, spread across a thousand customers). `cardTestingBurst` is the attack:
one customer, several small orders seconds apart, every one of them declined.

**`src/de/kafkastreams/fraud/Main.scala`** — the wiring. Builds the topology, configures the
application, starts it, and prints a state-store snapshot every ten seconds.

**`src/de/kafkastreams/fraud/SeedProducer.scala`** — a plain Kafka producer that writes the demo
scenario to the topics in real time, injecting a card-testing burst every twenty rounds.

### The tests

`test/src/de/kafkastreams/fraud/FraudTopologySuite.scala` runs the **complete topology** inside
`TopologyTestDriver`, which executes the real processing graph in the current process: no broker,
no network, no Docker — and, crucially, full control over time. Records go in with an explicit
timestamp, so a test can cover a ten-minute window in a millisecond.

One detail that trips up everybody writing such a test the first time: suppression releases a
window only when *stream time* moves past its end, and stream time only moves when records actually
reach the suppression node. The `advanceStreamTime` helper therefore pushes a **declined** payment
from an unrelated customer far into the future — a `Captured` one would be dropped by the filter
and would never advance the clock where it matters.

## Run it

Everything below is run from the repository root. Host ports stay inside the 10200-10299 range
reserved for this example.

**1. Run the tests. No Docker needed.**

```bash
./mill examples.02-kafka-streams-fraud.test
```

Expect 22 passing tests across four suites.

**2. Start Kafka and the web UI.**

```bash
docker compose -f examples/02-kafka-streams-fraud/docker/docker-compose.yml up -d --wait
```

This starts a single-node Kafka broker in **KRaft mode** (Kafka's own metadata protocol; since
Kafka 4.0 there is no ZooKeeper any more), creates the four topics, and starts a browser UI. The
`--wait` flag makes Compose block until every healthcheck passes, so there is nothing to wait for
manually.

| Service         | Host port | URL / address            |
| --------------- | --------- | ------------------------ |
| Kafka broker    | 10292     | `localhost:10292`        |
| Kafka web UI    | 10280     | <http://localhost:10280> |

**3. Start the fraud detector.**

```bash
./mill examples.02-kafka-streams-fraud.run
```

It prints `Topology started.` and then, every ten seconds, the contents of the state store — empty
at first.

**4. In a second terminal, produce traffic.**

```bash
./mill examples.02-kafka-streams-fraud.runMain de.kafkastreams.fraud.SeedProducer 400
```

Every twenty rounds it logs `injecting a card-testing burst from cust-probe-01`.

**5. Watch the alerts.** Within about two minutes (one window plus its grace period) the first
terminal starts listing `cust-probe-01` at the top of the state-store snapshot, and an alert
appears on the `fraud-alerts` topic:

```bash
docker exec de-02-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fraud-alerts --from-beginning --timeout-ms 15000
```

```json
{"customerId":"cust-probe-01","declinedCount":55,"totalDeclinedCents":10945,
 "windowStart":1788565440000,"windowEnd":1788565560000,"riskTier":"watchlist"}
```

`riskTier` is `watchlist` rather than `unknown`, which proves the stream-table join found the
customer in the `customer-risk` table. The same messages are visible in the web UI at
<http://localhost:10280> under **Topics → fraud-alerts → Messages**.

If Kafka is not on the default address, set `KAFKA_BOOTSTRAP_SERVERS` before running either
program.

## What to try next

- **Turn the hop off.** In `FraudTopology.defaultConfig`, set `advanceBy` equal to `windowSize`.
  The windows stop overlapping, each burst is reported once instead of twice — and a burst that
  straddles a window boundary can now slip under the threshold. Add such a case to
  `FraudTopologySuite` and watch it fail with the tumbling configuration.
- **Remove the suppression.** Delete the `.suppress(...)` line and re-run
  `FraudTopologySuite`. The "exactly one alert" test now sees one alert per decline past the
  threshold, which is exactly the noise real alerting systems drown in.
- **Shrink the join window** to `Duration.ofMillis(1)`. Orders and payments no longer pair up, and
  the alerts stop entirely — a good illustration of how a join window that is too tight silently
  loses data rather than erroring.
- **Restart the application** while the producer keeps running. Kafka Streams restores its state
  stores from their changelog topics, so the counts survive the restart. Watch the log lines about
  restoring state.
- **Start a second instance** in a third terminal. The two instances split the partitions between
  them, and each one can then only answer interactive queries about the customers it owns — the
  moment a distributed streaming application starts needing a way to route queries to the right
  instance.

## Clean up

```bash
docker compose -f examples/02-kafka-streams-fraud/docker/docker-compose.yml down -v
```

The `-v` flag also removes the broker's data volume, so the next `up` starts from an empty cluster.
The application's local state directory lives under the system temporary directory as
`de-02-kafka-streams-fraud` and can be deleted as well.
