# 01 - Exactly-once processing with the plain Apache Kafka client

Apache Kafka is a distributed log: producers append records to named, partitioned
topics, and consumers read them back at their own pace. This example shows how to
read from one topic, write to another, and guarantee that each input record
affects the output **exactly once** - even if the process is killed halfway
through a batch.

It deliberately uses the plain Apache Kafka **Java client** (`KafkaConsumer` and
`KafkaProducer`) rather than a higher-level library, because the whole point is
to see the six calls that make exactly-once work with nothing hiding them:
`initTransactions`, `beginTransaction`, `send`, `sendOffsetsToTransaction`,
`commitTransaction` and `abortTransaction`. Around that loop it uses
[Ox](https://ox.softwaremill.com), a direct-style Scala library for structured
concurrency, so that pressing Ctrl+C shuts the clients down in a defined order
instead of leaving them dangling.

## The use case

A payment-settlement service in the shared online-shop domain of this
repository.

Customers place orders, which land on the `orders` topic. The settlement service
reads each order, works out the amount to charge, and writes a record to the
`payments` topic. Charging a customer twice for one order is the failure this
example exists to prevent - it is the kind of bug that turns into refunds,
chargebacks and support tickets.

The naive version of this service has a gap you cannot close by trying harder:

1. it reads order `order-42`,
2. it writes the payment,
3. it commits its read position back to Kafka,
4. and it is killed between steps 2 and 3.

On restart it reads `order-42` again, because as far as Kafka knows it was never
processed, and writes a second payment. Kafka transactions close that gap by
making steps 2 and 3 a single atomic operation.

## How it works

### The pure core, testable without a broker

| File | What it does |
| --- | --- |
| `src/de/kafka/eos/Settlement.scala` | The business rule. Given an `Order` and a timestamp, it returns the `Payment` to charge, or a typed reason it refuses (no order lines, a non-positive total, a currency this provider does not settle). No Kafka anywhere. |
| `src/de/kafka/eos/OrderJson.scala` | Reads the JSON (JavaScript Object Notation) layout that the shared `de.common.json.Codecs` writes, back into an `Order`. A malformed record is returned as a `Left`, not thrown: bad records are an expected event on a real topic. |
| `src/de/kafka/eos/TransactionalSettlement.scala` | The loop body, and the heart of the example. It takes one polled batch and a `SettlementTransaction`, and returns a `BatchOutcome` of `Empty`, `Committed` or `Aborted`. |
| `src/de/kafka/eos/Settings.scala` | Topic names, broker addresses and identifiers as Scala 3 *opaque types*. At run time each is a plain `String`; at compile time you cannot pass a `TopicName` where a `TransactionalId` belongs. |

`SettlementTransaction` is a five-method interface - `begin`, `emit`, `commit`,
`abort` and `rewindTo`. That indirection is what makes the guarantee testable: the unit tests
hand `settleBatch` a recording stand-in and assert on the whole story ("began,
emitted one payment, then aborted"), with no broker and no docker involved.

### The Kafka wiring

| File | What it does |
| --- | --- |
| `src/de/kafka/eos/KafkaClients.scala` | Builds the consumer and the two producers, with every exactly-once-relevant setting written out and commented. |
| `src/de/kafka/eos/KafkaSettlementTransaction.scala` | The only file that touches the transaction API (application programming interface). It implements `SettlementTransaction` and contains no business logic. |
| `src/de/kafka/eos/SettlementService.scala` | Turns Kafka's Java `ConsumerRecords` into plain values, runs the loop, and decides when it ends. |
| `src/de/kafka/eos/OrderSeeder.scala` | Publishes generated orders from `de.common.gen.DataGenerator` to the `orders` topic. |
| `src/de/kafka/eos/AbortDemo.scala` | Writes one payment inside a transaction, aborts it, then reads the topic tail twice - once per isolation level - and reports what each could see. |
| `src/de/kafka/eos/Main.scala` | The command line (`seed`, `settle`, `abort-demo`) and the Ox wiring. |

### The four settings that actually do the work

Everything else in `KafkaClients.scala` is bookkeeping; these four are the
guarantee.

- **`transactional.id` on the producer.** A stable name for this service across
  restarts. When a producer with that name calls `initTransactions`, the broker
  *fences* every older producer holding it - a hung instance from before the
  crash can no longer write anything. Setting it also switches on
  `enable.idempotence`, which makes the broker discard a retried duplicate of a
  record it already stored.
- **`enable.auto.commit=false` on the consumer.** By default the client commits
  your read position on a timer, in the background, with no relation to whether
  you finished processing. That timer is precisely the gap described above, so
  it is switched off.
- **`sendOffsetsToTransaction`.** Instead of committing the read position
  separately, the consumer's offsets are handed to the *producer*, which writes
  them into Kafka's internal offsets topic as part of the same transaction as
  the payment records. One commit, both effects.
- **`isolation.level=read_committed` on the consumer.** Records belonging to an
  aborted or still-open transaction are withheld from the consumer. Without it,
  a downstream reader would see payments that were never really made.

### Aborting on purpose

`settleBatch` emits payments one at a time and stops at the first record it
cannot settle. If that happens after some payments were already sent, it calls
`abort()`: the transaction is thrown away, and those payments never become
visible to a `read_committed` reader. This is the same code path a crash would
take, only deliberate, which is what makes it demonstrable.

It then calls `rewindTo()`, which is the half of "abort" that is easy to forget.
Aborting discards the output, but the consumer has already moved its own
in-memory read position past those records. Without an explicit `seek` back, the
inputs would be silently skipped for the rest of the process's life - the
pipeline would lose records rather than duplicate them, which is a quieter bug
and a worse one.

### Graceful shutdown

`Main` extends Ox's `OxApp`, which gives the program a root structured-concurrency
scope. The Kafka clients are registered with `useCloseableInScope`. When you
press Ctrl+C, Ox interrupts the fork running the loop; the Kafka client reports
that as `InterruptException`, the loop returns normally, and the clients are
closed in reverse order of acquisition - so the consumer leaves its group
cleanly instead of making the group wait out a session timeout.

## Run it

The example needs a Java 21 runtime, because Ox is built on virtual threads.
Mill downloads one automatically (`jvmVersion` in `package.mill`); the compiled
bytecode still targets Java 17 like the rest of the repository.

**1. Start Kafka.** From the repository root:

```bash
docker compose -f examples/01-kafka-clients-exactly-once/docker/docker-compose.yml up -d --wait
```

That starts a single-node Kafka in KRaft mode (KRaft is Kafka's own metadata
protocol, which replaced Apache ZooKeeper, so one container is a whole cluster),
creates the `orders` and `payments` topics with three partitions each, and starts
a web user interface (UI).

`--wait` covers the long-running services, but topic creation is a one-shot job.
Verify it separately before seeding:

```bash
docker compose -f examples/01-kafka-clients-exactly-once/docker/docker-compose.yml \
  ps --all topic-setup
```

Its state must be `Exited (0)`; any other exit code means the topics are not ready.

- Kafka, for clients on your machine: `localhost:10192`
- Kafka UI, in a browser: <http://localhost:10180>

**2. Seed the input topic** with twelve generated orders:

```bash
./mill examples.01-kafka-clients-exactly-once.runMain de.kafka.eos.Main seed 12
```

```
published 12 orders to orders at localhost:10192
first order id: order-0392763
```

The generator is seeded, so you get those same twelve orders every time.

**3. Run the settlement service:**

```bash
./mill examples.01-kafka-clients-exactly-once.runMain de.kafka.eos.Main settle
```

```
settling orders from orders into payments; Ctrl+C to stop
committed 10 payment(s): order-0392763=170.88 EUR, order-0412737=260.24 EUR, ...
committed 2 payment(s): order-0204059=483.73 EUR, order-0427573=183.44 EUR
```

It then sits idle and prints nothing until new orders arrive. Press Ctrl+C to
stop it; it prints `settlement service stopped` on the way out.

**4. Watch an aborted transaction disappear:**

```bash
./mill examples.01-kafka-clients-exactly-once.runMain de.kafka.eos.Main abort-demo
```

```
one payment was written inside a transaction and then aborted
a read_uncommitted consumer sees 1 record(s); a read_committed consumer sees 0 record(s)
```

The record is physically on the log either way - aborting erases nothing - but
the broker refuses to hand it to a `read_committed` consumer.

**5. Run the tests.** They cover the business rule, the JSON reader and the
transactional loop, and need no broker at all:

```bash
./mill examples.01-kafka-clients-exactly-once.test
```

## What to try next

- **Prove it does not double-charge.** Run `settle`, let it finish, stop it, and
  run it again. The second run charges nobody, because the offsets were committed
  inside the same transaction as the payments. Count the payments to confirm:
  ```bash
  docker exec de-01-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic payments --from-beginning \
    --isolation-level read_committed --timeout-ms 6000 | wc -l
  ```
- **Kill it mid-batch.** Seed a few hundred orders, start `settle`, and kill the
  process hard (`kill -9`) while it is working. Restart it and count the payments:
  the total still matches the number of orders.
- **Break the isolation level.** In `Main.settle`, change
  `IsolationLevel.ReadCommitted` to `IsolationLevel.ReadUncommitted`, then re-run
  `abort-demo` followed by the count above. The abandoned payment now appears -
  which is the whole reason the default in this example is `read_committed`.
- **Feed it a bad record and watch the abort.** Publish something that is not an
  order and see the whole batch roll back:
  ```bash
  echo "this is not an order" | docker exec -i de-01-kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
  ```
  The service prints `aborted the transaction, nothing charged: ...` once per
  poll, forever. That is the honest behaviour of this design: the offsets were
  never committed and the reader was rewound, so the same batch comes back on the
  next poll and a permanently bad record stalls its partition until you decide
  what to do with it. (Routing it to a "dead letter" topic is the usual answer,
  and is out of scope here.) Stop the service with Ctrl+C when you have seen
  enough.
- **Inspect the topics** in the UI at <http://localhost:10180> - partitions,
  offsets, individual records, and the consumer group's committed positions.

## Clean up

```bash
docker compose -f examples/01-kafka-clients-exactly-once/docker/docker-compose.yml down -v
```

`-v` also removes the named volume holding Kafka's log directory, so the next
`up` starts from an empty cluster.

## Ports

Every host port of this example lives in the 10100-10199 range reserved for
example 01, so the stack runs alongside every other example in this repository.

| Host port | Service | Container port |
| --- | --- | --- |
| 10180 | Kafka UI | 8080 |
| 10192 | Kafka, host listener | 10192 |
