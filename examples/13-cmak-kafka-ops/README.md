# 13 - Operating Kafka: CMAK and the AdminClient

This example is about running an Apache Kafka cluster rather than writing a pipeline on top of one. It starts a
three-broker cluster, puts a web console next to it - **CMAK**, the Cluster Manager for Apache Kafka, formerly Kafka
Manager, from Yahoo - and then does the same operational jobs from Scala code through Kafka's `AdminClient` interface:
create topics with a deliberate partition count and replication factor, measure how far a consumer group is behind,
read and change a topic's retention, check that every partition is fully replicated, and move partitions between
brokers.

The point of doing both is that the console and the code are two views of the same thing. Anything CMAK shows you can
ask for from a program, and anything you script can be checked by eye in the browser.

## The use case

The shared online-shop domain of this repository has an order pipeline: `shop.orders`, `shop.payments` and
`shop.shipments`. The pipeline is already live, so this is what operations people call **day two** - the work that
starts after the first deployment and never ends:

- Someone asks for a new topic. How many partitions, and how many copies of each?
- The nightly reporting job is behind. By how much, and on which partition?
- Storage is filling up. Can retention on `shop.orders` come down from seven days to three, without disturbing the
  other settings on that topic?
- A broker was rebooted for a kernel patch. Is every partition fully replicated again?
- A fourth broker was added to the rack. Which partitions should move onto it, and how do we move them without copying
  the whole topic across the network for nothing?

Each of those is one step of the program in `Main.scala`, printed in order.

## Why this example runs ZooKeeper, and the others do not

Apache Kafka used to keep its cluster metadata - which brokers exist, which topics exist, where each partition lives -
in **Apache ZooKeeper**, a separate coordination service. Kafka 3 introduced **KRaft** (Kafka Raft), which moves that
metadata inside Kafka itself, and Kafka 4 removed ZooKeeper support entirely. Every other Kafka example in this
repository therefore runs a KRaft cluster with no ZooKeeper container at all.

CMAK reads cluster metadata straight out of ZooKeeper and has never been ported to KRaft, so a KRaft cluster is
invisible to it. This example consequently runs the older shape: one ZooKeeper, and three brokers from Confluent
Platform 7.9 (Kafka 3.9), the last release line whose brokers still accept a ZooKeeper connection.

That is not purely a cost. Three brokers plus ZooKeeper is exactly the setup in which replication, in-sync replica
sets and partition reassignment are demonstrable rather than theoretical - which is why this example, and not another,
is the one that shows them.

One more fitting detail: CMAK is itself a Scala application, built on the Play Framework. The console and the code in
this folder are written in the same language.

## How it works

### The pure core - no cluster required

These four files contain no Kafka class at all. They take numbers and give back answers, which is why the whole of the
`test/` folder runs without Docker.

- **`TopicPlan.scala`** - what an operator wants a topic to look like: name, partition count, replication factor and
  the per-topic settings such as `retention.ms`. `TopicPlan.orderPipeline` is the plan for the three shop topics.
  `validateAgainstCluster` catches the two mistakes that are only discovered painfully at runtime: asking for more
  replicas than there are brokers, and setting `min.insync.replicas` above the replication factor, which makes every
  `acks=all` write fail forever.
- **`ConsumerLag.scala`** - lag is the distance between the end of a partition's log and the offset a consumer group
  has committed. The subtraction is trivial; the two edge cases are not. A group that has never committed is reported
  as behind by the whole partition, and a committed offset that appears to be *ahead* of the log end - which happens
  because the two numbers are read a few milliseconds apart - is clamped to zero instead of reported as negative lag.
- **`Replication.scala`** - two things. `PartitionReplicaState` and `ReplicationHealth` answer "is this partition
  still safe": under-replicated (a copy has fallen behind), offline (no copy can lead), or led by a replica that is not
  its preferred leader. `ReassignmentPlanner` decides where copies *should* live, mirroring the round-robin-with-shift
  algorithm Kafka uses when it creates a topic.
- **`Reports.scala`** - every line the example prints. Keeping rendering separate from both the calculation and the
  cluster access is what allows the output to be asserted on character by character in `ReportsSuite`.

### The wiring - talks to a real cluster

- **`KafkaOps.scala`** - the only file that touches Apache Kafka's `Admin` interface. `AdminClient` is the Java API for
  administration, and the `kafka-topics.sh` / `kafka-consumer-groups.sh` scripts in a Kafka distribution are thin
  wrappers around exactly these calls. Every method here translates Kafka's types into the plain data types above and
  waits for the asynchronous `KafkaFuture` it gets back, because an operator script is a sequence of steps with nothing
  useful to do in between. Two choices worth noting: `incrementalAlterConfigs` is used rather than the older
  `alterConfigs`, which replaced a topic's *entire* configuration and silently reset anything the caller forgot to
  repeat; and `createMissingTopics` never touches a topic that already exists, because changing a live topic's
  partition count re-routes keys to different partitions.
- **`PipelineTraffic.scala`** - writes 500 orders from the shared `DataGenerator` and then reads 120 of them back with
  a consumer group that commits and leaves. Stopping early is deliberate: a console full of empty topics teaches
  nothing, and lag is only interesting once a group has committed somewhere in the middle.
- **`Main.scala`** - the seven steps, in order, each printing what it found before it changes anything.

### The non-obvious decision in the reassignment planner

Kafka picks a **random starting broker** for each topic it creates, so that several small topics do not all pile their
partition 0 onto the same machine. A planner that compares the current layout against one fixed rotation would
therefore declare every partition misplaced on a perfectly balanced topic, and copy the entire topic across the network
to fix nothing. `planExpansion` tries every rotation and keeps the one that moves the fewest partitions, so a balanced
topic yields an empty plan and a genuinely unbalanced one yields the smallest plan that fixes it. The plan is printed
as `old brokers -> new brokers` before it is applied, and because the planner is a pure function, what you read is
exactly what is sent.

## Run it

Start the stack from the repository root. The first run pulls four images (for six containers), so give it a few
minutes.

```bash
docker compose -f examples/13-cmak-kafka-ops/docker/docker-compose.yml up -d --wait
docker compose -f examples/13-cmak-kafka-ops/docker/docker-compose.yml run --rm cmak-register
```

`--wait` returns only once every healthcheck passes, so there is nothing to wait for by hand. A one-shot
`cmak-register` command then posts the cluster into CMAK and exits. It is an explicit batch
operation in the `setup` profile, so a completed registration cannot make `up --wait` fail.

Open the console at **<http://localhost:11380>**. The cluster is already registered as `de-13-orders`; click it to see
brokers, topics and consumer groups. The other host ports are:

| Port    | Service                                                     |
| ------- | ----------------------------------------------------------- |
| `11301` | Kafka broker 1 (bootstrap)                                  |
| `11302` | Kafka broker 2 (bootstrap)                                  |
| `11303` | Kafka broker 3 (bootstrap)                                  |
| `11380` | CMAK web console                                            |
| `11381` | ZooKeeper client port                                       |

Now run the operational tour:

```bash
./mill examples.13-cmak-kafka-ops.run
```

It prints seven sections. Abbreviated, the output looks like this:

```
1. cluster membership
---------------------
  brokers: 1, 2, 3
  existing user topics: none

2. topics of the order pipeline
-------------------------------
  shop.orders: 6 partitions x 3 replicas (18 partition copies in total)
    cleanup.policy = delete
    min.insync.replicas = 2
    retention.ms = 604800000
    unclean.leader.election.enable = false
  ...
  created: shop.orders, shop.payments, shop.shipments

4. consumer group lag
---------------------
  consumer group 'shop.orders.reporting' is 380 record(s) behind
    shop.orders-0            end=84       committed=46              lag=38
    shop.orders-1            end=88       committed=0               lag=88
    ...
    furthest behind: shop.orders-1 (88 record(s))

6. replication health
---------------------
  all 12 partition(s) are fully replicated
    broker 1 holds 12 partition copy/copies
    broker 2 holds 12 partition copy/copies
    broker 3 holds 12 partition copy/copies
```

A second group named `KMOffsetCache-<something>` also appears in section 4. That is CMAK's own consumer, which it uses
to read the internal offsets topic; it is a real part of running a console and is left visible rather than filtered out.

Run it again and the numbers change: the topics already exist, the group falls further behind because more orders were
produced, and the reassignment plan is empty because the cluster is already balanced.

The tests are pure and need none of the above:

```bash
./mill examples.13-cmak-kafka-ops.test
```

## What to try next

**Break a broker on purpose.** With the stack up, stop one:

```bash
docker stop de-13-kafka-3
sleep 15
./mill examples.13-cmak-kafka-ops.run
```

Section 6 now reports all twelve partitions as under-replicated and names broker 3 as the missing copy. Nothing is
*lost*: each partition still has two in-sync replicas, which is exactly what `min.insync.replicas=2` requires, so
producers using `acks=all` keep working. Reload CMAK's topic page and it shows the same thing in red. Bring the broker
back with `docker start de-13-kafka-3`, wait half a minute, and run again - the copies catch up on their own.

**Take it one broker further.** Stop a second broker and run the tour again. Now only one in-sync replica is left,
`min.insync.replicas=2` can no longer be met, and the producer in step 3 fails rather than writing to a partition it
cannot make durable. That is the setting doing its job: the pipeline stops rather than quietly accepting data it might
lose.

**Change a topic's shape from the console and read it back from code.** In CMAK, use *Topic -> shop.payments -> Update
Config* to set `retention.ms` to something small, then run the example and watch section 5 read your value before
overwriting `shop.orders`. It is the same `incrementalAlterConfigs` call underneath.

**Even out the leaders.** After a broker restart, section 6 often ends with "N partition(s) are not led by their
preferred replica": leadership stayed where it moved to during the outage, so one broker is doing more work than the
others. CMAK's *Preferred Replica Election* button on the cluster page hands leadership back to the first replica of
each partition.

**Watch a reassignment actually take time.** Produce far more data first - raise `count = 500` in
`Main.showTraffic` to something like 200000 - then stop a broker, restart it, and trigger the reassignment. Section 7
will report partitions still moving, because copying a large log between brokers is not instantaneous.

## Clean up

```bash
docker compose -f examples/13-cmak-kafka-ops/docker/docker-compose.yml down -v
```

`-v` also deletes the three broker volumes, so the next `up` starts from an empty cluster.
