# 14 - Apache HugeGraph: finding fraud rings in a graph

A **graph database** stores data as things and the connections between them, and it is built so that following a
connection is cheap no matter how many things there are. [Apache HugeGraph](https://hugegraph.apache.org) is one:
an open-source, distributed graph database that speaks **Gremlin**, the query language of Apache TinkerPop.

This example loads the online shop of this repository into HugeGraph as a graph - customers, orders, payment cards,
devices and shipping addresses - and then asks the question that is nearly impossible to write in SQL and almost
trivial to write here: *which groups of accounts are secretly the same person?*

Everything the example does is reachable over HugeGraph's HTTP interface, so the Scala 3 program needs no vendor
driver at all: an HTTP client, a JSON library, and about two hundred lines of pure logic.

## The use case

Look at any single order in the shop and it is fine. A customer, a card, an address, a plausible amount. Fraud does
not show up in one row; it shows up in the *pattern across rows*:

- Four accounts with different names, different emails, different addresses - all charging the same stolen card.
- Three accounts created within an hour of each other from the same phone, each claiming a first-order discount.
- Three accounts shipping to one flat, which is where the goods are collected before they are resold.

Each of those is a **fraud ring**: a set of accounts that a human operator can see are one operation, because they
share something they should not. The shared thing is the evidence, and the group is the finding.

This example plants exactly three such rings in otherwise ordinary generated traffic, then finds them again from the
graph without being told where they are. Two of the three planted rings deliberately overlap in one account, so they
are really one ring of five - and noticing that is the part a hand-written query gets wrong.

## Why a graph, and not SQL

Suppose the orders live in one relational table. Finding accounts that share a card is a self-join:

```sql
SELECT a.customer_id, b.customer_id, o1.card_id
FROM orders AS o1
JOIN orders AS o2 ON o1.card_id = o2.card_id AND o1.customer_id <> o2.customer_id
JOIN customers AS a ON a.customer_id = o1.customer_id
JOIN customers AS b ON b.customer_id = o2.customer_id;
```

That is already awkward, and it only covers *cards*. Devices and addresses need the same join twice more, `UNION`ed
together. Worse, it only finds accounts that share something **directly**. In this example `cust-0201` and
`cust-0302` share nothing at all: no card, no device, no address. They are the same operation only because
`cust-0203` used the device of one group and the drop address of the other. Catching that in SQL means joining the
result to itself again, and again, without knowing in advance how many times is enough. The honest SQL answer is a
recursive common table expression (`WITH RECURSIVE`), which most analysts will not write and most planners will not
optimise well.

In a graph the same question has no depth built into it:

```groovy
// every account reachable from cust-0201 by following any edges, up to four hops
g.V('cust-0201').repeat(__.both().simplePath()).times(4).emit().hasLabel('customer').dedup()
```

and "which accounts share this card" is one step, not a join:

```groovy
g.V('card-stolen-1').in('paid_with').in('placed').values('customer_id').dedup()
```

The difference is not syntax sugar. In a relational engine a join is a lookup by value that consults an index every
time. In a graph database the connection is stored *with* the vertex, so following it is a pointer hop whose cost
does not grow with the size of the database. That property is why "how are these two accounts related?" is a graph
question.

## How it works

### The graph model

Five kinds of vertex (a **vertex** is one thing) and four kinds of edge (an **edge** is one connection):

```
  customer ──placed──▶ order ──paid_with───▶ card
                         │
                         ├──placed_from──▶ device
                         └──ships_to─────▶ address
```

The order is the hub. A customer never points at a card directly; both point at the order. Two customers who share a
card are therefore four hops apart - customer, order, card, order, customer - and that number, four, is the depth
every "related accounts" query in this example uses.

### The files

| File | What it does |
| --- | --- |
| `PropertyGraph.scala` | A storage-independent description of a graph: `Vertex`, `Edge`, and the undirected adjacency view the algorithms read. Knows nothing about HugeGraph. |
| `FraudSchema.scala` | The schema as data: property keys, vertex labels, edge labels and index labels, plus the label and property names used everywhere else. |
| `Accounts.scala` | Who uses which card, device and address. Every customer gets private artefacts derived from their identifier; a `RingPlan` then makes a chosen handful share one. |
| `ShopGraph.scala` | The modelling step: orders from the shared `DataGenerator` plus an artefact assignment, in, property graph out. Pure. |
| `RingDetection.scala` | The analysis in plain Scala: connected components, k-hop neighbourhood, shortest path, degree centrality. This is what the unit tests exercise. |
| `GremlinQueries.scala` | The same questions written as Gremlin, built as strings with parameters kept out of the script. |
| `Payloads.scala` | The JSON bodies of every HugeGraph endpoint used, rendered from the values above. |
| `HugeGraphClient.scala` | The only file that performs input/output: sttp requests against the HugeGraph HTTP interface. |
| `Report.scala` | Turns results into console lines. |
| `Main.scala` | Wiring. Prints the in-memory answer, then the server's answer to the same questions. |

### Declaring the schema

HugeGraph will not accept data before a schema exists, which is unusual for a graph database and much closer to how a
relational database behaves. The schema has four kinds of element, created in this order because each refers to the
previous:

1. **Property keys** - the typed fields. `POST /graphs/hugegraph/schema/propertykeys` with
   `{"name":"total_cents","data_type":"LONG","cardinality":"SINGLE"}`.
2. **Vertex labels** - the kinds of thing, listing which property keys they may carry. Every label here uses the
   `CUSTOMIZE_STRING` identifier strategy, meaning the loader supplies the identifier (`cust-0101`, `card-stolen-1`)
   instead of the database generating one. That makes the load **idempotent**: sending the same vertex twice updates
   it rather than creating a duplicate, so running the program again is harmless.
3. **Edge labels** - the kinds of connection, naming the vertex label at each end. `frequency: SINGLE` means at most
   one edge of that label between a given pair.
4. **Index labels** - the indexes. A `SECONDARY` index answers an equality lookup such as `has('country', 'PL')`.
   Without one, that lookup would scan every vertex.

`FraudSchema.scala` holds all of this as ordinary case classes and `Payloads.scala` renders them, which is why
`PayloadsSuite` can check the exact JSON without a server running.

### Loading the data

Two endpoints, each taking a JSON array: `POST .../graph/vertices/batch` and `POST .../graph/edges/batch`. Vertices
must go first, because an edge names its endpoints and HugeGraph rejects an edge whose vertices do not exist yet.
The client sends them 200 at a time, which keeps every request under the server's batch limit.

### Asking the questions

The example uses both ways HugeGraph offers to traverse a graph.

**Gremlin**, at `POST /gremlin`. A traversal reads left to right as a walk. This one finds every card used by more
than one account, and lists those accounts:

```groovy
g.V().hasLabel('card')
  .where(__.in('paid_with').in('placed').dedup().count().is(gte(2)))
  .project('artefact', 'customers')
    .by(id())
    .by(__.in('paid_with').in('placed').values('customer_id').dedup().order().fold())
```

`__.in('paid_with')` walks *backwards* along an edge - from the card to the orders charged to it. `project` builds a
small record with one `by` clause per named field. Values such as an account identifier are sent as **bindings**,
named parameters the server substitutes, for the same reason SQL uses prepared statements: a value can never turn
into code.

**The built-in traversers**, under `/graphs/hugegraph/traversers/`. HugeGraph ships ready-made endpoints for the
walks people ask for most: `kout` (everything exactly *k* hops away) and `shortestpath` (the shortest connection
between two vertices) are the two used here. They compute the same answers a Gremlin script would, but they run as
tuned Java inside the server rather than as an interpreted script, and they take an explicit hop and result budget so
a mistake cannot walk the entire graph.

### The in-memory twin

Every query has a plain-Scala counterpart in `RingDetection.scala`, and `Main` prints both. This is not duplication
for its own sake:

- The tests need no Docker. `./mill __.test` passes on a machine with the daemon stopped.
- Ring detection stops being magic. A "fraud ring" is a **connected component** - a set of accounts reachable from
  one another through shared artefacts - found by breadth-first search over a map. Reading twenty lines of that is
  worth more than reading a paragraph about graph databases.
- Running both against the same data is a check on the modelling. When the program prints the same six-hop path
  twice, once from a `Map[String, Set[String]]` and once from HugeGraph, the schema and the queries agree.

### Two things worth knowing about the plumbing

Both were found by running this, and both are commented in the code:

- **The HTTP client is pinned to HTTP/1.1.** Java's built-in HTTP client offers to upgrade every connection to
  HTTP/2. HugeGraph 1.5.0 accepts that offer on `/gremlin` and then never answers, until its own query timeout fires
  and reports a `TimeoutException` - which looks exactly like a slow query and is not one. Every other endpoint is
  unaffected, which makes it a confusing failure to meet unprepared.
- **The Hubble console needs its metrics switched off.** Hubble 1.5.0 bundles a Spring Boot version whose
  machine-load metrics fail to initialise inside a container on a current Linux kernel, and the failure takes the
  whole web application down. The compose file disables those collectors through `SPRING_APPLICATION_JSON`.

## Run it

From the repository root.

**1. Start HugeGraph and its console.** The first start initialises the storage engine and takes about half a
minute; `--wait` returns only once both services report healthy.

```bash
docker compose -f examples/14-hugegraph-fraud-ring/docker/docker-compose.yml up -d --wait
```

| Service | Host port | What it is |
| --- | --- | --- |
| `hugegraph` | `11400` | The database. The whole HTTP interface lives here: <http://localhost:11400/versions> |
| `hubble` | `11401` | HugeGraph Hubble, the web console: <http://localhost:11401> |

**2. Run the example.**

```bash
./mill examples.14-hugegraph-fraud-ring.run
```

It prints the in-memory analysis first, then creates the schema, loads the graph and asks HugeGraph the same
questions. The interesting parts of the output:

```
Fraud rings found in memory
---------------------------
 5 accounts  cust-0201, cust-0202, cust-0203, cust-0301, cust-0302   shared: address-drop-1, device-farm-1
 4 accounts  cust-0101, cust-0102, cust-0103, cust-0104              shared: card-stolen-1

Artefacts shared by several accounts (Gremlin)
----------------------------------------------
card-stolen-1            used by  4 accounts: cust-0101, cust-0102, cust-0103, cust-0104
device-farm-1            used by  3 accounts: cust-0201, cust-0202, cust-0203
address-drop-1           used by  3 accounts: cust-0203, cust-0301, cust-0302

Shortest path cust-0201 -> cust-0302 (built-in shortestpath traverser)
----------------------------------------------------------------------
6 hops
cust-0201 -> order-0087219 -> device-farm-1 -> order-0250657 -> address-drop-1 -> order-0030640 -> cust-0302
```

The last block is the point of the whole example. `cust-0201` and `cust-0302` share nothing. The database found the
chain that connects them anyway.

**3. Run the tests.** They need no Docker at all - stop the stack first if you want to prove it.

```bash
./mill examples.14-hugegraph-fraud-ring.test
```

**4. Look at the graph.** Open <http://localhost:11401>, create a graph connection with:

| Field | Value |
| --- | --- |
| Graph name | `hugegraph` |
| Host | `hugegraph` (the service name; Hubble connects from inside the Docker network) |
| Port | `8080` |

Then open *Analysis* and run a query, for example:

```groovy
g.V('card-stolen-1').both().both()
```

Hubble draws the result, and a ring looks like what it is: a hub with several accounts hanging off it.

## What to try next

- **Ask the shortest-path question the other way.** Change `probeAccounts` in `Main.scala` to two accounts from
  *different* rings, for example `("cust-0101", "cust-0302")`, and both halves will agree that there is no path.
  There genuinely is not one: the card ring is a separate operation.
- **Add a fourth ring, or make an existing one bigger.** `ShopGraph.defaultRingPlans` is a plain list. Add a
  `RingPlan` that shares a card *and* an address, re-run, and watch the component grow. The tests in
  `RingDetectionSuite` show how to state an expected answer for a new shape.
- **Change the hop budget.** `relatedAccounts(..., depth = 4)` is what makes "shares an artefact" the definition of
  related. Raise it to 8 and accounts two artefacts apart appear too; the result grows fast, which is a useful thing
  to feel rather than to be told.
- **Query the graph by property instead of by identifier.** `g.V().hasLabel('customer').has('country', 'PL')` uses
  the `customerByCountry` index the schema declares. Drop that index label from `FraudSchema.shop`, reload into a
  fresh stack, and HugeGraph will refuse the query rather than silently scanning - a deliberate design choice worth
  meeting once.
- **Kill the database mid-run.** `docker compose ... stop hugegraph` while the program is loading. The client throws
  a `HugeGraphException` naming the endpoint and the status; the in-memory half of the report has already printed,
  because it never needed the server.

## Clean up

```bash
docker compose -f examples/14-hugegraph-fraud-ring/docker/docker-compose.yml down -v
```

`-v` also removes the named volume holding the RocksDB files, so the next start begins with an empty graph.
