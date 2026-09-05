# 15 - Apache CouchDB change data capture into Apache Kafka

This example follows a database's own feed of changes and republishes every one of them onto a message
broker. The database is **Apache CouchDB**, a document database that stores JSON documents and keeps a
durable, ordered record of every write to them. The broker is **Apache Kafka**. The technique has a name:
**change data capture**, usually shortened to CDC - instead of repeatedly asking a database "what does the
data look like now?", a service reads the database's log of changes and turns each one into an event.

Along the way the example shows the CouchDB ideas you need in order to do this correctly: documents and
their revisions (`_rev`), what a *conflict* is, the `_changes` feed in *continuous* mode, checkpointing the
last sequence number so a restart resumes instead of replaying everything, Mango queries and design
documents for reading the data back, and how a deleted document becomes a Kafka **tombstone** on a
compacted topic.

The code is Scala 3 written in *direct style*: calls block and return values, with no `Future` or `IO`
wrapper. [Ox](https://github.com/softwaremill/ox) provides structured concurrency and clean shutdown, and
[sttp](https://sttp.softwaremill.com) is the HTTP client - CouchDB has no binary protocol and no driver, so
an HTTP client is the only dependency it needs.

## The use case

The online shop of this repository keeps its **product catalogue** in CouchDB: one document per product,
identified by its stock keeping unit (SKU), holding a name, a category, a price and whether the product is
in stock. Merchandisers edit it all day - a price changes, an item sells out, a discontinued item is
deleted.

The streaming examples elsewhere in this repository work with *orders*, and an order line only carries a
SKU. To turn `SKU-KETTLE` into "Gooseneck kettle, 79.00 EUR", those jobs need the catalogue as a Kafka
topic they can join against. This example is the connector that gets it there: it follows the catalogue's
change feed and publishes every change to `catalogue.products`, keyed by SKU, on a **compacted** topic - a
topic on which Kafka keeps the latest record per key for ever, so a consumer starting tomorrow can replay
the topic and rebuild the whole catalogue.

## How it works

### The CouchDB ideas, in one paragraph each

**Documents and revisions.** Every document has an `_id` and a `_rev` such as `2-8f4c…`. The number in
front is the *generation*: how many times the document has been written. To change a document you must send
the revision you are replacing; if it is not the current one, CouchDB rejects the write. That is how it
notices two clients editing the same document.

**Conflicts.** When a write is *replicated in* from another CouchDB node rather than sent by a client,
CouchDB cannot reject it - the write already happened somewhere else. It keeps both branches of the
document's history and marks the document **conflicted**. One branch is picked deterministically as the
*winner* and served by default; the other is still there, waiting for the application to resolve it. A
conflict is a state to handle, not an error that was hidden from you. Asking for the change feed with
`style=all_docs` makes conflicts visible, because CouchDB then lists every leaf revision of the document
rather than only the winner.

**The `_changes` feed.** `GET /<database>/_changes?feed=continuous` returns an HTTP response that does not
end: CouchDB writes one JSON object per change, each on its own line, and keeps writing as new changes
happen. Every entry carries a `seq` - an opaque bookmark - and handing that bookmark back as `since` on the
next connection resumes exactly where you stopped.

**Deletions.** Deleting a document does not erase it. CouchDB keeps a stub carrying the identifier, a new
revision, and `_deleted: true`. That stub is what appears in the feed, and it maps one-to-one onto Kafka's
tombstone: a record with a real key and a `null` value.

### The files

Every rule worth testing lives in a pure function, and everything that talks to a server is a thin shell
around it.

| File | What it does |
| --- | --- |
| `src/de/couchdb/cdc/Catalogue.scala` | The vocabulary: `DocId`, `Revision`, `SequenceId`, `Availability`, `CatalogueProduct`, and the starting catalogue. `Sku` and `Money` come from the shared `de.common.domain` model. |
| `src/de/couchdb/cdc/CatalogueDocument.scala` | Turns a product into the JSON document CouchDB stores, and back. The `_id` is the SKU, which makes writing the same product twice an update rather than a second copy. |
| `src/de/couchdb/cdc/ChangesFeed.scala` | Parses one line of the continuous feed into a heartbeat, a change, or the end-of-feed marker. Pure, and tested against a recorded payload. |
| `src/de/couchdb/cdc/ChangeMapper.scala` | The business rule: a change becomes a record with the document identifier as key, a deletion becomes a tombstone, and CouchDB's own documents (`_design/…`) and documents of other kinds are ignored. |
| `src/de/couchdb/cdc/Checkpoint.scala` | The bookmark: how it advances, how often it is written, and how it is stored. |
| `src/de/couchdb/cdc/ChangeProcessor.scala` | The decision loop. It takes a `ChangeSink`, a `CheckpointStore` and a `ConnectorLog` as traits, so the whole loop - including the order in which a record is published and the checkpoint moved - is unit-tested with no CouchDB and no Kafka. |
| `src/de/couchdb/cdc/CouchDbClient.scala` | The HTTP calls: create the database, read and write documents, run a Mango query, read a view, and open the continuous feed. |
| `src/de/couchdb/cdc/CouchDbCheckpointStore.scala` | Stores the bookmark in CouchDB itself. |
| `src/de/couchdb/cdc/KafkaChangeSink.scala` | Creates the compacted topic and publishes records with an idempotent producer. |
| `src/de/couchdb/cdc/CatalogueQueries.scala` | The design document with its view, and the Mango selector - the two ways of reading the catalogue back. |
| `src/de/couchdb/cdc/ConnectorService.scala` | The long-running loop: follow one feed connection, then the next, until interrupted. |
| `src/de/couchdb/cdc/Main.scala` | Command line parsing and wiring. |

### The decisions worth explaining

**Where the checkpoint lives.** In CouchDB, in a document whose identifier starts with `_local/`. A local
document is invisible to the `_changes` feed and is never replicated - which is exactly right for a
bookmark, and is how CouchDB's own replicator records its progress. It also means the connector keeps no
state of its own: delete its container and it resumes where it left off.

**Publish first, checkpoint afterwards.** The record reaches Kafka before the bookmark moves, and the
bookmark is written only every few changes. A crash in between therefore replays a handful of changes. That
is **at-least-once** delivery, and it is safe here because the key is the document identifier: a replayed
change produces the same record for the same key, so a consumer of the compacted topic cannot tell that it
happened. The opposite order - checkpoint first - would silently lose changes instead.

**Why the "endless" feed still ends.** The connector asks CouchDB to close an idle feed after 30 seconds
(`timeout=30000`) and to send a blank heartbeat line every 10 seconds (`heartbeat=10000`), then reconnects
from the bookmark. The heartbeat keeps proxies from dropping a connection they think is dead. The timeout
exists because a blocking read on a classic Java stream does *not* end when Ox interrupts the thread: a feed
that never closed would leave shutdown hanging for ever. Bounding each connection gives the loop a regular,
predictable moment to notice that it has been asked to stop.

**Two ways to read the catalogue back.** The `report` command runs both. A **Mango query** is a declarative
JSON selector, close in spirit to a SQL `WHERE` clause, written the moment a question comes up. A **design
document** holds a **view**: a JavaScript map function that CouchDB runs over every document once and then
keeps up to date, producing a persistent sorted index - faster to read, but you have to decide up front what
to index.

## Run it

All commands are run from the repository root. Host ports are all in the 11500-11599 range reserved for
example 15.

**1. Start CouchDB, Kafka and the Kafka web interface.**

```bash
docker compose -f examples/15-couchdb-changes-to-kafka/docker/docker-compose.yml up -d
```

Compose waits for CouchDB to be healthy, runs a one-shot init container that creates the system databases
and the `catalogue` database and seeds it with five products, and waits for Kafka to be healthy. Two web
interfaces are then available:

- CouchDB (Fauxton): <http://localhost:11598/_utils> - user `admin`, password `couchdb`
- Kafka: <http://localhost:11580>

**2. Install the design document and write the catalogue from the Scala model.**

```bash
./mill examples.15-couchdb-changes-to-kafka.run seed
```

```
stored SKU-COFFEE as revision 2-9395c2ac6c167b6dae4cb6f5572c8061
stored SKU-GRINDER as revision 2-95a932e69ccd35223a38cc35735de279
...
```

The generation number is `2` rather than `1` because the init container already wrote generation 1.

**3. Start the connector.** It runs until you press Ctrl+C.

```bash
./mill examples.15-couchdb-changes-to-kafka.run follow
```

```
following catalogue from sequence 0
published SKU-COFFEE
ignoring _design/catalogue: CouchDB internal document
published SKU-GRINDER
published SKU-KETTLE
published SKU-FILTER
published SKU-MUG
```

**4. Change the catalogue, in a second terminal.** Mill runs one task at a time, so leave the connector in
its own terminal and use the CouchDB web interface, `curl`, or - after stopping the connector - these
commands:

```bash
./mill examples.15-couchdb-changes-to-kafka.run out-of-stock SKU-KETTLE
./mill examples.15-couchdb-changes-to-kafka.run remove SKU-FILTER
```

The connector prints `published SKU-KETTLE` and `published tombstone for SKU-FILTER`.

**5. Look at the topic.**

```bash
docker exec de-15-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic catalogue.products \
  --from-beginning --timeout-ms 8000 --property print.key=true
```

Each record is keyed by SKU; the deleted product appears with the value `null`. The same records are
browsable at <http://localhost:11580>.

**6. Read the catalogue back through both query styles.**

```bash
./mill examples.15-couchdb-changes-to-kafka.run report hardware
```

```
Mango query: 1 in-stock product(s) in category 'hardware'
  SKU-GRINDER Flat burr grinder 189.00 EUR
view by_category: products per category
  accessories: 1
  beans: 1
  consumables: 1
  hardware: 2
```

**7. Run the tests.** They cover the feed parsing, the checkpoint arithmetic and the change-to-record
mapping against a recorded feed payload, so no container has to be running:

```bash
./mill examples.15-couchdb-changes-to-kafka.test
```

## What to try next

- **See at-least-once delivery happen.** Stop the connector with Ctrl+C a moment after a change is
  published but before the checkpoint is written (it is written every five changes), then start it again.
  The last few changes are republished, and because the key is the SKU the compacted topic is unchanged.
- **Watch a checkpoint.** Open
  <http://localhost:11598/catalogue/_local/catalogue-connector-checkpoint> in a browser and refresh it while
  the connector runs. Delete the document and restart the connector: it replays the whole history from
  sequence 0.
- **Make a conflict.** In Fauxton, open a product, note its `_rev`, then use `curl` to write the document
  twice starting from that same revision. The second write is rejected with `409 Conflict`. To see a real
  conflict - two branches kept side by side - replicate the database into a second one, change the document
  differently on both sides, and replicate back; the connector then logs `… has 2 leaf revisions` and sets
  `"conflicted": true` on the published record.
- **Change the checkpoint interval.** `Settings.defaults.checkpointEveryNChanges` is 5. Set it to 1 and the
  bookmark never falls behind, at the cost of one extra CouchDB write per change.
- **Watch compaction remove a tombstone.** The broker in this stack is configured to compact aggressively.
  Delete a product, then consume the topic from the beginning a few minutes later and see the key disappear.

## Clean up

```bash
docker compose -f examples/15-couchdb-changes-to-kafka/docker/docker-compose.yml down -v
```

`-v` also removes the CouchDB and Kafka volumes, so the next `up` starts from an empty catalogue.
