# 03 - ksqlDB: an operations dashboard written in SQL

Apache Kafka stores endless streams of events, but a topic on its own answers no questions. ksqlDB is
a server that puts SQL on top of those topics: you declare a stream, write a `SELECT`, and ksqlDB
keeps the answer up to date forever, writing the result back into Kafka as it changes. This example
defines a small operations dashboard entirely in SQL, then drives it from Scala 3 over ksqlDB's HTTP
API - submitting statements, subscribing to a live result, and asking a one-shot question.

Nothing here needs a Kafka client library. The Scala side is an HTTP client and a JSON parser, which
is exactly what makes ksqlDB interesting: the stream processing lives in the server, and any language
that can speak HTTP can drive it.

## The use case

The online shop shared by every example in this repository emits two kinds of event: an `Order` when
a customer checks out, and a `Payment` when the card is charged. Operations wants two tiles on a
wall display:

1. **Revenue per country per minute** - how much money is coming in right now, split by market.
2. **Customers with declined payments** - who could not pay, so support can reach out.

Both tiles are a `SELECT`. The first is a windowed aggregation, the second a join followed by a
count. Neither requires a line of stream-processing code.

## How it works

### The SQL, in `sql/`

The four files are submitted in order and are the whole data pipeline.

- **`01-create-streams.sql`** declares `orders_raw` and `payments_raw`. A ksqlDB `STREAM` is a schema
  laid over an existing Kafka topic; creating one copies no data. `KEY` marks the column stored in
  the Kafka record key rather than in the JSON value, and `TIMESTAMP` names the column that carries
  *event time* - when the order was placed, not when ksqlDB read it. Event time is what makes the
  windowed tile meaningful.

- **`02-create-analytics.sql`** builds the dashboard in four steps. `orders_enriched` flattens each
  order and computes its total with `REDUCE`, which folds over the array of order lines the way
  `foldLeft` does in Scala. `revenue_per_country_per_minute` is a `TABLE` (one row per key, kept
  current) grouped by country over `WINDOW TUMBLING (SIZE 1 MINUTE)`, so its key is really the pair
  *(country, minute)*. `declined_payments` joins each declined payment back to the order it belongs
  to - a stream-to-stream join must be time-bounded, hence `WITHIN 1 HOURS` - and
  `declined_payments_per_customer` counts those per customer.

- **`03-push-query.sql`** is a **push query**: `EMIT CHANGES` turns the `SELECT` into a subscription
  that never finishes on its own. `LIMIT 5` is the only reason this one ends.

- **`04-pull-query.sql`** is a **pull query**: no `EMIT CHANGES`, so it reads the current state of
  the table once and closes - the shape a dashboard uses when someone refreshes the page.

### The Scala, in `src/de/ksqldb/`

The code is split so that everything worth testing can be tested without Docker.

- **`KsqlStatements.scala`** - pure text. It splits a `.sql` file into statements (dropping `--`
  comments), and renders a domain `Order` or `Payment` as an `INSERT INTO ... VALUES` statement,
  including SQL quote escaping. Seeding through ksqlDB itself is why this example needs no Kafka
  producer.

- **`KsqlProtocol.scala`** - pure parsing. It builds the JSON request bodies and interprets the two
  response shapes: the array `POST /ksql` returns, and the line-by-line chunked stream `POST /query`
  returns. The response of a query is one enormous JSON array streamed piece by piece, so each line
  still carries the array's punctuation (a leading `[` or `,`, a trailing `]`), which is stripped
  before parsing. The schema string is split on commas *outside* angle brackets, because a nested
  `ARRAY<STRUCT<...>>` type contains commas of its own.

- **`KsqlDbClient.scala`** - the only file that touches the network, about 150 lines. It uses
  sttp client4's synchronous backend in *direct style*: every call is an ordinary blocking method
  that returns a value, with no `Future` and no `IO` wrapper. Failures - an unreachable server, an
  HTTP error, a rejected statement - come back as a `KsqlFailure` value rather than an exception.
  `streamQuery` reads the response body as an `InputStream` and hands each parsed line to a callback,
  so a push query prints its first row immediately instead of after the connection closes.

  One non-obvious detail: the client sends `Accept: application/vnd.ksql.v1+json`. Given a free
  choice, a modern ksqlDB server answers `/query` in a newer, different format; naming version 1
  pins the response shape.

- **`Main.scala`** - the wiring, and nothing else. It applies the two DDL scripts (treating "already
  exists" as success, so re-running is safe), seeds 120 orders with their payments from the shared
  `DataGenerator`, runs the push query, then runs the pull query.

The events come from `de.common.gen.DataGenerator`, seeded so that two runs produce identical data.
Roughly one payment in ten is declined, which is what fills the second tile.

### The Docker stack, in `docker/`

Three containers: a single Apache Kafka broker in KRaft mode (no ZooKeeper - the broker holds its own
metadata), the ksqlDB server, and the ksqlDB command line client, kept idle so you can attach to it.
Health checks and `depends_on: condition: service_healthy` mean `up` returns only once ksqlDB is
actually answering.

## Run it

Start the stack from the repository root:

```bash
docker compose -f examples/03-ksqldb-orders/docker/docker-compose.yml up -d --wait
```

Then run the example:

```bash
./mill examples.03-ksqldb-orders.run
```

Expect output along these lines (the exact customer identifiers are fixed by the generator seed):

```
Talking to ksqlDB at http://localhost:10388

== declaring the raw streams (01-create-streams.sql)
   CREATE STREAM orders_raw ( id VARCHAR KEY, customerId VARCHAR, line... -> SUCCESS
   CREATE STREAM payments_raw ( orderId VARCHAR KEY, amount STRUCT<cen... -> SUCCESS

== declaring the dashboard (02-create-analytics.sql)
   CREATE STREAM orders_enriched AS SELECT id, customerId, country, RE... -> SUCCESS
   ...

== inserting 120 orders and their payments
   240 rows inserted

== push query: customers with declined payments, as the totals change (03-push-query.sql)
   CUSTOMERID | DECLINEDCOUNT | DECLINEDCENTS
   cust-0248 | 1 | 17088
   ...
   (server closed the query: Limit Reached)

== pull query: revenue per minute for Germany (04-pull-query.sql)
   COUNTRY | WINDOWSTART | WINDOWEND | REVENUECENTS | ORDERCOUNT
   DE | 1699999980000 | 1700000040000 | 504469 | 20
   DE | 1700000040000 | 1700000100000 | 151964 | 7

Done.
```

Running it a second time is safe - the streams and tables are already there and are kept - but it
inserts the same 120 orders again, so the totals double. Start from `docker compose down -v` when you
want the original numbers back.

The unit tests cover the statement building and the response parsing, and need no containers:

```bash
./mill examples.03-ksqldb-orders.test
```

### Ports and addresses

| What | Address |
| --- | --- |
| ksqlDB REST API | `http://localhost:10388` (`/info`, `/healthcheck` are worth opening in a browser) |
| Kafka broker, from your machine | `localhost:10392` |
| Kafka broker, from inside the stack | `kafka:29092` |

ksqlDB has no web interface of its own; the interactive shell is the CLI container:

```bash
docker compose -f examples/03-ksqldb-orders/docker/docker-compose.yml exec ksqldb-cli \
  ksql http://ksqldb-server:8088
```

Inside it, try `SHOW STREAMS;`, `SHOW QUERIES;` or
`PRINT 'orders' FROM BEGINNING LIMIT 3;`.

To point the Scala client somewhere else, pass a URL as the first argument or set `KSQLDB_URL`.

## What to try next

- **Change the window.** Edit `WINDOW TUMBLING (SIZE 1 MINUTE)` in `02-create-analytics.sql` to
  `SIZE 10 SECONDS`, then in the CLI run `DROP TABLE revenue_per_country_per_minute DELETE TOPIC;`
  and re-run the example. Ten times as many buckets appear, each with fewer orders.

- **Watch a push query react.** In one terminal, open the CLI and run
  `SELECT country, revenueCents FROM revenue_per_country_per_minute EMIT CHANGES;` (no `LIMIT`). In
  another, re-run `./mill examples.03-ksqldb-orders.run`. Rows appear in the first terminal as the
  second one inserts.

- **Feed it from outside.** ksqlDB matches JSON field names case-insensitively, so an ordinary Kafka
  producer using the shared camelCase layout works too:

  ```bash
  docker compose -f examples/03-ksqldb-orders/docker/docker-compose.yml exec -T kafka \
    kafka-console-producer --bootstrap-server kafka:29092 --topic orders \
    --property parse.key=true --property key.separator=:
  ```

  Then paste one line and press Ctrl-D:

  ```
  order-ext-1:{"customerId":"cust-9999","lines":[{"sku":"SKU-MUG","quantity":3,"unitPrice":{"cents":1000,"currency":"EUR"}}],"placedAt":1700000200000,"country":"IT"}
  ```

  A pull query for `country = 'IT'` now returns revenue for a market the generator never produced.

- **Break the pull query.** Ask for a country with no orders, or drop `WHERE` entirely, and see how
  ksqlDB responds to a query it cannot answer from a single key lookup.

## Clean up

```bash
docker compose -f examples/03-ksqldb-orders/docker/docker-compose.yml down -v
```

`-v` removes the Kafka volume as well, so the next `up` starts from an empty broker.
