# 09 - Trino: one SQL query over a lakehouse and an operational database

Trino is a distributed SQL query engine. It stores no data of its own. Instead it *connects* to
systems that do - object storage, relational databases, message queues - and lets you query all of
them with the same SQL, in the same statement. This example runs a single `SELECT` that joins a
table made of Parquet files in object storage with a table that lives inside PostgreSQL, without
copying either side anywhere. That ability, called **query federation**, is the reason Trino exists.

The Scala 3 program in this folder is a thin client: it connects over the Trino JDBC driver (Java
Database Connectivity, the standard Java API for talking to a database), creates and fills the
lakehouse table, then runs the `.sql` files in `resources/sql/` and prints each result as a
formatted text table.

## The use case

The online shop of this repository keeps its data in two places, as most companies do:

- **Order facts** pile up in the lakehouse. They are append-only, they are large, and they are
  stored as [Delta Lake](https://delta.io) tables: folders of Parquet files plus a transaction log
  that records which files make up the current version of the table. The files sit in an
  S3-compatible object store (here [MinIO](https://min.io), which speaks the Amazon Simple Storage
  Service protocol locally).
- **Customer master data** lives in the operational PostgreSQL database that the shop application
  writes to. It is small, it changes constantly, and nobody wants a nightly copy of it.

The question the business asks is "how much revenue came from each customer tier, per country?".
Answering it needs both sides. The traditional answer is a pipeline that copies PostgreSQL into the
lakehouse every night, with a day of lag and a job to maintain. Trino's answer is to read both
sources at query time:

```sql
SELECT c.tier, o.country, count(*), sum(o.total_cents) / 100.0
FROM delta.shop.orders AS o                  -- Parquet files in MinIO
JOIN postgresql.public.customers AS c        -- rows inside PostgreSQL
  ON c.customer_id = o.customer_id
GROUP BY c.tier, o.country;
```

## How it works

### The Trino naming model

Every table in Trino is addressed as **`catalog.schema.table`**.

- A **catalog** is one configured connection to a data source. This stack has two of them, plus the
  built-in `system` catalog: `delta` (the Delta Lake connector pointed at MinIO and the Hive
  Metastore) and `postgresql` (the PostgreSQL connector). Each is one properties file in
  `docker/trino/catalog/`; adding a data source means adding a file.
- A **schema** is a namespace inside a catalog. For the Delta connector it maps to a folder in the
  object store; for PostgreSQL it is a PostgreSQL schema, `public` here.
- A **table** is what you would expect.

Because the catalog is part of the name, a join across two systems needs no special syntax at all -
the query above is ordinary SQL.

### The services

`docker/docker-compose.yml` starts four long-lived services and three one-shot init containers:

| Service | What it is | Why it is here |
| --- | --- | --- |
| `minio` | S3-compatible object storage | Holds the Parquet files and the Delta transaction log |
| `minio-init` | MinIO command line client | Creates the `lakehouse` bucket, then exits |
| `hive-metastore` | Apache Hive Metastore | The catalogue that maps a table name to a folder |
| `hive-metastore-init` | one `chown` | Hands the metastore's data volume to its unprivileged user |
| `postgres` | PostgreSQL 16 | The operational database with the customer rows |
| `postgres-init` | `psql` | Loads `docker/postgres/01-customers.sql`, then exits |
| `trino` | Trino 476 coordinator | The engine; the only service the Scala program talks to |

The **Hive Metastore** deserves a sentence, because its name is misleading: it has nothing to do
with running Hive queries. It is a small service holding one thing - a list of which tables exist
and in which folder their files live. Trino asks it "where is `delta.shop.orders`?", gets a path
back, and reads the files itself. Its own bookkeeping goes into an embedded Apache Derby database on
a Docker volume; a production deployment would point it at a shared PostgreSQL or MySQL server
instead, which matters only when several metastore instances have to share one catalogue.

The metastore does touch the object store in one place: creating a schema creates that schema's
folder. It does that through Hadoop's S3A filesystem, which is why `docker/hive/core-site.xml`
carries the MinIO endpoint and credentials and why the container puts Hadoop's S3A jars on the
classpath with `HIVE_AUX_JARS_PATH`. Trino itself does not use those jars - it has its own native S3
client, configured in `docker/trino/catalog/delta.properties`.

### The Scala side

The code is split so that everything except the connection itself is a pure function, which is what
lets the tests run with no cluster at all:

| File | Responsibility |
| --- | --- |
| `src/de/trino/lakehouse/TrinoSession.scala` | The only place that knows about JDBC. `TrinoSession.connected` opens a connection, hands it to a block and closes it again even if that block throws, so no caller can leak one. `TrinoSession` itself is a one-method interface, which is what the tests replace with a fake. |
| `src/de/trino/lakehouse/ResultTable.scala` | The query result as plain text plus `ResultCursor`, a three-method view of a result set. `ResultTable.from` walks a cursor; it never sees a `java.sql.ResultSet`. |
| `src/de/trino/lakehouse/TableRenderer.scala` | Draws a `ResultTable` as a fixed-width text table. Pure string handling. |
| `src/de/trino/lakehouse/SqlScript.scala` | Splits the text of a `.sql` file into statements: a JDBC driver runs one statement per call, and a semicolon inside a string literal must not end one. Keeps the comment lines above a statement as its description. |
| `src/de/trino/lakehouse/SqlLibrary.scala` | Finds the shipped `.sql` files on the classpath and lists them in execution order. |
| `src/de/trino/lakehouse/ScriptRunner.scala` | Runs a parsed script against a session and formats the whole run. Query plans are printed verbatim instead of squeezed into a table. |
| `src/de/trino/lakehouse/DeltaSeed.scala` | Builds the `CREATE SCHEMA` / `CREATE TABLE` / `INSERT` statements from the shared `de.common.gen.DataGenerator` orders. Pure: orders in, SQL out. |
| `src/de/trino/lakehouse/Main.scala` | The wiring. Waits for the coordinator, seeds, then runs the scripts. |

### Seeding the lakehouse table

Trino can *write* Delta Lake tables, not only read them, so this example needs no Apache Spark job
and no files prepared in advance. `DeltaSeed` produces ordinary SQL, `Main` sends it over the same
connection as the queries, and the result is a genuine Delta table in MinIO - transaction log
included - that any other engine can read.

That makes this example **standalone**: it does not depend on the output of example 07. If you have
run example 07 and want to query the tables it wrote instead, point the `delta` catalog at that
example's bucket and skip the `seed` step; the queries themselves need only a table named
`delta.shop.orders`.

The orders come from the shared generator with its default seed, so the table is identical on every
run, and the script starts with `DROP TABLE IF EXISTS`, so re-seeding is safe.

### The SQL files

They run in this order and are meant to be read top to bottom:

1. `01-explore-catalogs.sql` - `SHOW CATALOGS`, `SHOW TABLES`, `DESCRIBE`: the catalog model itself.
2. `02-cross-catalog-join.sql` - the federated join, and a variant whose filter is pushed down into
   PostgreSQL.
3. `03-explain-the-join.sql` - `EXPLAIN` and `EXPLAIN (TYPE IO)`: the plan Trino would run, with one
   table scan per source feeding a single join operator.
4. `04-session-properties.sql` - `SET SESSION join_distribution_type = 'PARTITIONED'` and the same
   `EXPLAIN` again, so you can see a session property change the plan; then a look at
   `system.runtime.queries`, the table behind the web interface's query statistics.

## Run it

From the repository root:

```bash
# 1. Start the stack and wait until every service reports healthy (first run pulls ~2 GB).
docker compose -f examples/09-trino-lakehouse-sql/docker/docker-compose.yml up -d --wait

# 2. Seed the Delta table and run every SQL file.
./mill examples.09-trino-lakehouse-sql.run
```

`run` takes an optional mode: `seed` only writes the table, `query` only runs the SQL files, and no
argument at all does both.

Expect output along these lines - 500 orders loaded, then one block per statement showing the
comment, the SQL and the result:

```
Connecting to jdbc:trino://localhost:10980/delta/shop as lakehouse
Seeding delta.shop.orders with 500 orders (13 statements)
Seeded delta.shop.orders

### 02-cross-catalog-join.sql
====================================================================================================
-- The query that explains why Trino exists: one statement, two storage systems.
SELECT ...
tier   | country | order_count | revenue_eur
-------+---------+-------------+------------
silver | UA      | 37          | 11253.6
bronze | FR      | 45          | 10210.3
...
10 rows
```

Ports published by this example, all inside its assigned 10900-10999 range:

| Address | What |
| --- | --- |
| <http://localhost:10980> | Trino web interface - every query, its state, its input rows and bytes |
| <http://localhost:10901> | MinIO console, login `minioadmin` / `minioadmin` - browse the Parquet files and `_delta_log` |
| `localhost:10900` | MinIO S3 endpoint |
| `localhost:10932` | PostgreSQL, database `shop`, user `shop`, password `shop` |

Open the Trino web interface after a run: each query has a detail page with the running time, the
number of rows read per source and the same plan `EXPLAIN` printed.

You can also use the command line client that ships inside the container:

```bash
docker exec -it de-09-trino trino
```

## What to try next

- **Watch the pushdown.** Run `EXPLAIN (TYPE IO, FORMAT TEXT) SELECT count(*) FROM
  postgresql.public.customers WHERE tier = 'gold';` and then the same without the `WHERE`. The plan
  shows which predicate Trino handed to PostgreSQL rather than evaluating itself.
- **Compare the join strategies.** Run the last `EXPLAIN` of `04-session-properties.sql` with
  `BROADCAST`, with `PARTITIONED` and with the `AUTOMATIC` default, and compare the plans and the
  timings on the web interface.
- **Add a third catalog.** Copy `docker/trino/catalog/postgresql.properties`, point it at another
  database, restart the `trino` container, and join across three systems.
- **Look at the table format.** In the MinIO console, open `lakehouse/shop/orders/_delta_log/`. Each
  `INSERT` appended one JSON file listing the Parquet files it added: that log *is* the table.
- **Query an old version.** Delta keeps history, so `SELECT count(*) FROM
  delta.shop."orders$history"` shows every write, and `FOR VERSION AS OF 1` reads the table as it was
  after the first insert.

## Clean up

```bash
docker compose -f examples/09-trino-lakehouse-sql/docker/docker-compose.yml down -v
```

`-v` also deletes the volumes, so the bucket, the metastore catalogue and the customer rows are gone
and the next `up` starts from nothing.
