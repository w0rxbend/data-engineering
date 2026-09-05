# 12 - Handing data from Scala to Polars over Apache Arrow

This example moves half a million order lines from a Scala 3 program to **Polars** - a DataFrame library written in
Rust - and back again, without ever turning the data into text. A DataFrame is a table with named, typed columns, the
same idea as a spreadsheet or a SQL table held in memory. The Scala side writes **Apache Arrow** buffers to disk; the
Polars side opens the same typed column layout without formatting and reparsing every value as text.

“Zero-copy” is a layout compatibility claim, not a claim of free input/output: IPC still frames batches and writes
bytes to disk, and a particular reader may map or copy those buffers according to its implementation and operating
system. The guarantee demonstrated here is that the boundary preserves the typed column representation and avoids a
row-by-row CSV-style conversion.

Along the way the example runs the *same* aggregation three ways - in ordinary object-oriented Scala, in Scala directly
over the Arrow column buffers, and in Polars - prints all three timings, and checks that all three produce identical
numbers to the cent.

> [Example 11](../11-parquet-arrow-toolkit/README.md) in this repository is the Arrow primer: what a columnar layout
> is, why it is fast, and how the format is laid out on disk. This example assumes you have read it and concentrates
> on the *boundary crossing*.

## The use case

The online shop from the shared domain model (`common/src/de/common/domain/Model.scala`) has a Java Virtual Machine
(JVM) service that owns ingestion. It accepts orders, validates them, and writes the order archive. That service is
staying on the JVM: it has the connection pools, the schema registry, and the ten years of business rules.

The analytics team does not want to work there. They want Polars: lazy query plans, a native engine, and a Python
notebook. The traditional answer is to export comma-separated values (CSV) nightly, which means the JVM spends minutes
formatting numbers as text and Polars spends minutes parsing that text back into numbers - work that produces no
information at all.

The answer here is Arrow. The JVM service writes:

- `order_lines.arrow` - the fact table, one row per order line, in Arrow's Inter-Process Communication (IPC) format.
  IPC is the name Arrow gives to its on-disk framing of columnar batches.
- `regions.arrow` - a five-row dimension table mapping country to sales region, so Polars has something real to join.
- `order_lines_parquet/` - the same fact table at rest as Apache Parquet: compressed, column-encoded, with per-chunk
  statistics.

Polars reads exactly those files, aggregates, and writes `polars_revenue.arrow` back. The Scala program reads that
result and prints it next to its own.

## How it works

### The Scala side (`src/de/polars/bridge/`)

| File | What it does |
| --- | --- |
| `OrderLineRow.scala` | Flattens the nested domain `Order` (which owns a list of `OrderLine`s) into one flat row per line, repeating the order-level attributes. Also holds `RegionRow`, the tiny country-to-region dimension table. |
| `ArrowSchemas.scala` | The Arrow schemas: field names, physical types, nullability. The schema travels *inside* the file, so Polars never needs a copy of this code. |
| `ArrowIpc.scala` | Writes and reads Arrow IPC files batch by batch, publishes them through sibling temporary files, and verifies input fingerprints. `OrderLineVectors` resolves the eight columns once per batch instead of once per row. |
| `ParquetExport.scala` | Converts an Arrow IPC file into a Parquet dataset. |
| `RevenueAggregation.scala` | The same aggregation twice: `fromRows` over case classes, `fromArrowFile` straight over the column buffers. |
| `RevenueReport.scala` | Renders results, agreement checks and timings as plain text. |
| `Stopwatch.scala` | A small warm-up-then-best-of-N timer. |
| `DataSet.scala` | The file names both sides agree on, in one place. |
| `Main.scala` | The only file that does input/output: generate, write, aggregate, print. |

A few decisions in there are worth explaining.

**Money stays in cents.** `Money` in the shared domain keeps a `Long` of minor units. Summing cents is exact; summing
floating point numbers is not, and a cross-language comparison that disagreed in the last decimal place would teach the
wrong lesson.

**Timestamps are zone-less milliseconds.** Arrow distinguishes "an instant in a named time zone" from "a naive
wall-clock reading". The shared domain stores plain epoch milliseconds with no zone, so the honest mapping is Arrow's
zone-less `Timestamp(MILLISECOND, null)`. Polars shows it as `datetime[ms]`.

**The writer casts, the reader does not.** `OrderLineVectors` casts each column to its concrete Arrow class
(`VarCharVector`, `BigIntVector`, …) because that is the hot path over half a million rows. `ArrowIpc.readRevenue`,
which reads the five-row answer Polars sends back, deliberately goes through the generic `getObject` interface instead.
It has to: Polars encodes text as Arrow's *large string* type with 64-bit offsets, which is a different Java class from
the 32-bit `VarCharVector` this module writes. Both satisfy the same logical type, and a well-behaved reader accepts
either. `test/resources/polars_revenue.arrow` is a real file produced by the container, checked in so the test suite can
prove that compatibility without Docker.

**Parquet is written by the native Arrow library, not by Hadoop.** The `arrow-dataset` artifact bundles the Arrow C++
code as a native library and can write Parquet straight from Arrow batches. The classic JVM alternative, `parquet-mr`,
drags in a large part of Apache Hadoop to write one local file. A rerun builds the dataset in a sibling staging
directory and replaces the old dataset only after the native writer succeeds. A failed conversion therefore leaves the
last complete dataset readable. Java cannot portably replace a non-empty directory atomically, so interruption during
the final replacement requires rerunning this deterministic export.

One build wrinkle: Arrow keeps its buffers *outside* the Java heap and reaches into `java.nio` internals to do it. Java
17 and later close that package off by default, so `package.mill` adds
`--add-opens=java.base/java.nio=ALL-UNNAMED` to `forkArgs` for both `run` and `test`. Without it, every Arrow call
fails with `InaccessibleObjectException`.

**A result is tied to its inputs.** The Polars container records SHA-256 digests of both inputs in
`polars_input.sha256` and publishes that manifest last, as the completion marker. Scala checks it before opening
`polars_revenue.arrow`. It therefore rejects a result left over after the inputs were regenerated with different data
instead of presenting a plausible but stale comparison. Scala also writes every Arrow input to a sibling temporary file
and moves it into place only after the writer and its off-heap vectors have closed. This protects the previous file from
ordinary writer failures; Java does not promise atomic replacement on every filesystem.

### The Polars side (`docker/aggregate.py`)

Polars has no official JVM API. The intentionally small Python adapter is therefore the native-engine boundary: Scala
owns data generation, Arrow schemas, Parquet export, independent aggregation, manifest verification, and the final
equality check; Python owns only the Polars query and its Arrow result.

`pl.scan_ipc(path)` does **not** read the file. It returns a `LazyFrame`: a description of a computation. Only
`.collect()` runs it, and before it runs Polars optimises the whole description. You can see the result yourself - the
script prints `plan.explain()` and saves it to `data/polars_query_plan.txt`:

```
SORT BY [col("country")]
  AGGREGATE[maintain_order: false]
    [col("order_id").n_unique() ..., col("quantity").sum() ..., col("line_total_cents").sum() ...] BY [col("country"), col("region")]
    FROM
    simple π 5/5 ["country", "region", ... 3 other columns]
      LEFT JOIN:
      LEFT PLAN ON: [col("country")]
        Ipc SCAN [/data/order_lines.arrow]
        PROJECT 4/8 COLUMNS
      RIGHT PLAN ON: [col("country")]
        Ipc SCAN [/data/regions.arrow]
        PROJECT */2 COLUMNS
      END LEFT JOIN
```

Read it bottom-up. The line that matters is `PROJECT 4/8 COLUMNS`: the query asks for four of the eight columns, so
Polars pushed that knowledge down into the scan and will never touch the other four. In a row-oriented format that
saving is impossible - the unwanted fields sit physically between the wanted ones.

The script also collects with `engine="streaming"`, which runs the plan in chunks rather than materialising the whole
table. That is Polars' answer to data larger than memory: the same query, the same code, bounded memory. On a 38 MiB
file it merely demonstrates that the option exists; on a 380 GiB one it is the difference between an answer and an
out-of-memory error. `RevenueAggregation.fromArrowFile` on the Scala side does the same thing by hand, one record batch
at a time.

Finally the script shows a **window expression**: each product's share of the revenue of its own region.

```python
(pl.col("revenue_cents") / pl.col("revenue_cents").sum().over("region") * 100).round(2)
```

`over("region")` evaluates the sum once per region and broadcasts it back onto every row of that region without
collapsing the rows - the same idea as SQL's `SUM(...) OVER (PARTITION BY region)`.

### Is crossing the boundary worth it?

Here is a run on one developer laptop, 499,990 order lines:

| Implementation | Time | Notes |
| --- | --- | --- |
| Scala over case classes | ~136 ms | The version most JVM codebases actually contain. |
| Scala over Arrow vectors | ~57 ms | Same JVM, same process, no objects allocated per row. |
| Polars, streaming engine | ~38 ms | Plus roughly 300 ms of process startup, which the table ignores. |

Read those numbers honestly:

- **Polars is faster, but not by the order of magnitude the marketing suggests** - not on this query, at this size, on
  this machine. Most of the win over naive Scala came from going columnar, and *that* win was available without leaving
  the JVM at all.
- **The boundary is not free.** Writing 38 MiB of Arrow takes longer than the aggregation does. Starting a Python
  process takes longer still. For a query that runs once per request, crossing the boundary is a straight loss.
- **It pays when the work on the far side is large or repeated.** One export, then a data scientist running fifty
  exploratory queries over the same files, is the shape that wins. So is a nightly job whose Polars half is minutes of
  work rather than milliseconds.
- **It also pays when the alternative is CSV.** That is the comparison this example is really making. Arrow versus
  Polars-on-the-JVM is a close call; Arrow versus text export is not close at all.
- **The tie-breaker is usually people, not milliseconds.** The analytics team gets to work in the tools they know
  against the exact rows the production service wrote, with no schema drift and no parsing bugs in between. That is
  worth more than 16 milliseconds.

## Run it

Everything is run from the **repository root**.

**1. Write the Arrow and Parquet files and aggregate on the JVM.**

```bash
./mill examples.12-polars-arrow-bridge.run examples/12-polars-arrow-bridge/data 20000
```

Expect roughly this, followed by a note that no Polars result exists yet:

```
generated 20000 orders -> 49852 order lines
wrote .../data/order_lines.arrow (3.7 MiB)
wrote .../data/regions.arrow (0.0 MiB)
wrote .../data/order_lines_parquet/order_lines_0.parquet (1.6 MiB)

revenue per country, computed on the JVM:
country  region  orders  units   revenue
-------  ------  ------  ------  -----------------
DE       DACH    3939    19610  979,350.40 EUR
...
case classes and arrow vectors agree on all 5 rows.
```

To use a different size, pass the directory and the order count:
`./mill examples.12-polars-arrow-bridge.run examples/12-polars-arrow-bridge/data 20000`.

**2. Run Polars over exactly those buffers.**

```bash
DOCKER_UID=$(id -u) DOCKER_GID=$(id -g) \
  docker compose -f examples/12-polars-arrow-bridge/docker/docker-compose.yml run --build --rm polars
```

The first run builds the image (a pinned `python:3.12-slim-bookworm` plus `polars==1.44.1`), which takes a minute. The
`DOCKER_UID`/`DOCKER_GID` prefix makes the container write its output as you rather than as root, so you can delete the
`data/` directory afterwards without `sudo`; on a machine where your account is the usual `1000:1000` you can leave it
off.

The container prints the query plan, the aggregate, the window expression, a row-count cross-check against the Parquet
copy, and its own timing - then exits.

**3. Read the Polars answer back from Scala.**

```bash
./mill examples.12-polars-arrow-bridge.run examples/12-polars-arrow-bridge/data 20000
```

Now the tail of the output compares the two sides:

```
revenue per country, computed by Polars and read back over Arrow:
...
arrow vectors and polars agree on all 5 rows.
polars reported 38.35 ms for the same aggregation
```

That comparison is an executable check, not only output: a stale/missing manifest or any differing country aggregate
makes the Scala process exit non-zero.

**Tests**, which never start a container:

```bash
./mill examples.12-polars-arrow-bridge.test
```

**Host ports:** none. This stack is a single one-shot batch container with no server in it and
`network_mode: none`, so it publishes nothing and cannot collide with any other example. The host port range reserved
for example 12 is 11200-11299 and remains unused.

## What to try next

- **Interrupt and recover.** Start the Polars step and press Ctrl-C while it is running. The last valid result stays in
  place, and the input manifest is published last as the completion marker. For unchanged inputs Scala can still read
  that previous result; for changed inputs it refuses the stale result because the digests no longer match. Run the same
  Compose command again to replace the result. Parquet conversion is also staged before replacement, but Java cannot
  portably replace a non-empty directory atomically; interruption during that final replacement requires rerunning the
  deterministic JVM export.

- **Change the batch size.** `ArrowIpc.DefaultBatchSize` is 4096 rows. Set it to 100 and watch the file grow and the
  read slow down: every batch carries its own metadata and breaks the vectorised inner loop. Set it to 500,000 and the
  writer holds the whole table in off-heap memory at once.
- **Add a column and only touch one side.** Add a field to `OrderLineRow` and `ArrowSchemas.orderLines`, then re-run.
  The Python script keeps working untouched, because it asks for columns by name and the schema arrives with the data.
- **Force Polars to read the Parquet copy instead.** Change `pl.scan_ipc(ORDER_LINES)` to
  `pl.scan_parquet(PARQUET_DIR / "*.parquet")` in `revenue_per_country`. It is slower here (decompression) but the file
  is 16 MiB instead of 38 MiB - the at-rest/in-memory trade-off, measurable in one edit.
- **Break the streaming claim.** Raise the order count to five million and compare peak memory between
  `plan.collect(engine="streaming")` and plain `plan.collect()` (use `/usr/bin/time -v` inside the container).
- **Make the case classes look good.** `RevenueAggregation.fromRows` uses `groupBy`, which materialises every group.
  Rewrite it as a single `foldLeft` over a mutable map and re-measure: how much of the JVM's disadvantage was Arrow, and
  how much was ordinary allocation cost?

## Clean up

```bash
docker compose -f examples/12-polars-arrow-bridge/docker/docker-compose.yml down -v
rm -rf examples/12-polars-arrow-bridge/data
```

The `data/` directory is generated output and is ignored by this example's `.gitignore`; deleting it costs nothing but
a re-run of step 1.
