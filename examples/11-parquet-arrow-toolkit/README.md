# Example 11 - Apache Parquet on disk, Apache Arrow in memory

Every other example in this repository quietly relies on two file and memory formats and never shows them
to you. Apache Spark writes Parquet in example 07, Trino reads it in example 09, PrestoDB scans it in
example 10 - and in all three cases the format is hidden behind an engine that decides for you what gets
read. This example removes the engine.

Here, a plain Scala 3 program writes Apache Parquet files itself, opens their footer metadata and prints
what is inside, measures what each way of reading costs, compares compression codecs, and then moves the
same rows through Apache Arrow, including Arrow's inter-process file format. There is no query planner, no
cluster, and no SQL. The goal is that after reading this, "columnar storage" and "zero-copy interchange"
stop being slogans and become things you have watched happen.

Two terms, spelled out once:

- **Apache Parquet** is a *file* format. It stores a table column by column, compresses it, and records an
  index of what is where. It optimises for taking up little space and for letting a reader skip most of
  the file.
- **Apache Arrow** is a *memory* format. It stores a table column by column in flat, uncompressed buffers
  at predictable offsets. It optimises for being usable immediately, with no decoding, and for being handed
  between processes and programming languages without conversion.

They are complementary, not competing: Parquet is how a table rests, Arrow is how it travels and how it is
worked on.

## The use case

An **order archive**. The shop from `de.common.domain` has been placing orders for years. Finance wants the
history kept cheaply, and analysts occasionally want to ask it a question - "what did each product earn?" -
without waking up a data warehouse.

So the archive is a directory of Parquet files, and the toolkit in this example is the thing that writes it
and queries it. One order becomes one row per order line, so the archive is a flat table of eight columns:

| column | meaning |
| --- | --- |
| `order_id` | which order the line belongs to |
| `customer_id` | who placed it |
| `country` | which storefront it came from |
| `placed_at` | when, in milliseconds since 1970-01-01 UTC |
| `sku` | the stock keeping unit, the shop's product identifier |
| `quantity` | how many were bought |
| `unit_price_cents` | price of one, in cents |
| `line_total_cents` | `unit_price_cents` times `quantity` |

Repeating `order_id` and `country` on every line looks wasteful and is not, for a reason the run makes
visible: `country` holds five distinct values across thirty thousand rows and ends up occupying about two
percent of the file.

## How it works

The code separates the pure reasoning from the input and output, so that almost all of it is unit-testable
without containers or even files.

### The domain

**`OrderArchive.scala`** turns shared-domain `Order` values into flat `ArchiveRow` values and **sorts them by
timestamp**. The sort is not cosmetic. Parquet records the smallest and largest value of every column in
every row group; if rows arrived in random time order, every row group would span the whole time range and
none could ever be skipped. Sorting on the column you filter by is the cheapest physical-design decision in
a columnar archive - it is what "clustering" or "Z-ordering" does in the table formats of examples 07 and 08.

**`ArchiveSchema.scala`** holds the Avro record shape. The `parquet-avro` library accepts an Apache Avro
schema and translates it into Parquet's own schema, which is the only reason Avro appears at all; no Avro
file is ever written. It also builds *projection* schemas - a subset of the fields - which is how a
projection is pushed into the reader.

### Writing

**`ParquetArchiveWriter.scala`** writes rows to a local file. Its `WriteOptions` expose the three knobs this
example varies: the compression codec, the row-group size, and whether dictionary encoding is allowed.

The row-group size is set to 256 KiB, far below Parquet's 128 MiB default. That is a demonstration choice:
it makes the sample archive span nine row groups instead of one, so row-group skipping becomes observable at
small scale. On a real archive, tiny row groups are a mistake - they multiply footer metadata and shorten
the runs of repeated values that compression feeds on.

The writer also switches Hadoop to `RawLocalFileSystem`, which is the local filesystem without the hidden
`.crc` checksum sidecar files Hadoop otherwise leaves next to everything it writes.

### The file layout, and reading the footer

A Parquet file is a stack of nested containers:

```
file
 ├─ row group 0            a horizontal slice of the table
 │   ├─ column chunk       all values of one column, for this slice
 │   │   ├─ page           the smallest compressed, encoded unit
 │   │   └─ page
 │   └─ column chunk ...
 ├─ row group 1 ...
 └─ footer                 schema, and for every column chunk: location,
                           size, encodings, min, max, null count
    PAR1                   four magic bytes marking the end
```

The footer is at the **end**, which surprises people. It has to be: a writer streaming rows does not know
how large a row group will be until it has written it, so the index can only be written last. A reader
therefore seeks to the last eight bytes, learns the footer's length, seeks back, and reads it - touching a
few kilobytes regardless of how big the file is.

**`FileLayout.scala`** is the plain-data description of all of that (`ParquetLayout`, `RowGroupLayout`,
`ColumnChunkLayout`) plus `ScanPlanner`, the pure arithmetic that says what each read strategy must cost.
Nothing in this file opens a file, which is why `ScanPlannerSuite` can build a layout by hand and check the
numbers on paper.

**`FooterReader.scala`** is the thin adapter that produces a `ParquetLayout` from a real file.

### The three ways of reading

**`ArchiveReader.scala`** implements them, and `Main` prints both the *predicted* cost (from the footer alone)
and the *measured* cost (from Hadoop's own byte counter):

1. **Full scan** - every column of every row group.
2. **Projection pushdown** - only `sku` and `line_total_cents`. "Pushdown" means the restriction is handed
   down to the storage layer instead of applied afterwards: the reader never opens the byte ranges of the
   other six columns. Those two columns are about a quarter of the file, so this is roughly a four-fold
   saving.
3. **Predicate pushdown** - only rows whose `placed_at` falls in a narrow window. Two different things happen
   here and it is worth telling them apart. Skipping a whole row group is decided from the footer statistics
   before any data is read, and is where the saved bytes come from. Discarding individual non-matching rows
   inside a surviving row group happens after decoding and saves no input or output; it only saves you from
   writing the `filter` yourself.

Statistics can never cause a match to be *missed*: at worst a row group survives that turns out to hold
nothing useful, which costs time but not correctness.

The two numbers never match exactly - the real reader also reads the footer, page headers, and rounds reads
up to buffer boundaries - but they move together, which is the point.

### Codecs and dictionary encoding

`Main` writes the same rows four times: uncompressed, Snappy, Zstandard, and Zstandard with dictionary
encoding switched off. Snappy is fast and moderate; Zstandard is slower to write and noticeably smaller.

**Dictionary encoding** is the other half of why Parquet files are small, and it is not compression. Within
each column chunk, Parquet builds a dictionary of the distinct values and stores a small integer per row
instead of the value. `country` has five distinct values, so its chunk becomes a five-entry dictionary plus
a run of tiny integers - a couple of percent of the file for a column present on every row. The footer names
the encoding (`RLE_DICTIONARY`, or `PLAIN_DICTIONARY` in files written the pre-2.0 way), which is how
`ColumnChunkLayout.usesDictionary` can tell.

Turning the dictionary off makes the Zstandard file *smaller* in this run, which looks backwards until you
see why: general-purpose compression finds the same repetition the dictionary was exploiting, and without a
dictionary there is also no dictionary page to store. On columns with higher cardinality, or with a faster
codec, the dictionary wins. That is the honest shape of the trade-off, and the reason to measure rather than
assume.

### Crossing into Arrow

**`ArrowBridge.scala`** builds a `VectorSchemaRoot` - Arrow's name for a batch of records: a schema, one
vector per column, and a row count. The buffers live *outside* the Java heap, so they are freed explicitly
rather than by the garbage collector; `RootAllocator` tracks every byte and throws on close if any was
leaked, which turns a memory leak into a failing test.

`sumColumn` shows what "zero copy" buys. `BigIntVector.get(i)` computes an address - buffer start plus eight
times `i` - and reads eight bytes. No record object is constructed, no field is looked up by name, and there
is nothing for the garbage collector to clean up. Aggregating a column at a time straight over a buffer is
the shape every vectorised query engine is built around.

`writeIpcFile` and `readIpcFile` use the **Arrow IPC file format** (IPC stands for inter-process
communication). The name is the whole idea: the bytes in the file are the *same layout* as the bytes in
memory, framed by a little metadata. Loading one is closer to a memory map than to parsing. That is also why
the Arrow file in the run below is about four times *larger* than the Parquet file holding the same rows -
it is uncompressed and unencoded on purpose, because the goal is to be usable instantly rather than small.

This is why Arrow is the interchange format between processes and languages: a Python, Rust, C++ or Java
program that speaks Arrow can be handed a memory region instead of a serialised stream. Example 12 builds
directly on that.

### Object storage

**`ObjectStore.scala`** optionally copies the finished archive to MinIO. It exists to make one point
concrete: nothing about Parquet or Arrow depends on a local disk. The bytes are identical whether they land
in a directory or in a bucket - which is why every lakehouse in this repository can keep its tables in
object storage and still be read by any engine.

### Why this example needs almost no infrastructure

Because a file format is not a service. Parquet has no server, no coordinator, no metastore, no catalog and
no daemon: it is a specification for arranging bytes, implemented as a library that runs inside your own
process. Arrow is the same for memory. Everything demonstrated here - writing, footer inspection, projection
and predicate pushdown, codec comparison, vector allocation, IPC round-trip - happens inside one JVM (Java
Virtual Machine) process against a directory on disk.

The single MinIO container in `docker/docker-compose.yml` is therefore not needed to run the example. It is
there so you can point the same code at object storage and confirm that nothing changes. Everything else in
this repository that looks like it is "about Parquet" is really about the engine reading it; this example is
the part underneath, and it is small enough to test exhaustively - which is exactly what
`./mill examples.11-parquet-arrow-toolkit.test` does, with no Docker running at all.

## Run it

Everything except the last step runs with no containers. From the repository root:

```bash
./mill examples.11-parquet-arrow-toolkit.test
./mill examples.11-parquet-arrow-toolkit.run
```

The archive is written to `out/11-parquet-arrow-toolkit/`. Expect roughly this (numbers vary a little with
the library version):

```
== 1. archive: 29905 order lines from 12000 orders

file size        567.93 KiB
written by       parquet-mr version 1.15.2 (...)
rows             29905
row groups       9
columns          8 (order_id, customer_id, country, placed_at, sku, quantity, ...)
codec            SNAPPY
data / decoded   555.29 KiB / 995.41 KiB  (1.8x compression)

== 3. columns, as the footer records them

column            size        share  encodings                    min            max
----------------  ----------  -----  ---------------------------  -------------  -------------
order_id          127.01 KiB  22.9%  BIT_PACKED,PLAIN_DICTIONARY  order-0000141  order-0999902
country           11.67 KiB   2.1%   BIT_PACKED,PLAIN_DICTIONARY  DE             UA
...

== 4. what each read strategy has to touch

read strategy                          row groups  bytes to read  vs full scan
-------------------------------------  ----------  -------------  ------------
full scan (all columns, all groups)    9 of 9      555.29 KiB     1.0x
projection (line_total_cents, sku)     9 of 9      129.77 KiB     4.3x
predicate (placed_at in a 20% window)  3 of 9      187.86 KiB     3.0x

== 5. compression codecs and dictionary encoding, same rows each time

variant              file size    vs uncompressed  dictionary
-------------------  -----------  ---------------  ----------
uncompressed         1008.09 KiB  1.0x             yes
snappy               567.93 KiB   1.8x             yes
zstd                 362.62 KiB   2.8x             yes
zstd, no dictionary  328.32 KiB   3.1x             no

== 6. the same data in Apache Arrow, in memory and as an IPC file

Arrow IPC file: 2.25 MiB in 8 record batches, 29905 rows read back unchanged: true
```

To also exercise the object-storage half, start the one container and re-run with the upload switched on:

```bash
docker compose -f examples/11-parquet-arrow-toolkit/docker/docker-compose.yml up -d --wait
docker compose -f examples/11-parquet-arrow-toolkit/docker/docker-compose.yml run --rm minio-init
UPLOAD_TO_OBJECT_STORE=true ./mill examples.11-parquet-arrow-toolkit.run
```

Ports and addresses:

| what | address |
| --- | --- |
| MinIO S3 API, used by the example | <http://localhost:11100> |
| MinIO web console (`minioadmin` / `minioadmin`) | <http://localhost:11101> |

Open the console, go to the `archive` bucket and the `orders` prefix, and you will see the six files the run
produced - the four Parquet variants, the default archive, and the Arrow IPC file.

Every setting has an environment variable: `ARCHIVE_DIR`, `ORDER_COUNT`, `GENERATOR_SEED`, `ARROW_BATCH_ROWS`,
`UPLOAD_TO_OBJECT_STORE`, `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_PREFIX`.

## What to try next

- **Break the sort.** In `OrderArchive.rowsFrom`, remove the `.sortBy(_.placedAtEpochMillis)` and run again.
  Every row group's `placed_at` range now spans the whole archive, none can be skipped, and the predicate
  row reports `9 of 9`. This is the single most instructive change in the example.
- **Change the row-group size.** Set `WriteOptions.DemonstrationRowGroupBytes` to 16 KiB and then to 4 MiB.
  Small groups skip more precisely but bloat the footer; one large group cannot skip at all.
- **Widen or narrow the predicate window.** Edit `Main.middleWindow` to take a fiftieth of the range instead
  of a fifth, and watch the surviving row groups drop to one.
- **Project a different pair of columns.** Change `OrderArchive.ProjectedColumns` to
  `Set("country", "quantity")` - two heavily dictionary-encoded columns - and see the projected read fall to
  a few percent of the full scan.
- **Raise the log level.** Set `org.slf4j.simpleLogger.defaultLogLevel=info` in `resources/simplelogger.properties`
  to watch Parquet announce every row group it reads and every filter it applies.
- **Inspect the files with another tool.** `pip install parquet-tools` then
  `parquet-tools inspect out/11-parquet-arrow-toolkit/orders.parquet`, and compare its footer dump with what
  section 2 and 3 printed. The formats are open; nothing here is specific to Scala or the JVM.

## Clean up

```bash
docker compose -f examples/11-parquet-arrow-toolkit/docker/docker-compose.yml down -v
```

The archive files under `out/11-parquet-arrow-toolkit/` are ordinary files; delete the directory when you no
longer want them.
