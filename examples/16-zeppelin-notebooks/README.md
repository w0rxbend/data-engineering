# Example 16 - Apache Zeppelin: the notebook front end of the lakehouse

Apache Zeppelin is a **notebook server**: a web application in which a document is made of *paragraphs*,
each of which can be run, and whose output - a table, a chart, rendered text - appears underneath it.
The document lives on the server, not on a laptop, so anyone with the address can open it, run it and
see the same numbers.

This example puts Zeppelin in front of the online-shop lakehouse that the rest of this repository
builds. It ships two ready-made notes that are installed into the container and are therefore present the
first time the server starts, and it covers the four things that confuse newcomers to Zeppelin:
interpreters, the `%` magics, dynamic forms, and how a note is shared or scheduled.

## The use case

An analyst wants to know how the shop is doing this fortnight. She does not want to install Apache
Spark, a Java Development Kit, a Trino client or a charting library; she wants a browser tab. She opens
Zeppelin, picks a country from a dropdown, and gets orders per day and revenue per article - straight
off the Delta Lake tables the data engineers already maintain, with no export and no copy of the data.

The data itself is the shared domain of this repository: `Order`s produced by
`de.common.gen.DataGenerator`, the same generator every other example uses.

## How it works

### The moving parts

```
  MinIO  ──►  Hive Metastore  ──►  Trino  ──►  %jdbc  paragraphs
   (S3)          (catalogue)                        Zeppelin
     └────────────────────────────────►  %spark paragraphs
```

MinIO is an object store that speaks the Amazon S3 protocol - it stands in for the cloud storage a real
lakehouse lives on. The Delta Lake tables are folders of Apache Parquet files in that store, plus a
transaction log saying which files make up the current version of each table. Two engines read those
same files: Apache Spark inside the Zeppelin container, and Trino in its own container. The Hive
Metastore is the small catalogue Trino consults to learn which tables exist and where their folders are.

### The files

| file | what it is |
| --- | --- |
| `resources/notebooks/*.zpln` | the two shipped notes. A `.zpln` file is a JSON document named `<note name>_<note id>.zpln`; Zeppelin scans its notebook folder at startup, which is why a file placed there appears in the note list with no import step. |
| `resources/interpreter/interpreter.json` | the interpreter configuration: which interpreters exist and how they are set up. Installed as Zeppelin's own `conf/interpreter.json`. |
| `src/de/zeppelin/notebooks/LakehouseSeed.scala` | pure functions turning generated `Order`s into the SQL that creates and fills the Delta Lake tables. No input/output anywhere in it. |
| `src/de/zeppelin/notebooks/TrinoSession.scala` | the thin adapter that sends those statements over a Java Database Connectivity (JDBC) connection, and waits for the Trino coordinator to be ready. |
| `src/de/zeppelin/notebooks/Notebook.scala`, `Magic.scala`, `DynamicForm.scala`, `InterpreterConfig.scala` | a small reader for the two file formats above, so the notebooks can be checked by a test. |
| `src/de/zeppelin/notebooks/NotebookCheck.scala` | the rules a shipped notebook must satisfy. |
| `src/de/zeppelin/notebooks/Main.scala` | wires the two together: `check` validates the notebooks, `seed` loads the lakehouse. |
| `docker/docker-compose.yml` | the whole stack: MinIO, the metastore, Trino, Zeppelin, and the init containers that prepare them. |

### Interpreters, and the `%` magics

Zeppelin runs no code itself. Every paragraph is handed to an **interpreter** - a separate operating
system process that knows one language or one system. The first line of the paragraph, the **magic**,
picks which one:

| magic | meaning |
| --- | --- |
| `%md` | the `md` interpreter setting: renders Markdown |
| `%spark` | the `spark` setting, its default interpreter: Scala against a live Spark session |
| `%spark.sql` | the `spark` setting, its `sql` interpreter: SQL over the tables that session knows |
| `%jdbc` | the `jdbc` setting: SQL over a JDBC driver, here Trino's |

There is a vocabulary trap worth naming. An **interpreter group** is the code - `jdbc` is the code that
can talk to any database. An **interpreter setting** is a named, configured instance of a group -
`interpreter.json` here defines one setting called `jdbc` whose address happens to be Trino's. A
paragraph's magic names the *setting*. A second setting called `warehouse`, pointing the same `jdbc`
code at a different database, would be selected by writing `%warehouse`.

Inside one `jdbc` setting there is a third level, the **connection prefix**: the properties are called
`default.url`, `default.driver` and so on, and a second set named `analytics.url`, `analytics.driver`
would be reachable as `%jdbc(analytics)`. This example uses only the `default` prefix.

`interpreter.json` in this example configures three settings and nothing else, so `%python`, `%sh` and
`%flink` are deliberately unavailable. Every property has a `description` field, so the file also reads
as documentation; the same values are shown in the browser under **Interpreter** in the top-right menu.

### Interpreter binding, and why paragraph order matters

**Interpreter binding** is the per-note list of settings a note may use, reachable through the gear icon
in the note toolbar. The first entry is the note's default: a paragraph with no magic runs on it. Note
01 defaults to `spark`, note 02 to `jdbc`.

Zeppelin has **no dependency graph between paragraphs**. A paragraph sees whatever the ones
before it left behind in the interpreter process. In note 01 the Scala paragraph registers the temporary
views `orders` and `order_lines`; every later `%spark.sql` paragraph queries them. Run them out of
order, or restart the `spark` interpreter, and the SQL paragraphs fail until the Scala one has run
again. *Run all paragraphs* in the note toolbar runs them top to bottom, which is why it is the button
to press.

Each interpreter setting also chooses how its process is shared - one process for the whole server, one
per note, or one per user. Sharing is what makes those temporary views visible across paragraphs;
isolating is what stops one runaway note from taking down everybody else's session.

### Dynamic forms

Writing `${country=DE,DE|PL|UA|FR|ES}` inside a paragraph makes Zeppelin draw a dropdown above it,
defaulting to `DE`, and substitute the chosen value into the text before the interpreter sees it. That
is the whole mechanism: it is a Zeppelin feature, not a Spark or SQL one, so it works in front of any
interpreter. Both shipped notes use one, and it is what lets a reader change the analysis without
touching a line of SQL.

### Visualisation

Any paragraph whose result is a table gets a row of icons underneath: table, bar chart, pie chart, area
chart, line chart, scatter plot. The settings panel next to them decides which column is a *key* (the
axis) and which is a *value* (the height of the bar). The choice is stored in the note file, in each
paragraph's `config.results` object, so the shipped notes already open on the right chart.

### The test that earns its keep

A notebook is data, not code. Nothing compiles it, so a paragraph beginning `%trino` instead of `%jdbc`
would fail only when a reader clicks run, minutes after the containers have started.
`ShippedNotebooksSuite` reads the exact files the compose stack installs and asserts that:

* every note is valid JSON with an identifier, a name and paragraphs;
* every note's file name agrees with the note inside it;
* every magic names an interpreter setting that `interpreter.json` actually configures, and a dotted magic such as
  `%spark.sql` names a real interpreter inside that setting;
* malformed leading magic syntax is rejected instead of silently falling back to the note default;
* the `jdbc` setting points at Trino with Trino's driver, and the `spark` setting at MinIO with the
  Delta Lake extension;
* the country dropdown offers exactly the five countries the shared generator produces, and defaults to
  one of them.

It needs no Docker, and it runs in well under a second.

The command-line `check` runs the same rules and now exits non-zero before `seed` if any notebook is invalid. Printing
a warning and continuing would leave a fully populated lakehouse paired with a notebook that still fails on click.

### What the Docker Compose stack does before Zeppelin starts

The Zeppelin image contains the *interpreter* that drives Apache Spark but no Spark distribution, and it
contains no Delta Lake, no S3 filesystem and no Trino driver. The `zeppelin-init` container fills those
gaps once, into a named volume: it downloads Apache Spark 3.5.3, drops the Delta Lake 3.2.1, `hadoop-aws`
and AWS SDK jars into that distribution's `jars` folder, fetches the Trino driver, and copies this
example's notebooks and interpreter configuration into place. It runs the Zeppelin image itself, because
a Docker named volume is pre-filled from the image of the first container that mounts it - using any
other image would wipe the configuration files that ship inside `conf`.

The notebooks and `interpreter.json` are copied into writable volumes rather than bind-mounted from the
repository, because Zeppelin rewrites both when a reader edits a note or an interpreter in the browser.
The consequence is worth knowing: **`docker compose up` re-installs the shipped notes**, discarding
browser edits to them. Export a note you want to keep before bringing the stack up again.

The version pins are intentional. Zeppelin 0.12 documents Spark 3.3 through 3.5 and Scala 2.12/2.13 support when
`SPARK_HOME` is supplied, and the [Delta Lake compatibility table](https://docs.delta.io/releases/) pairs Delta 3.2.x
with Spark 3.5.x. This stack therefore combines Spark 3.5.3's Scala 2.12 distribution with Delta 3.2.1. Trino 476's
[Delta Lake connector](https://trino.io/docs/476/connector/delta-lake.html) supplies the other engine and uses its
native S3 client for MinIO. These pins establish upstream-declared compatibility; they do not make concurrent writes
from Spark and Trino safe. The exercise has one writer, the Trino seeder.

## Run it

From the repository root:

```bash
# 1. Start the stack. The first run downloads Apache Spark (about 400 MB) inside the
#    init container, so expect a few minutes; later runs reuse the volume.
docker compose -f examples/16-zeppelin-notebooks/docker/docker-compose.yml up -d --wait

# 2. Check the notebooks and load the lakehouse: 2100 orders spread over 14 days.
./mill examples.16-zeppelin-notebooks.run
```

The default mode checks first and seeds only if validation passes. Seeding is repeatable but not atomic across both
tables: it empties and refills `orders`, then does the same for `order_lines`, as a sequence of Delta transactions. If
the process or Trino stops halfway, readers can temporarily see incomplete or mismatched tables. Restore the stack and
run the same command again; the initial deletes repair the partial attempt. Do not run two seeders, or a Spark writer,
at the same time—the Compose catalog explicitly enables Trino's non-concurrent-write mode for this single-writer demo.

Expected output from step 2:

```
Configured interpreter settings: jdbc, md, spark
01 Lakehouse Tour: 5 paragraphs, interpreters spark, md -> ok
02 Trino Federation: 5 paragraphs, interpreters jdbc, md -> ok
Connecting to jdbc:trino://localhost:11680 as seeder
Seeding delta.shop with 2100 orders over 14 days (80 statements)
Seeded delta.shop.orders and delta.shop.order_lines
```

Then open **<http://localhost:11690>**. Two notes are waiting in the list:

* **01 Lakehouse Tour** - Markdown, a Scala paragraph reading the Delta Lake tables, and two `%spark.sql`
  paragraphs with a country dropdown and a bar chart. Press *Run all paragraphs*; the first Spark
  paragraph takes about half a minute while the interpreter starts.
* **02 Trino Federation** - the same questions asked of Trino through `%jdbc`.

The other addresses this stack publishes:

| address | what it is |
| --- | --- |
| <http://localhost:11690> | Zeppelin, the notebook interface |
| <http://localhost:11680> | the Trino web interface: every query, its plan and how much it read |
| <http://localhost:11601> | the MinIO console, user `minioadmin`, password `minioadmin` |
| <http://localhost:11600> | the MinIO S3 endpoint itself |
| <http://localhost:11640> | the Spark driver's web interface, live while a `%spark` paragraph runs |

Run only the notebook checks, with nothing started at all:

```bash
./mill examples.16-zeppelin-notebooks.test
```

## What to try next

1. **Break a magic on purpose.** Change `%spark.sql` to `%sparksql` in
   `resources/notebooks/01 Lakehouse Tour_2ZEPSHOP01.zpln` and run
   `./mill examples.16-zeppelin-notebooks.test`. The failure names the interpreter and lists the ones
   that are configured - the mistake is caught without a single container running.
   Try `%spark.typo` as well: the setting exists, but the new member-level check reports that `typo` is not one of its
   configured interpreters. The default `all` command stops before changing either Delta table.
2. **Add a choice to the dropdown.** Put `PT` into the country form and re-run: the query returns
   nothing, because the shared generator only produces the five countries listed. The test notices this
   too, which is the point of asserting on the option list.
3. **Restart the `spark` interpreter** from the interpreter page, then re-run a `%spark.sql` paragraph in
   note 01. It fails until the Scala paragraph has run again - a concrete demonstration of what
   interpreter state means.
4. **Schedule a note.** The clock icon in the note toolbar accepts a cron expression, for example
   `0 0 6 * * ?` for every morning at six. Zeppelin then runs the whole note on that schedule and leaves
   the results in it, turning the note into a daily report.
5. **Export and re-import.** The export icon downloads the note as a `.zpln` file - the same JSON as in
   `resources/notebooks/`. That file is what makes a notebook reviewable work rather than a private
   scratchpad: commit it, and the test suite in this example starts guarding it.
6. **Compare the two engines.** Run the revenue-per-article paragraph in both notes. The numbers agree
   because Spark and Trino read the same Delta Lake files - then look at the Trino web interface to see
   how much of the object store the query actually touched.
7. **Interrupt a seed and replay it.** Stop Trino during the insert phase, observe that the command fails, start Trino,
   and run the seed again. The second run deletes both partial table contents and deterministically rebuilds them. This
   demonstrates retry recovery, not a cross-table transaction: readers are not isolated from the partial interval.

## Further reading in this repository

This is the last of the sixteen examples, and it sits on top of several of them:

* **07 - Spark and Delta Lake** builds the medallion lakehouse whose table format note 01 reads.
* **09 - Trino over a lakehouse** is the engine behind note 02, with cross-catalog joins and query plans.
* **10 - Presto and Hive** is the same idea with the engine Trino was forked from.
* **05 and 08** fill such tables continuously from Apache Kafka, which is what a real shop would do
  instead of running a seeding helper.

## Clean up

```bash
docker compose -f examples/16-zeppelin-notebooks/docker/docker-compose.yml down -v
```

`-v` also removes the volumes, including the downloaded Spark distribution - leave it out if you intend
to start the stack again soon and would rather not download it a second time.
