# Validation

Validation is layered so quick structural mistakes fail before the mixed-version,
engine-heavy build starts. Passing a lower layer does not imply a higher one passed.

## Repository entry point

```bash
scripts/check-repo.sh static
scripts/check-repo.sh build
scripts/check-repo.sh all
```

Set `MILL_DOCKER=1` for the Docker-only build path. Native Mill runs with
`--no-server` and defaults to two workers; override `MILL_JOBS` if the machine is
memory-constrained:

```bash
MILL_JOBS=1 scripts/check-repo.sh build
MILL_DOCKER=1 scripts/check-repo.sh build
```

The Docker runner itself is intentionally fixed at two Mill workers in
`compose.yml`.

## What each layer proves

### Static contracts

`scripts/check-repo.sh static` performs:

1. `bash -n` over the repository shell scripts;
2. the Python `unittest` regression suite for `run-example.sh` argument handling,
   volume-preserving shutdown, Connect assembly, Polars batch refusal, and Docker
   Mill dispatch, plus the native Polars adapter's manifest contract;
3. `docker compose config --quiet` for the root build runner and each of the 16
   example Compose files;
4. an explicit count check that prevents a missing example stack from silently
   reducing coverage.

Compose parsing resolves YAML, interpolation, references, and the Compose model.
It does **not** pull images, execute health checks, verify remote tags, or prove
that every service starts on the current host.

### Build contracts

`scripts/check-repo.sh build` performs, in order:

1. `mill.scalalib.scalafmt/checkFormatAll` for Scala source and test formatting;
2. `__.compile` for every discovered Scala and Java module;
3. `__.test` for the complete test graph.

Mill concurrency is bounded because Spark and Flink test processes are relatively
memory-heavy. Their module definitions also disable parallel test-suite execution
where a suite owns a Spark session.

The build covers `common` on Scala 2.12, 2.13, and 3 through its Mill cross module,
plus the Scala examples and the Java Connect plugin. Tests deliberately use fakes,
temporary files, local Spark/Flink harnesses, and checked-in fixtures rather than
requiring the Compose stacks.

### Runtime smoke checks

Runtime validation is example-specific. Use the selected README as the assertion
list, and avoid treating “container is running” as proof that the data path works:

```bash
scripts/run-example.sh up 09
scripts/run-example.sh status 09
scripts/run-example.sh run 09
scripts/run-example.sh down 09
```

Use `reset` only when the scenario requires empty volumes. Example 04 must build
and mount its plugin assembly; example 12 uses its documented finite batch command
instead of `up`. A comprehensive image-start smoke run is intentionally not part
of pull-request CI because the 16 stacks are large and some download substantial
runtime assets.

## Verification record — 2026-09-05

`scripts/check-repo.sh all` passed on Linux with Docker Engine and Compose:

- all 228 Scala sources passed formatting;
- every discovered Scala and Java module compiled;
- the complete test graph passed, with 511 tests and no failures, errors, or
  skipped tests in the 19 module reports under `out/**/testForked.dest/`;
- all 11 Python regression tests passed;
- the root build runner and all 16 example Compose configurations parsed.

Mill can reuse unchanged test results, so the final console log may list fewer
tests than the complete set of module reports. The Java Connect plugin's 16 tests
were cached in this run; its configuration and partition-encoding changes had
already passed their module test command.

These additional checks exercised actual data paths:

| Path | Observed result |
| --- | --- |
| Docker Mill runner | Shared Scala 3 tests (5) and the Scala Arrow bridge tests (19) passed inside the build container; the latter require no Python installation there. |
| 01 Kafka transactions | The aborted payment was visible to `read_uncommitted` (1 record) and absent from `read_committed` (0 records). |
| 02 Kafka Streams | The one-shot scenario against a three-partition topic produced two alerts, each containing five declines and 995 cents; the seeded risk flag was `watchlist`. |
| 09 Trino | Seeded 500 orders, then completed the Delta/PostgreSQL federation walkthrough against the Compose stack. |
| 11 Parquet / Arrow | The Docker Mill command completed the local compression comparison and Arrow round trip with 29,905 rows; the optional object-store upload was skipped. |
| 12 Polars | Generated 20,000 orders, ran the native Polars container, then verified that Scala and Polars agreed on all five country aggregates and the input manifest. |
| 13 CMAK / Kafka administration | Registered the three-broker cluster, produced 500 records, consumed and committed 120, and completed the replication, lag, and reassignment reports. |
| 14 HugeGraph | Backend vertex counts, edge counts, and the six-hop shortest path all matched the Scala reference graph. |
| 15 CouchDB CDC | Published five seeded catalogue changes, persisted the bookmark, then restarted from that sequence with zero changes republished. |
| 16 Zeppelin resources | The local `check` command validated both five-paragraph notebooks and their configured interpreter names. |

This record covers selected live integrations, not a full startup and execution
of every stack. In particular, the Zeppelin check validates notebook resources;
it does not execute those paragraphs in a running Zeppelin server. Spark and
Flink tests exercise local engine harnesses; their distributed Compose jobs are
a separate runtime layer. Re-run the relevant walkthrough when changing its
images, dependencies, or external-service configuration.

## CI

`.github/workflows/ci.yml` separates fast repository contracts, a Docker Mill smoke
test, and the complete Mill build. The Docker job runs shared Scala 3 and Arrow
bridge tests through `scripts/mill-docker.sh` on a fresh runner.
The build runs on Temurin 21 so the launcher can execute every module; Mill still
selects the module-level Temurin 17 or 21 runtime from `build.mill`. The workflow
uses two Mill workers to keep Spark/Flink memory pressure bounded and cancels
superseded branch runs.

When changing build or validation behavior, keep the local script authoritative so
CI and contributor commands cannot drift apart. Mill formatting follows its
[official Scalafmt integration](https://mill-build.org/mill/scalalib/linting.html),
and Compose validation uses Docker's
[`config --quiet`](https://docs.docker.com/reference/cli/docker/compose/config/)
model check.
