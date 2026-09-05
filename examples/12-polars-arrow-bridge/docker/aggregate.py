"""Read the Arrow buffers the Scala side wrote, aggregate them with Polars, write the answer back as Arrow.

Polars is a DataFrame library written in Rust. A DataFrame is a table with named, typed columns - the same idea as a
spreadsheet or a SQL table, held in memory. Polars stores those columns in the Apache Arrow layout, which is the reason
this script can open the file the Java Virtual Machine produced without converting anything: the bytes on disk already
are the bytes Polars wants in memory.

The script is deliberately linear and prints what it does at every step, because its output is half the lesson.
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))

ORDER_LINES = DATA_DIR / "order_lines.arrow"
REGIONS = DATA_DIR / "regions.arrow"
PARQUET_DIR = DATA_DIR / "order_lines_parquet"

REVENUE_OUT = DATA_DIR / "polars_revenue.arrow"
TIMING_OUT = DATA_DIR / "polars_timing_millis.txt"
PLAN_OUT = DATA_DIR / "polars_query_plan.txt"
INPUT_MANIFEST_OUT = DATA_DIR / "polars_input.sha256"


def replace_text(target: Path, contents: str) -> None:
    """Write a complete sibling file before asking the filesystem to replace the target."""
    temporary = target.with_name(f".{target.name}.tmp")
    try:
        temporary.write_text(contents, encoding="utf-8")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(64 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def manifest_for(order_lines: Path, regions: Path) -> str:
    """Describe the exact two inputs using the format the Scala reader verifies."""
    return "".join(f"{path.name}  {sha256(path)}\n" for path in (order_lines, regions))


def input_manifest() -> str:
    return manifest_for(ORDER_LINES, REGIONS)


def banner(text: str) -> None:
    print()
    print(text)
    print("-" * len(text))


def require_inputs() -> None:
    """Fail with an instruction rather than a stack trace when the Scala side has not run yet."""
    missing = [str(path) for path in (ORDER_LINES, REGIONS) if not path.exists()]
    if missing:
        raise SystemExit(
            "missing input file(s): "
            + ", ".join(missing)
            + "\nrun `./mill examples.12-polars-arrow-bridge.run` from the repository root first."
        )


def revenue_per_country() -> pl.LazyFrame:
    """Build the query plan that answers the same question the Scala code answers.

    Nothing is read here. `scan_ipc` returns a LazyFrame: a description of a computation, not a table. Polars
    optimises the whole description before touching a single byte, which is how it knows that only four of the eight
    columns in the file are needed and that the join can be pushed around.
    """
    order_lines = pl.scan_ipc(ORDER_LINES)
    regions = pl.scan_ipc(REGIONS)

    return (
        order_lines.join(regions, on="country", how="left")
        .group_by("country", "region")
        .agg(
            pl.col("order_id").n_unique().cast(pl.Int64).alias("order_count"),
            pl.col("quantity").sum().cast(pl.Int64).alias("units"),
            pl.col("line_total_cents").sum().cast(pl.Int64).alias("revenue_cents"),
        )
        .sort("country")
    )


def best_selling_products() -> pl.DataFrame:
    """A window expression: each product's share of the revenue of its own region.

    `over("region")` tells Polars to evaluate the sum once per region and broadcast it back onto every row of that
    region, without collapsing the rows the way a group-by would. It is the same idea as SQL's
    `SUM(...) OVER (PARTITION BY region)`.
    """
    return (
        pl.scan_ipc(ORDER_LINES)
        .join(pl.scan_ipc(REGIONS), on="country", how="left")
        .group_by("region", "sku")
        .agg(pl.col("line_total_cents").sum().alias("revenue_cents"))
        .with_columns(
            (pl.col("revenue_cents") / pl.col("revenue_cents").sum().over("region") * 100)
            .round(2)
            .alias("pct_of_region")
        )
        .sort("region", "revenue_cents", descending=[False, True])
        .collect()
    )


def parquet_matches_arrow(expected_rows: int) -> None:
    """Check that the Parquet copy of the fact table holds the same rows as the Arrow copy.

    Parquet is the compressed at-rest format; Arrow is the uncompressed in-memory one. Polars reads both, and the point
    of writing both from Scala is that you can pick per situation: Parquet when the file has to be stored or shipped,
    Arrow when another process on the same machine is about to read it immediately.
    """
    if not PARQUET_DIR.exists():
        print(f"no Parquet directory at {PARQUET_DIR}, skipping the cross-check")
        return
    actual_rows = pl.scan_parquet(PARQUET_DIR / "*.parquet").select(pl.len()).collect().item()
    verdict = "matches" if actual_rows == expected_rows else "DIFFERS FROM"
    print(f"parquet holds {actual_rows:,} rows, which {verdict} the {expected_rows:,} rows in the Arrow file")


def main() -> None:
    global pl
    import polars as pl

    require_inputs()
    source_manifest = input_manifest()

    print(f"polars {pl.__version__}")
    print(f"reading {ORDER_LINES} ({ORDER_LINES.stat().st_size / 1048576:.1f} MiB)")

    plan = revenue_per_country()

    banner("the optimised query plan (read it bottom-up)")
    explained = plan.explain()
    print(explained)
    replace_text(PLAN_OUT, explained + "\n")

    # `engine="streaming"` runs the plan in chunks instead of loading the whole file at once, so the same query works
    # on a file larger than the machine's memory. On a file this size it mostly demonstrates that the option exists.
    banner("running it")
    started_at = time.perf_counter()
    revenue = plan.collect(engine="streaming")
    elapsed_millis = (time.perf_counter() - started_at) * 1000
    print(revenue)
    print(f"polars took {elapsed_millis:.2f} ms")

    banner("best selling products per region (a window expression)")
    print(best_selling_products())

    banner("cross-checking the Parquet copy")
    total_rows = pl.scan_ipc(ORDER_LINES).select(pl.len()).collect().item()
    parquet_matches_arrow(total_rows)

    # Hand the answer back the same way it arrived: as an Arrow buffer, not as comma-separated text.
    if input_manifest() != source_manifest:
        raise SystemExit("Arrow inputs changed while Polars was reading them; rerun the container")
    # Keep the last valid result if this run fails. The manifest is published last as the completion marker: until then
    # Scala either accepts the previous result for unchanged inputs or rejects it as stale for changed inputs.
    temporary_revenue = REVENUE_OUT.with_name(f".{REVENUE_OUT.name}.tmp")
    try:
        revenue.write_ipc(temporary_revenue)
        os.replace(temporary_revenue, REVENUE_OUT)
    finally:
        temporary_revenue.unlink(missing_ok=True)
    replace_text(TIMING_OUT, f"{elapsed_millis:.2f}\n")
    replace_text(INPUT_MANIFEST_OUT, source_manifest)

    banner("written back for the JVM")
    print(f"{REVENUE_OUT}  ({revenue.height} rows)")
    print(f"{TIMING_OUT}")
    print(f"{PLAN_OUT}")
    print()
    print("now run `./mill examples.12-polars-arrow-bridge.run` again to read this result from Scala.")


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--input-manifest":
        print(manifest_for(Path(sys.argv[2]), Path(sys.argv[3])), end="")
    else:
        main()
