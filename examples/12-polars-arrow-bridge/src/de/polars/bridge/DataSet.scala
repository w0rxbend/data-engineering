package de.polars.bridge

import java.nio.file.Path

/**
 * The file layout both sides of the bridge agree on.
 *
 * The Scala process writes into this directory; the Polars container mounts the very same directory and writes its
 * answer back into it. Naming the paths in one place keeps the Python script and the Scala code from drifting apart -
 * the container mounts the directory at a fixed path, so only the file names have to match.
 */
final case class DataSet(root: Path) {

  /** The fact table: one row per order line, written by Scala, read by Polars. */
  def orderLinesArrow: Path = root.resolve("order_lines.arrow")

  /** The dimension table Polars joins against. */
  def regionsArrow: Path = root.resolve("regions.arrow")

  /** The same fact table at rest, compressed and column-encoded. */
  def parquetDirectory: Path = root.resolve("order_lines_parquet")

  /** The aggregate Polars writes back for the JVM to read. */
  def polarsRevenueArrow: Path = root.resolve("polars_revenue.arrow")

  /** SHA-256 digests of the two Arrow inputs from which Polars produced its result. */
  def polarsInputManifest: Path = root.resolve("polars_input.sha256")

  /** Milliseconds Polars needed for its aggregation, written as a single decimal number. */
  def polarsTimingMillis: Path = root.resolve("polars_timing_millis.txt")

  /** The lazy query plan Polars printed, kept so the README can point at a real file. */
  def polarsQueryPlan: Path = root.resolve("polars_query_plan.txt")
}
