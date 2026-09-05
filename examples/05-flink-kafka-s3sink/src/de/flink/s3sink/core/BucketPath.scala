package de.flink.s3sink.core

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

/**
 * Turns a finished batch into the directory it is stored under.
 *
 * The layout is Hive-style partitioning, meaning every directory name is a `column=value` pair:
 *
 * {{{
 * customer_id=cust-0042/dt=2023-11-14/hour=22/part-0-3
 * }}}
 *
 * Query engines such as Trino, Apache Spark and Apache Hive read those names as real columns. A query filtered to one
 * day therefore only opens the files under `dt=2023-11-14` instead of scanning the whole bucket -- an optimisation
 * called partition pruning.
 *
 * The date is derived from the *window start* and formatted in UTC. Using the window start rather than the wall clock
 * means a replay of yesterday's data lands in yesterday's directories. It does not make a fresh replay idempotent:
 * Flink generates new part-file names for a separately submitted job, so the old and replayed records coexist.
 */
object BucketPath {

  private val DatePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC)
  private val HourPattern = DateTimeFormatter.ofPattern("HH").withZone(ZoneOffset.UTC)

  /**
   * Object-storage keys have no escaping rules, so anything that could break the `column=value/` structure is replaced
   * by a hyphen.
   */
  private def sanitise(raw: String): String = {
    val cleaned = raw.trim.map(c => if (c.isLetterOrDigit || c == '-' || c == '_' || c == '.') c else '-')
    if (cleaned.isEmpty) "unknown" else cleaned
  }

  /** The directory (relative to the sink's base path) a batch is written to. */
  def forBatch(batch: CustomerOrderBatch): String = {
    val windowStart = Instant.ofEpochMilli(batch.windowStartMillis)
    s"customer_id=${sanitise(batch.customerId.value)}/" +
      s"dt=${DatePattern.format(windowStart)}/" +
      s"hour=${HourPattern.format(windowStart)}"
  }
}
