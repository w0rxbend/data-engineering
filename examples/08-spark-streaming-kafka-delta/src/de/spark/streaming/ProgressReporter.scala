package de.spark.streaming

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryProgress

/**
 * Prints one readable line per completed micro-batch.
 *
 * Apache Spark publishes a `StreamingQueryProgress` object after every micro-batch, and it is the primary tool for
 * answering "is my stream keeping up?". Three numbers matter most:
 *
 *   - `inputRowsPerSecond` - how fast records arrive from Apache Kafka;
 *   - `processedRowsPerSecond` - how fast the job consumes them. When this stays below the input rate the job is
 *     falling behind and the lag grows without bound;
 *   - `numInputRows` per batch together with the batch duration - a sudden jump usually means the job was stalled and
 *     is now catching up on a backlog.
 *
 * The same information is visible in the Spark web user interface under the "Structured Streaming" tab, but a log line
 * survives the job exiting.
 */
final class ProgressReporter(log: String => Unit) extends StreamingQueryListener {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
    log(s"query '${event.name}' started (id ${event.id})")

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
    log(ProgressReporter.describe(event.progress))

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
    log(s"query ${event.id} terminated${event.exception.fold("")(reason => s" with: $reason")}")
}

object ProgressReporter {

  /** Renders the interesting fields of a progress report as a single line. */
  def describe(progress: StreamingQueryProgress): String = {
    // Spark reports NaN ("not a number") for a rate it cannot compute yet,
    // which is the case for the very first batch of a query.
    def rate(value: Double): String = if (value.isNaN) "n/a" else f"$value%.1f"

    Seq(
      s"batch ${progress.batchId}",
      s"query '${Option(progress.name).getOrElse("unnamed")}'",
      s"rows ${progress.numInputRows}",
      s"in/s ${rate(progress.inputRowsPerSecond)}",
      s"out/s ${rate(progress.processedRowsPerSecond)}",
      s"took ${progress.batchDuration}ms"
    ).mkString(" | ")
  }
}
