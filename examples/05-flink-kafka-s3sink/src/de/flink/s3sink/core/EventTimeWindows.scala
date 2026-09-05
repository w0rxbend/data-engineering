package de.flink.s3sink.core

/**
 * Arithmetic for fixed-size, non-overlapping ("tumbling") event-time windows.
 *
 * Event time is the timestamp carried inside the event itself -- for an `Order` that is `placedAtEpochMillis`, the
 * moment the shop accepted the order. It is deliberately not the moment Apache Flink happened to read the record, which
 * would make results depend on network delays and on how often the job is restarted.
 *
 * Windows are aligned to the epoch (1970-01-01T00:00:00Z), so a one-hour window always starts on the hour and every
 * parallel worker agrees on the boundaries without any coordination.
 */
object EventTimeWindows {

  /** Start of the window that contains `epochMillis`, in milliseconds. */
  def windowStart(epochMillis: Long, windowSizeMillis: Long): Long = {
    require(windowSizeMillis > 0L, "window size must be a positive number of milliseconds")
    Math.floorDiv(epochMillis, windowSizeMillis) * windowSizeMillis
  }

  /**
   * End of the window that contains `epochMillis`, exclusive.
   *
   * A timer registered on this timestamp fires as soon as the watermark passes it, which is Flink's way of saying "no
   * further event for this window is expected".
   */
  def windowEnd(epochMillis: Long, windowSizeMillis: Long): Long =
    windowStart(epochMillis, windowSizeMillis) + windowSizeMillis
}
