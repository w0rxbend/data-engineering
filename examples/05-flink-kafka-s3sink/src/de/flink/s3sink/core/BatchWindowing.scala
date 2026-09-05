package de.flink.s3sink.core

import de.common.domain.{CustomerId, Order}

/**
 * The windowing decision, expressed as a pure function.
 *
 * A single customer can easily have orders belonging to two different windows buffered at the same time: Apache Flink
 * advances the watermark only every couple of hundred milliseconds, so a fast source can deliver the first order of the
 * next window before the previous window's timer has fired. Mixing both into one output record would silently corrupt
 * the results.
 *
 * Every buffered order therefore remembers which window it belongs to, and closing a window means "take out exactly the
 * orders of that window, leave the rest in the buffer".
 */
object BatchWindowing {

  /** One order waiting in state, tagged with the window it was assigned to. */
  final case class BufferedOrder(windowStartMillis: Long, order: Order)

  /**
   * @param batch
   *   the record to write, or `None` when the window turned out to be empty (for example after a duplicate timer)
   * @param remaining
   *   the orders that belong to a later window and stay buffered
   */
  final case class ClosedWindow(batch: Option[CustomerOrderBatch], remaining: List[BufferedOrder])

  def assign(order: Order, windowSizeMillis: Long): BufferedOrder =
    BufferedOrder(EventTimeWindows.windowStart(order.placedAtEpochMillis, windowSizeMillis), order)

  /** Closes the window starting at `windowStartMillis`. */
  def close(
      customerId: CustomerId,
      buffered: List[BufferedOrder],
      windowStartMillis: Long,
      windowSizeMillis: Long
  ): ClosedWindow = {
    val (closing, remaining) = buffered.partition(_.windowStartMillis == windowStartMillis)
    val accumulator          = BatchAccumulator.fold(closing.map(_.order))
    val batch                =
      if (accumulator.isEmpty) None
      else Some(accumulator.toBatch(customerId, windowStartMillis, windowStartMillis + windowSizeMillis))
    ClosedWindow(batch, remaining)
  }
}
