package de.flink.s3sink.core

import de.common.domain.{CustomerId, Money, Order, OrderId}

/**
 * The result the job writes to object storage: everything one customer ordered during one event-time window, reduced to
 * a single record.
 *
 * @param customerId
 *   the customer the window belongs to
 * @param windowStartMillis
 *   inclusive lower bound of the window, epoch millis
 * @param windowEndMillis
 *   exclusive upper bound of the window, epoch millis
 * @param orderIds
 *   the orders that fell into the window, in arrival order
 * @param total
 *   the summed value of those orders
 */
final case class CustomerOrderBatch(
    customerId: CustomerId,
    windowStartMillis: Long,
    windowEndMillis: Long,
    orderIds: List[OrderId],
    total: Money
) {
  def orderCount: Int = orderIds.size
}

/**
 * The running total the job keeps in Flink state while a window is still open.
 *
 * Keeping this as a plain immutable value (rather than reading and writing Flink state fields directly inside the
 * process function) is what makes the windowing logic testable without starting a cluster.
 */
final case class BatchAccumulator(orderIds: List[OrderId], totalCents: Long, currency: String) {

  /** Folds one more order into the accumulator. Orders are never mutated. */
  def add(order: Order): BatchAccumulator = {
    val orderTotal = order.total
    if (orderIds.nonEmpty && orderTotal.currency != currency) {
      throw new IllegalArgumentException(
        s"order ${order.id.value} is priced in ${orderTotal.currency} but the open batch for " +
          s"customer ${order.customerId.value} is priced in $currency; a single batch cannot mix currencies"
      )
    }
    BatchAccumulator(orderIds :+ order.id, totalCents + orderTotal.cents, orderTotal.currency)
  }

  def isEmpty: Boolean = orderIds.isEmpty

  /** Closes the accumulator into the record that is written to storage. */
  def toBatch(customerId: CustomerId, windowStartMillis: Long, windowEndMillis: Long): CustomerOrderBatch =
    CustomerOrderBatch(customerId, windowStartMillis, windowEndMillis, orderIds, Money(totalCents, currency))
}

object BatchAccumulator {

  /** Currency placeholder used while the accumulator still holds no order. */
  val UnknownCurrency = "EUR"

  val empty: BatchAccumulator = BatchAccumulator(Nil, 0L, UnknownCurrency)

  /** Folds a whole sequence of orders, which is what the unit tests exercise. */
  def fold(orders: Seq[Order]): BatchAccumulator = orders.foldLeft(empty)(_ add _)
}
