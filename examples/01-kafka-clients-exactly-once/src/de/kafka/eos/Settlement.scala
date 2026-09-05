package de.kafka.eos

import de.common.domain.{Money, Order, OrderId, Payment, PaymentStatus}

/** A reason why an order cannot be turned into a payment. */
enum SettlementRejection {

  /** The order carries no lines, so there is nothing to charge for. */
  case NothingToCharge(orderId: OrderId)

  /** The total came out as zero or negative; charging it would be wrong. */
  case NonPositiveTotal(orderId: OrderId, cents: Long)

  /** The payment provider of this example only settles a single currency. */
  case UnsupportedCurrency(orderId: OrderId, currency: String)

  def describe: String = this match {
    case NothingToCharge(orderId) =>
      s"order ${orderId.value} has no order lines"
    case NonPositiveTotal(orderId, cents) =>
      s"order ${orderId.value} totals $cents cents, which is not a chargeable amount"
    case UnsupportedCurrency(orderId, currency) =>
      s"order ${orderId.value} is priced in $currency, and only ${Settlement.settledCurrency} can be charged"
  }
}

/**
 * The business rule of this example, with no Kafka anywhere in sight.
 *
 * Keeping the rule in its own object is what makes the service testable: the unit tests in `SettlementTest` exercise it
 * directly, with no broker running.
 */
object Settlement {

  /** The one currency this (deliberately simple) payment provider accepts. */
  val settledCurrency: String = "EUR"

  /**
   * Works out the payment to charge for an order.
   *
   * @param order
   *   the order to settle
   * @param occurredAtEpochMillis
   *   the wall-clock time to stamp on the payment, passed in rather than read from the system clock so that the result
   *   is reproducible
   * @return
   *   the payment to publish, or the reason it cannot be charged
   */
  def settle(order: Order, occurredAtEpochMillis: Long): Either[SettlementRejection, Payment] = {
    val total: Money = order.total
    if (order.lines.isEmpty) {
      Left(SettlementRejection.NothingToCharge(order.id))
    } else if (total.currency != settledCurrency) {
      Left(SettlementRejection.UnsupportedCurrency(order.id, total.currency))
    } else if (total.cents <= 0L) {
      Left(SettlementRejection.NonPositiveTotal(order.id, total.cents))
    } else {
      Right(Payment(order.id, total, PaymentStatus.Captured, occurredAtEpochMillis))
    }
  }
}
