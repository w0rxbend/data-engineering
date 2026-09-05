package de.spark.lakehouse.core

import de.common.domain._

/**
 * Flat, table-shaped versions of the shared domain objects.
 *
 * Why a second set of case classes at all? The shared domain model in `de.common.domain` is nested (an `Order` holds a
 * `List[OrderLine]`, a `Money` holds an amount plus a currency) and uses value classes such as `OrderId`. That shape is
 * excellent for business logic and poor for a warehouse table, where analysts expect one flat row with plain columns.
 *
 * Keeping the two apart is a deliberate boundary: the domain model never learns about Apache Spark, and the table
 * layout can change without touching the domain. The functions in this file are the only translation point.
 *
 * Every record also carries `ingestedAtEpochMillis`: the moment the row landed in the lake, as opposed to the moment
 * the business event happened. Deduplication in the silver layer needs it to decide which of two copies of the same
 * order is the newer one.
 */
object BronzeRecords {

  /** One row of the bronze orders table: an order flattened to a single line, prices in cents. */
  final case class OrderRow(
      orderId: String,
      customerId: String,
      country: String,
      lineCount: Int,
      totalCents: Long,
      currency: String,
      placedAtEpochMillis: Long,
      ingestedAtEpochMillis: Long
  )

  /** One row of the bronze payments table. */
  final case class PaymentRow(
      orderId: String,
      amountCents: Long,
      currency: String,
      status: String,
      occurredAtEpochMillis: Long,
      ingestedAtEpochMillis: Long
  )

  /** One row of the bronze shipments table. */
  final case class ShipmentRow(
      orderId: String,
      status: String,
      occurredAtEpochMillis: Long,
      ingestedAtEpochMillis: Long
  )

  def orderRow(order: Order, ingestedAtEpochMillis: Long): OrderRow = {
    val total = order.total
    OrderRow(
      orderId = order.id.value,
      customerId = order.customerId.value,
      country = order.country,
      lineCount = order.lines.size,
      totalCents = total.cents,
      currency = total.currency,
      placedAtEpochMillis = order.placedAtEpochMillis,
      ingestedAtEpochMillis = ingestedAtEpochMillis
    )
  }

  def paymentRow(payment: Payment, ingestedAtEpochMillis: Long): PaymentRow =
    PaymentRow(
      orderId = payment.orderId.value,
      amountCents = payment.amount.cents,
      currency = payment.amount.currency,
      status = payment.status.toString,
      occurredAtEpochMillis = payment.occurredAtEpochMillis,
      ingestedAtEpochMillis = ingestedAtEpochMillis
    )

  def shipmentRow(shipment: Shipment, ingestedAtEpochMillis: Long): ShipmentRow =
    ShipmentRow(
      orderId = shipment.orderId.value,
      status = shipment.status.toString,
      occurredAtEpochMillis = shipment.occurredAtEpochMillis,
      ingestedAtEpochMillis = ingestedAtEpochMillis
    )
}
