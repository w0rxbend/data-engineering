package de.common.json

import de.common.domain._

/**
 * Renders the shared domain model as JSON.
 *
 * Every example that publishes domain events to Apache Kafka uses these functions, so all examples agree byte-for-byte
 * on the message layout.
 */
object Codecs {

  def money(value: Money): String =
    Json.obj("cents" -> Some(Json.num(value.cents)), "currency" -> Some(Json.string(value.currency)))

  def orderLine(line: OrderLine): String =
    Json.obj(
      "sku"       -> Some(Json.string(line.sku.value)),
      "quantity"  -> Some(Json.num(line.quantity)),
      "unitPrice" -> Some(money(line.unitPrice))
    )

  def order(value: Order): String =
    Json.obj(
      "id"         -> Some(Json.string(value.id.value)),
      "customerId" -> Some(Json.string(value.customerId.value)),
      "lines"      -> Some(Json.arr(value.lines.map(orderLine))),
      "placedAt"   -> Some(Json.num(value.placedAtEpochMillis)),
      "country"    -> Some(Json.string(value.country))
    )

  def payment(value: Payment): String =
    Json.obj(
      "orderId"    -> Some(Json.string(value.orderId.value)),
      "amount"     -> Some(money(value.amount)),
      "status"     -> Some(Json.string(value.status.toString)),
      "occurredAt" -> Some(Json.num(value.occurredAtEpochMillis))
    )

  def shipment(value: Shipment): String =
    Json.obj(
      "orderId"    -> Some(Json.string(value.orderId.value)),
      "status"     -> Some(Json.string(value.status.toString)),
      "occurredAt" -> Some(Json.num(value.occurredAtEpochMillis))
    )

  def clickEvent(value: ClickEvent): String =
    Json.obj(
      "customerId" -> Some(Json.string(value.customerId.value)),
      "page"       -> Some(Json.string(value.page)),
      "sku"        -> value.sku.map(s => Json.string(s.value)),
      "occurredAt" -> Some(Json.num(value.occurredAtEpochMillis))
    )
}
