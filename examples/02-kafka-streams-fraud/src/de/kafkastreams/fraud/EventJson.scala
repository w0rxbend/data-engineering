package de.kafkastreams.fraud

import de.common.domain.*
import de.common.json.{Codecs, Json}

/**
 * Translation between the domain model and the JSON bytes on the Kafka topics.
 *
 * Writing is delegated to `de.common.json.Codecs` so that every example in this repository puts byte-identical `Order`
 * and `Payment` messages on the wire. Reading is done here with `ujson`, uPickle's small JSON parser: the shared
 * `common` module cannot depend on a JSON library because it is compiled for three different Scala versions at once, so
 * parsing lives in the examples.
 */
object EventJson {

  // ---------------------------------------------------------------- reading

  def readMoney(node: ujson.Value): Money =
    Money(node("cents").num.toLong, node("currency").str)

  def readOrderLine(node: ujson.Value): OrderLine =
    OrderLine(Sku(node("sku").str), node("quantity").num.toInt, readMoney(node("unitPrice")))

  def readOrder(raw: String): Order = {
    val node = ujson.read(raw)
    Order(
      id = OrderId(node("id").str),
      customerId = CustomerId(node("customerId").str),
      lines = node("lines").arr.map(readOrderLine).toList,
      placedAtEpochMillis = node("placedAt").num.toLong,
      country = node("country").str
    )
  }

  def readPayment(raw: String): Payment = {
    val node   = ujson.read(raw)
    val status = node("status").str
    Payment(
      orderId = OrderId(node("orderId").str),
      amount = readMoney(node("amount")),
      status = PaymentStatus
        .fromString(status)
        .getOrElse(throw new IllegalArgumentException(s"unknown payment status: $status")),
      occurredAtEpochMillis = node("occurredAt").num.toLong
    )
  }

  def readCustomerRisk(raw: String): CustomerRisk = {
    val node = ujson.read(raw)
    CustomerRisk(CustomerId(node("customerId").str), node("tier").str)
  }

  def readPaidOrder(raw: String): PaidOrder = {
    val node   = ujson.read(raw)
    val status = node("status").str
    PaidOrder(
      orderId = OrderId(node("orderId").str),
      customerId = CustomerId(node("customerId").str),
      country = node("country").str,
      amount = readMoney(node("amount")),
      status = PaymentStatus
        .fromString(status)
        .getOrElse(throw new IllegalArgumentException(s"unknown payment status: $status")),
      occurredAtEpochMillis = node("occurredAt").num.toLong
    )
  }

  def readDeclineTally(raw: String): DeclineTally = {
    val node = ujson.read(raw)
    DeclineTally(node("count").num.toInt, node("totalCents").num.toLong)
  }

  def readFraudAlert(raw: String): FraudAlert = {
    val node = ujson.read(raw)
    FraudAlert(
      customerId = CustomerId(node("customerId").str),
      declinedCount = node("declinedCount").num.toInt,
      totalDeclinedCents = node("totalDeclinedCents").num.toLong,
      windowStartEpochMillis = node("windowStart").num.toLong,
      windowEndEpochMillis = node("windowEnd").num.toLong,
      riskTier = node("riskTier").str
    )
  }

  // ---------------------------------------------------------------- writing

  def writeOrder(value: Order): String = Codecs.order(value)

  def writePayment(value: Payment): String = Codecs.payment(value)

  def writeCustomerRisk(value: CustomerRisk): String =
    Json.obj(
      "customerId" -> Some(Json.string(value.customerId.value)),
      "tier"       -> Some(Json.string(value.tier))
    )

  def writePaidOrder(value: PaidOrder): String =
    Json.obj(
      "orderId"    -> Some(Json.string(value.orderId.value)),
      "customerId" -> Some(Json.string(value.customerId.value)),
      "country"    -> Some(Json.string(value.country)),
      "amount"     -> Some(Codecs.money(value.amount)),
      "status"     -> Some(Json.string(value.status.toString)),
      "occurredAt" -> Some(Json.num(value.occurredAtEpochMillis))
    )

  def writeDeclineTally(value: DeclineTally): String =
    Json.obj(
      "count"      -> Some(Json.num(value.count)),
      "totalCents" -> Some(Json.num(value.totalCents))
    )

  def writeFraudAlert(value: FraudAlert): String =
    Json.obj(
      "customerId"         -> Some(Json.string(value.customerId.value)),
      "declinedCount"      -> Some(Json.num(value.declinedCount)),
      "totalDeclinedCents" -> Some(Json.num(value.totalDeclinedCents)),
      "windowStart"        -> Some(Json.num(value.windowStartEpochMillis)),
      "windowEnd"          -> Some(Json.num(value.windowEndEpochMillis)),
      "riskTier"           -> Some(Json.string(value.riskTier))
    )
}
