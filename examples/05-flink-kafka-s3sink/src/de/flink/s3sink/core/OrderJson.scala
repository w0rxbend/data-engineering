package de.flink.s3sink.core

import de.common.domain._
import de.common.json.Codecs
import io.circe.{Decoder, HCursor}

/**
 * Reads the shared `Order` wire format produced by `de.common.json.Codecs` and writes the batch record back out as one
 * JSON object per line.
 *
 * Decoding never throws. A malformed record is a routine event on a real Kafka topic -- one bad producer must not take
 * the whole job down -- so the failure is returned as a value and the caller decides what to do with it.
 */
object OrderJson {

  /** Why a Kafka record could not be turned into an `Order`. */
  final case class DecodeFailure(reason: String, payload: String)

  private implicit val moneyDecoder: Decoder[Money] = (cursor: HCursor) =>
    for {
      cents    <- cursor.downField("cents").as[Long]
      currency <- cursor.downField("currency").as[String]
    } yield Money(cents, currency)

  private implicit val orderLineDecoder: Decoder[OrderLine] = (cursor: HCursor) =>
    for {
      sku       <- cursor.downField("sku").as[String]
      quantity  <- cursor.downField("quantity").as[Int]
      unitPrice <- cursor.downField("unitPrice").as[Money]
    } yield OrderLine(Sku(sku), quantity, unitPrice)

  private implicit val orderDecoder: Decoder[Order] = (cursor: HCursor) =>
    for {
      id         <- cursor.downField("id").as[String]
      customerId <- cursor.downField("customerId").as[String]
      lines      <- cursor.downField("lines").as[List[OrderLine]]
      placedAt   <- cursor.downField("placedAt").as[Long]
      country    <- cursor.downField("country").as[String]
    } yield Order(OrderId(id), CustomerId(customerId), lines, placedAt, country)

  /** Parses the bytes of one Kafka record. */
  def decode(bytes: Array[Byte]): Either[DecodeFailure, Order] = {
    val payload = new String(bytes, "UTF-8")
    io.circe.parser
      .decode[Order](payload)(orderDecoder)
      .left
      .map(error => DecodeFailure(error.getMessage, payload))
  }

  /** Re-uses the shared encoder so producers and consumers cannot drift apart. */
  def encodeOrder(order: Order): String = Codecs.order(order)

  /** One line of newline-delimited JSON, the format written to object storage. */
  def encodeBatch(batch: CustomerOrderBatch): String =
    de.common.json.Json.obj(
      "customerId"  -> Some(de.common.json.Json.string(batch.customerId.value)),
      "windowStart" -> Some(de.common.json.Json.num(batch.windowStartMillis)),
      "windowEnd"   -> Some(de.common.json.Json.num(batch.windowEndMillis)),
      "orderCount"  -> Some(de.common.json.Json.num(batch.orderCount)),
      "orderIds"    -> Some(de.common.json.Json.arr(batch.orderIds.map(id => de.common.json.Json.string(id.value)))),
      "totalCents"  -> Some(de.common.json.Json.num(batch.total.cents)),
      "currency"    -> Some(de.common.json.Json.string(batch.total.currency))
    )
}
