package de.flink.cep.sla.core

import de.common.domain.{OrderId, Shipment, ShipmentStatus}
import de.common.json.{Codecs, Json}
import io.circe.{Decoder, HCursor}

/**
 * Reads the shared `Shipment` wire format produced by `de.common.json.Codecs`, and writes an `SlaAlert` back out as one
 * JSON object.
 *
 * Decoding never throws. A malformed record is a routine event on a shared Apache Kafka topic -- one misbehaving
 * producer must not take the whole job down -- so a failure is returned as a value and the caller decides what to do
 * with it.
 */
object ShipmentJson {

  /** Why a Kafka record could not be turned into a `Shipment`. */
  final case class DecodeFailure(reason: String, payload: String)

  private implicit val shipmentDecoder: Decoder[Shipment] = (cursor: HCursor) =>
    for {
      orderId    <- cursor.downField("orderId").as[String]
      rawStatus  <- cursor.downField("status").as[String]
      occurredAt <- cursor.downField("occurredAt").as[Long]
      status     <- ShipmentStatus
        .fromString(rawStatus)
        .toRight(io.circe.DecodingFailure(s"unknown shipment status '$rawStatus'", cursor.history))
    } yield Shipment(OrderId(orderId), status, occurredAt)

  /** Parses the bytes of one Kafka record. */
  def decode(bytes: Array[Byte]): Either[DecodeFailure, Shipment] = {
    val payload = new String(bytes, "UTF-8")
    io.circe.parser
      .decode[Shipment](payload)(shipmentDecoder)
      .left
      .map(error => DecodeFailure(error.getMessage, payload))
  }

  /** Re-uses the shared encoder so producers and consumers cannot drift apart. */
  def encodeShipment(shipment: Shipment): String = Codecs.shipment(shipment)

  /** The alert as it is published to the alert topic. */
  def encodeAlert(alert: SlaAlert): String =
    Json.obj(
      "orderId"        -> Some(Json.string(alert.orderId.value)),
      "outcome"        -> Some(Json.string(alert.outcome.name)),
      "breach"         -> Some(if (alert.outcome.isBreach) "true" else "false"),
      "lastStatus"     -> Some(Json.string(alert.lastObservedStatus.toString)),
      "lastObservedAt" -> Some(Json.num(alert.lastObservedAtEpochMillis)),
      "evaluatedAt"    -> Some(Json.num(alert.evaluatedAtEpochMillis)),
      "deadline"       -> Some(Json.num(alert.deadlineEpochMillis)),
      "latenessMs"     -> Some(Json.num(alert.latenessMillis)),
      "message"        -> Some(Json.string(alert.message))
    )
}
