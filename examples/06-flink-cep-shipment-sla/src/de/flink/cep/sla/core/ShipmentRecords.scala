package de.flink.cep.sla.core

import de.common.domain.{OrderId, Shipment, ShipmentStatus}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.tuple.{Tuple2 => FlinkTuple2, Tuple3 => FlinkTuple3}

/**
 * The shapes that travel between Apache Flink operators.
 *
 * Flink has to serialise every record that crosses an operator boundary. It ships fast, purpose-built serialisers for
 * its own `Tuple` types and for Java primitives, but it falls back to the general-purpose Kryo library for Scala case
 * classes that contain Scala collections or `sealed trait`s -- which is slow and, on some Scala versions, outright
 * fragile.
 *
 * The domain model therefore stays pure Scala inside the operators, and only these flat tuples cross the wire.
 * `toShipment` and `fromShipment` are the two places where the two worlds meet.
 */
object ShipmentRecords {

  /** `(orderId, statusName, occurredAtEpochMillis)`: one shipment milestone flowing through the pattern matcher. */
  type Event = FlinkTuple3[String, String, java.lang.Long]

  /** `(orderId, alertJson)`: one finished statement about an order, on its way to Apache Kafka. */
  type Alert = FlinkTuple2[String, String]

  def event(orderId: String, statusName: String, occurredAtEpochMillis: Long): Event =
    FlinkTuple3.of(orderId, statusName, java.lang.Long.valueOf(occurredAtEpochMillis))

  def fromShipment(shipment: Shipment): Event =
    event(shipment.orderId.value, shipment.status.toString, shipment.occurredAtEpochMillis)

  /**
   * Turns a record back into the domain model. The status name was validated when the record was decoded from Kafka, so
   * an unknown one here means a programming error rather than bad input, and failing loudly is the right answer.
   */
  def toShipment(record: Event): Shipment =
    Shipment(
      orderId = OrderId(orderIdOf(record)),
      status = ShipmentStatus
        .fromString(statusOf(record))
        .getOrElse(throw new IllegalArgumentException(s"unknown shipment status '${statusOf(record)}'")),
      occurredAtEpochMillis = eventTimeOf(record)
    )

  def alert(orderId: String, alertJson: String): Alert = FlinkTuple2.of(orderId, alertJson)

  def alertOf(value: SlaAlert): Alert = alert(value.orderId.value, ShipmentJson.encodeAlert(value))

  /** Flink cannot infer a type through a Scala type alias, so both are named explicitly. */
  val eventTypeInformation: TypeInformation[Event] =
    Types.TUPLE[Event](Types.STRING, Types.STRING, Types.LONG)

  val alertTypeInformation: TypeInformation[Alert] =
    Types.TUPLE[Alert](Types.STRING, Types.STRING)

  def orderIdOf(record: Event): String   = record.f0
  def statusOf(record: Event): String    = record.f1
  def eventTimeOf(record: Event): Long   = record.f2.longValue()
  def alertJsonOf(record: Alert): String = record.f1
}
