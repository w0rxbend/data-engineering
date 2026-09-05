package de.flink.cep.sla.job

import de.flink.cep.sla.core.{ShipmentJson, ShipmentRecords}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Turns the bytes of a Kafka record into the flat tuple the pattern matcher consumes.
 *
 * A record that cannot be parsed is logged and dropped rather than thrown. On a shared topic a single misbehaving
 * producer would otherwise crash the job, Flink would restart it from the last checkpoint, read the same bad record
 * again and crash again -- an endless restart loop.
 */
@SerialVersionUID(1L)
final class ShipmentDeserializationSchema extends DeserializationSchema[ShipmentRecords.Event] {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[ShipmentDeserializationSchema])

  override def deserialize(bytes: Array[Byte]): ShipmentRecords.Event =
    throw new UnsupportedOperationException(
      "records are emitted through the Collector overload so that unparsable records can be skipped"
    )

  override def deserialize(bytes: Array[Byte], out: Collector[ShipmentRecords.Event]): Unit =
    ShipmentJson.decode(bytes) match {
      case Right(shipment) => out.collect(ShipmentRecords.fromShipment(shipment))
      case Left(failure)   =>
        logger.warn("Skipping a Kafka record that is not a valid Shipment event: {}", failure.reason)
    }

  override def isEndOfStream(nextElement: ShipmentRecords.Event): Boolean = false

  override def getProducedType: TypeInformation[ShipmentRecords.Event] = ShipmentRecords.eventTypeInformation
}
