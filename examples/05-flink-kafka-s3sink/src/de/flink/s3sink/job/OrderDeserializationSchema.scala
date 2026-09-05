package de.flink.s3sink.job

import de.flink.s3sink.core.OrderJson
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Turns the bytes of a Kafka record into the flat tuple the pipeline uses.
 *
 * A record that cannot be parsed is logged and dropped rather than thrown. On a shared topic a single misbehaving
 * producer would otherwise crash the job, Flink would restart it from the last checkpoint, read the same bad record
 * again and crash again -- an endless restart loop.
 */
@SerialVersionUID(1L)
final class OrderDeserializationSchema extends DeserializationSchema[OrderRecords.Incoming] {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[OrderDeserializationSchema])

  override def deserialize(bytes: Array[Byte]): OrderRecords.Incoming =
    throw new UnsupportedOperationException(
      "records are emitted through the Collector overload so that unparsable records can be skipped"
    )

  override def deserialize(bytes: Array[Byte], out: Collector[OrderRecords.Incoming]): Unit =
    OrderJson.decode(bytes) match {
      case Right(order) =>
        out.collect(
          OrderRecords.incoming(order.customerId.value, order.placedAtEpochMillis, OrderJson.encodeOrder(order))
        )
      case Left(failure) =>
        logger.warn("Skipping a Kafka record that is not a valid Order event: {}", failure.reason)
    }

  override def isEndOfStream(nextElement: OrderRecords.Incoming): Boolean = false

  override def getProducedType: TypeInformation[OrderRecords.Incoming] =
    OrderRecords.incomingTypeInformation
}
