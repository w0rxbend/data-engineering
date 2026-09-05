package de.kafkastreams.fraud

import java.nio.charset.StandardCharsets.UTF_8

import de.common.domain.{Order, Payment}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

/**
 * Custom Serdes for the domain model.
 *
 * A Serde ("serializer/deserializer") is the pair of functions Kafka Streams uses to turn a value into bytes before it
 * goes on a topic or into a state store, and back again on the way out. Kafka ships Serdes for primitives such as
 * `String` and `Long` only, so every domain type needs its own.
 */
final class JsonSerde[A](encode: A => String, decode: String => A) extends Serde[A] {

  override def serializer(): Serializer[A] = new Serializer[A] {
    override def serialize(topic: String, value: A): Array[Byte] =
      if (value == null) { null }
      else { encode(value).getBytes(UTF_8) }
  }

  override def deserializer(): Deserializer[A] = new Deserializer[A] {
    override def deserialize(topic: String, bytes: Array[Byte]): A =
      if (bytes == null) { null.asInstanceOf[A] }
      else { decode(new String(bytes, UTF_8)) }
  }
}

/** The concrete Serdes this example needs, one per message type. */
object JsonSerdes {
  val order: Serde[Order]               = new JsonSerde(EventJson.writeOrder, EventJson.readOrder)
  val payment: Serde[Payment]           = new JsonSerde(EventJson.writePayment, EventJson.readPayment)
  val paidOrder: Serde[PaidOrder]       = new JsonSerde(EventJson.writePaidOrder, EventJson.readPaidOrder)
  val declineTally: Serde[DeclineTally] = new JsonSerde(EventJson.writeDeclineTally, EventJson.readDeclineTally)
  val customerRisk: Serde[CustomerRisk] = new JsonSerde(EventJson.writeCustomerRisk, EventJson.readCustomerRisk)
  val fraudAlert: Serde[FraudAlert]     = new JsonSerde(EventJson.writeFraudAlert, EventJson.readFraudAlert)
}
