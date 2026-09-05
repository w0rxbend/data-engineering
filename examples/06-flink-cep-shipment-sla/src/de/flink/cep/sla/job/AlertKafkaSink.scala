package de.flink.cep.sla.job

import de.flink.cep.sla.core.ShipmentRecords
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.kafka.clients.producer.ProducerRecord

import java.nio.charset.StandardCharsets

/**
 * Writes alert records to an Apache Kafka topic.
 *
 * The order identifier becomes the Kafka message key. Kafka routes all messages with the same key to the same
 * partition, so every alert about one order stays in order, and a compacted downstream view would keep the newest
 * statement per order.
 */
@SerialVersionUID(1L)
final class AlertRecordSerializationSchema(topic: String)
    extends KafkaRecordSerializationSchema[ShipmentRecords.Alert] {

  override def serialize(
      alert: ShipmentRecords.Alert,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: java.lang.Long
  ): ProducerRecord[Array[Byte], Array[Byte]] =
    new ProducerRecord(
      topic,
      alert.f0.getBytes(StandardCharsets.UTF_8),
      alert.f1.getBytes(StandardCharsets.UTF_8)
    )
}

object AlertKafkaSink {

  /**
   * At-least-once delivery is the right trade-off for alerts: a duplicate alert about a late parcel is harmless, a
   * missing one is not. Exactly-once would need Kafka transactions and would hold alerts back until the next checkpoint
   * completes, which is the opposite of what an alerting path wants.
   */
  def apply(bootstrapServers: String, topic: String): KafkaSink[ShipmentRecords.Alert] =
    KafkaSink
      .builder[ShipmentRecords.Alert]()
      .setBootstrapServers(bootstrapServers)
      .setRecordSerializer(new AlertRecordSerializationSchema(topic))
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
}
