package de.kafka.eos

import de.common.domain.Payment
import de.common.json.Codecs

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters.*

/**
 * The real Kafka implementation of `SettlementTransaction`.
 *
 * It is the only file in the example that knows the transaction API, and it contains no business logic at all - exactly
 * the split that lets the loop be tested without a broker.
 */
final class KafkaSettlementTransaction(
    producer: KafkaProducer[String, String],
    consumer: KafkaConsumer[String, String],
    paymentsTopic: TopicName
) extends SettlementTransaction {

  def begin(): Unit = producer.beginTransaction()

  /**
   * The order id is used as the record key, so all payments for one order land on the same partition and therefore stay
   * in order relative to each other.
   */
  def emit(payment: Payment): Unit = {
    val record = new ProducerRecord[String, String](
      paymentsTopic.value,
      payment.orderId.value,
      Codecs.payment(payment)
    )
    producer.send(record)
    ()
  }

  /**
   * Commits output records and input progress in one atomic step.
   *
   * `sendOffsetsToTransaction` hands the consumed offsets to the producer, which writes them to Kafka's internal
   * offsets topic *inside* the same transaction as the payment records. `commitTransaction` then makes both visible
   * together. The consumer group metadata is passed along so the broker can reject the commit if this consumer has
   * meanwhile been kicked out of the group - a stale member must not be allowed to commit.
   */
  def commit(consumed: List[SourceOffset]): Unit = {
    producer.sendOffsetsToTransaction(nextOffsetsPerPartition(consumed).asJava, consumer.groupMetadata())
    producer.commitTransaction()
  }

  def abort(): Unit = producer.abortTransaction()

  def rewindTo(consumed: List[SourceOffset]): Unit =
    lowestOffsetPerPartition(consumed).foreach { case (partition, offset) => consumer.seek(partition, offset) }

  private def lowestOffsetPerPartition(consumed: List[SourceOffset]): Map[TopicPartition, Long] =
    consumed
      .groupBy(source => new TopicPartition(source.topic, source.partition))
      .view
      .mapValues(sources => sources.map(_.offset).min)
      .toMap

  /**
   * Kafka stores "the offset to read next", so the committed value is the highest offset seen in the batch plus one,
   * per topic-partition.
   */
  private def nextOffsetsPerPartition(consumed: List[SourceOffset]): Map[TopicPartition, OffsetAndMetadata] =
    consumed
      .groupBy(source => new TopicPartition(source.topic, source.partition))
      .view
      .mapValues(sources => new OffsetAndMetadata(sources.map(_.offset).max + 1L))
      .toMap
}
