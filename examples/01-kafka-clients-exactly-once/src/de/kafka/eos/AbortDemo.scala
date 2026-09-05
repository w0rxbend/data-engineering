package de.kafka.eos

import de.common.domain.{Money, OrderId, Payment, PaymentStatus}
import de.common.json.Codecs

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import ox.{resourceScope, useCloseableInScope}

import java.time.Duration
import scala.jdk.CollectionConverters.*

/** How many records each isolation level could see after an aborted transaction. */
final case class AbortVisibility(readUncommitted: Int, readCommitted: Int) {

  def describe: String =
    s"a read_uncommitted consumer sees $readUncommitted record(s); " +
      s"a read_committed consumer sees $readCommitted record(s)"
}

/**
 * Shows, end to end, that an aborted transaction is invisible to a consumer reading with
 * `isolation.level=read_committed`.
 *
 * The demonstration writes one deliberately bogus payment inside a transaction, calls `abortTransaction`, and then
 * reads the tail of the payments topic twice: once with each isolation level. The record is physically on the log
 * either way - aborting does not erase anything - but the broker refuses to hand it to a `read_committed` consumer.
 */
object AbortDemo {

  private val pollTimeout: Duration = Duration.ofMillis(500)

  /** Two empty polls in a row is this example's definition of "caught up". */
  private val emptyPollsMeaningEndOfTopic = 2

  private val demoGroupPrefix = "abort-demo"

  def run(bootstrapServers: BootstrapServers, paymentsTopic: TopicName): AbortVisibility = {
    val tailBeforeAbort = endOffsets(bootstrapServers, paymentsTopic)
    writeAndAbort(bootstrapServers, paymentsTopic)
    AbortVisibility(
      readUncommitted = countFrom(bootstrapServers, paymentsTopic, tailBeforeAbort, IsolationLevel.ReadUncommitted),
      readCommitted = countFrom(bootstrapServers, paymentsTopic, tailBeforeAbort, IsolationLevel.ReadCommitted)
    )
  }

  /** Opens a transaction, sends one payment, then throws the transaction away. */
  private def writeAndAbort(bootstrapServers: BootstrapServers, paymentsTopic: TopicName): Unit = resourceScope {
    val producer = useCloseableInScope(
      KafkaClients.transactionalProducer(bootstrapServers, Settings.abortDemoTransactionalId)
    )
    // `initTransactions` must be called exactly once per producer, before any
    // transaction. It registers the transactional id with the broker and
    // fences out any older producer holding the same id.
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(
      new ProducerRecord(paymentsTopic.value, abandonedPayment.orderId.value, Codecs.payment(abandonedPayment))
    )
    producer.flush()
    producer.abortTransaction()
  }

  /** A payment that must never be charged; if you can see it, something is wrong. */
  private val abandonedPayment: Payment =
    Payment(
      orderId = OrderId("order-never-charged"),
      amount = Money.eur(999999L),
      status = PaymentStatus.Declined,
      occurredAtEpochMillis = 0L
    )

  /** Where the topic ends right now, per partition. */
  private def endOffsets(
      bootstrapServers: BootstrapServers,
      topic: TopicName
  ): Map[TopicPartition, Long] = resourceScope {
    val consumer = useCloseableInScope(newConsumer(bootstrapServers, IsolationLevel.ReadUncommitted))
    val assigned = assignAllPartitions(consumer, topic)
    consumer.endOffsets(assigned.asJava).asScala.view.mapValues(_.longValue).toMap
  }

  /** Counts every record from the given starting positions to the end of the topic. */
  private def countFrom(
      bootstrapServers: BootstrapServers,
      topic: TopicName,
      startOffsets: Map[TopicPartition, Long],
      isolationLevel: IsolationLevel
  ): Int = resourceScope {
    val consumer = useCloseableInScope(newConsumer(bootstrapServers, isolationLevel))
    assignAllPartitions(consumer, topic).foreach { partition =>
      consumer.seek(partition, startOffsets.getOrElse(partition, 0L))
    }
    drain(consumer)
  }

  private def drain(consumer: KafkaConsumer[String, String]): Int = {
    var seen       = 0
    var emptyPolls = 0
    while (emptyPolls < emptyPollsMeaningEndOfTopic) {
      val batch = consumer.poll(pollTimeout).count()
      seen += batch
      emptyPolls = if (batch == 0) emptyPolls + 1 else 0
    }
    seen
  }

  private def assignAllPartitions(
      consumer: KafkaConsumer[String, String],
      topic: TopicName
  ): List[TopicPartition] = {
    val partitions = consumer
      .partitionsFor(topic.value)
      .asScala
      .toList
      .map(info => new TopicPartition(info.topic(), info.partition()))
    consumer.assign(partitions.asJava)
    partitions
  }

  /**
   * Each call gets its own consumer group id so the demonstration never disturbs, or is disturbed by, the settlement
   * service's committed offsets.
   */
  private def newConsumer(
      bootstrapServers: BootstrapServers,
      isolationLevel: IsolationLevel
  ): KafkaConsumer[String, String] =
    KafkaClients.consumer(
      bootstrapServers,
      ConsumerGroupId(s"$demoGroupPrefix-${isolationLevel.configValue}"),
      isolationLevel
    )
}
