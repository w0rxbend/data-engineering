package de.kafka.eos

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.{InterruptException, WakeupException}
import ox.forever

import java.time.Duration
import scala.jdk.CollectionConverters.*

/**
 * Drives the transactional loop: poll, settle, repeat, until asked to stop.
 *
 * The interesting logic lives in `TransactionalSettlement`; this file only turns Kafka's Java `ConsumerRecords` into
 * the plain `ConsumedRecord` values that logic works with, and decides when the loop ends.
 */
object SettlementService {

  private val pollTimeout: Duration = Duration.ofMillis(500)

  /**
   * Runs until the surrounding Ox scope is cancelled (for example because you pressed Ctrl+C, which `OxApp` turns into
   * an interrupt of this fork).
   *
   * Kafka's Java client reports that interrupt as `InterruptException`, and a call to `consumer.wakeup()` from another
   * thread as `WakeupException`. Both mean "stop polling", so both end the loop normally and let the enclosing scope
   * close the clients.
   *
   * @param consumer
   *   subscribed to the input topic; owned by the caller
   * @param transaction
   *   the transaction the batches run inside
   * @param now
   *   supplies payment timestamps
   * @param report
   *   called once per batch, for logging or for tests
   */
  def runUntilCancelled(
      consumer: KafkaConsumer[String, String],
      transaction: SettlementTransaction,
      now: () => Long,
      report: BatchOutcome => Unit
  ): Unit =
    try
      forever {
        report(TransactionalSettlement.settleBatch(poll(consumer), now, transaction))
      }
    catch {
      case _: WakeupException | _: InterruptException => ()
    }

  private def poll(consumer: KafkaConsumer[String, String]): List[ConsumedRecord] =
    consumer
      .poll(pollTimeout)
      .asScala
      .iterator
      .map { record =>
        ConsumedRecord(
          source = SourceOffset(record.topic(), record.partition(), record.offset()),
          payload = record.value()
        )
      }
      .toList

  /** Subscribes a consumer to a single topic. */
  def subscribe(consumer: KafkaConsumer[String, String], topic: TopicName): Unit =
    consumer.subscribe(List(topic.value).asJava)

  /**
   * Renders a batch outcome as one line of human-readable output.
   *
   * An empty poll produces `None` rather than a line of text: an idle service polls several times a second, and saying
   * "nothing happened" that often would bury the lines that matter.
   */
  def summarise(outcome: BatchOutcome): Option[String] = outcome match {
    case BatchOutcome.Empty =>
      None
    case BatchOutcome.Committed(payments) =>
      val amounts = payments.map(payment => s"${payment.orderId.value}=${payment.amount}").mkString(", ")
      Some(s"committed ${payments.size} payment(s): $amounts")
    case BatchOutcome.Aborted(failures) =>
      Some(s"aborted the transaction, nothing charged: ${failures.map(_.describe).mkString("; ")}")
  }
}
