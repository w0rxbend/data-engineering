package de.kafkastreams.fraud

import java.time.{Clock, Instant}
import java.util.Properties

import scala.util.Using

import de.common.domain.CustomerId
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/** One payment attempt together with the Kafka record timestamp used for both of its source records. */
final case class TimestampedAttempt(attempt: DemoScenario.Attempt, recordTimestampEpochMillis: Long) {
  def sourceKey: String = attempt.order.id.value

  /** The key used by the topology's decline repartition. */
  def customerKey: String = attempt.order.customerId.value
}

/** A finite attack followed by the record that closes all hopping windows containing it. */
final case class OneShotScenario(attack: List[TimestampedAttempt], streamTimeAdvance: TimestampedAttempt) {
  def records: List[TimestampedAttempt] = attack :+ streamTimeAdvance
}

/**
 * Builds the one-shot scenario without talking to Kafka.
 *
 * Kafka Streams tracks stream time per task. The closing record deliberately uses the suspect customer too, so the
 * topology repartitions it to the same task as the attack. Its later timestamp closes suppression without relying on
 * wall-clock sleeps.
 */
final class OneShotScenarioFactory(clock: Clock, topologyConfig: FraudTopology.Config) {

  def create(suspect: CustomerId, attempts: Int): OneShotScenario = {
    require(attempts > 0, s"attempts must be positive, got $attempts")

    val attackAt = clock.instant().toEpochMilli
    val attack   = DemoScenario
      .cardTestingBurst(suspect, attempts, attackAt, spacingMillis = 0L)
      .map(attempt => TimestampedAttempt(attempt, attackAt))

    val closeAt        = attackAt + topologyConfig.windowSize.toMillis + topologyConfig.grace.toMillis + 1L
    val closingAttempt = DemoScenario
      .cardTestingBurst(suspect, attempts = 1, startEpochMillis = closeAt, spacingMillis = 0L)
      .head

    OneShotScenario(attack, TimestampedAttempt(closingAttempt, closeAt))
  }
}

/**
 * Writes demo traffic into the `orders`, `payments` and `customer-risk` topics.
 *
 * Run this next to `Main`: it produces ordinary shop traffic and, every so often, a card-testing burst from one
 * customer, which is what makes an alert appear on the `fraud-alerts` topic a couple of minutes later.
 */
object SeedProducer {

  /** The customer whose card testing this demo keeps replaying. */
  private val suspect = CustomerId("cust-probe-01")

  /** How many ordinary attempts are produced between two bursts. */
  private val attemptsBetweenBursts = 20

  /** How many declined attempts one burst contains. */
  private val burstSize = 5

  private val pauseMillis = 500L

  def main(args: Array[String]): Unit = {
    val bootstrapServers = KafkaSettings.bootstrapServers
    val oneShotFactory   = new OneShotScenarioFactory(Clock.systemUTC(), FraudTopology.defaultConfig)
    Using.resource(new KafkaProducer[String, String](producerProperties(bootstrapServers))) { producer =>
      publishRiskProfiles(producer)
      producer.flush()
      args.toList match {
        case "once" :: Nil => publishOneShotScenario(producer, oneShotFactory.create(suspect, burstSize))
        case Nil           => publishLiveTraffic(producer, 400)
        case raw :: Nil    =>
          raw.toIntOption.filter(_ > 0) match {
            case Some(rounds) => publishLiveTraffic(producer, rounds)
            case None         => throw new IllegalArgumentException("usage: SeedProducer [once | positive-round-count]")
          }
        case _ => throw new IllegalArgumentException("usage: SeedProducer [once | positive-round-count]")
      }
      producer.flush()
    }
    println("Done.")
  }

  /**
   * Publishes one complete, deterministic window without sleeping.
   *
   * Suppression releases results only after stream time has passed the end of the window. The final one-decline record
   * uses the same customer key, so it reaches the same repartition task, and is far enough beyond the attack to close
   * every window containing it. It cannot trigger a new alert of its own because it is alone in its later windows.
   */
  private[fraud] def publishOneShotScenario(
      producer: Producer[String, String],
      scenario: OneShotScenario
  ): Unit = {
    // One source task must emit the whole scenario in order. Distinct source partitions
    // could forward the clock-advance record before another task forwards its attack.
    scenario.attack.foreach(record => publish(producer, record.attempt, record.recordTimestampEpochMillis, Some(0)))
    producer.flush()
    val advance = scenario.streamTimeAdvance
    publish(producer, advance.attempt, advance.recordTimestampEpochMillis, Some(0))
    val closeAt = Instant.ofEpochMilli(scenario.streamTimeAdvance.recordTimestampEpochMillis)
    println(s"Published one ${scenario.attack.size}-decline card-testing burst and advanced stream time to $closeAt")
  }

  private def publishLiveTraffic(producer: KafkaProducer[String, String], rounds: Int): Unit = {
    println(s"Producing $rounds rounds of live demo traffic")
    val background = DemoScenario.backgroundTraffic(rounds).iterator
    (1 to rounds).foreach { round =>
      // Every event is stamped with the wall clock, because the topology
      // windows on the record timestamp rather than on a field in the value.
      val now = System.currentTimeMillis()
      publish(producer, background.next(), now)
      if (round % attemptsBetweenBursts == 0) {
        println(s"Round $round: injecting a card-testing burst from ${suspect.value}")
        DemoScenario.cardTestingBurst(suspect, burstSize, now, spacingMillis = 0L).foreach { attempt =>
          publish(producer, attempt, now)
        }
      }
      Thread.sleep(pauseMillis)
    }
  }

  private def publishRiskProfiles(producer: KafkaProducer[String, String]): Unit =
    DemoScenario.riskProfiles(suspect).foreach { risk =>
      send(producer, FraudTopology.CustomerRiskTopic, risk.customerId.value, EventJson.writeCustomerRisk(risk))
    }

  /** Sends the order and its payment, both keyed by order id so they join. */
  private def publish(
      producer: Producer[String, String],
      attempt: DemoScenario.Attempt,
      at: Long,
      sourcePartition: Option[Int] = None
  ): Unit = {
    val key = attempt.order.id.value
    send(producer, FraudTopology.OrdersTopic, key, EventJson.writeOrder(attempt.order), at, sourcePartition)
    send(producer, FraudTopology.PaymentsTopic, key, EventJson.writePayment(attempt.payment), at, sourcePartition)
  }

  private def send(
      producer: Producer[String, String],
      topic: String,
      key: String,
      value: String,
      at: Long = System.currentTimeMillis(),
      sourcePartition: Option[Int] = None
  ): Unit =
    producer
      .send(
        new ProducerRecord(topic, sourcePartition.map(Int.box).orNull, java.lang.Long.valueOf(at), key, value)
      )
      .get()

  private def producerProperties(bootstrapServers: String): Properties = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties
  }
}
