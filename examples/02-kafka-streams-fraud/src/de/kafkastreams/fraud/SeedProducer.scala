package de.kafkastreams.fraud

import java.util.Properties

import scala.util.Using

import de.common.domain.CustomerId
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

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
    val rounds           = args.headOption.map(_.toInt).getOrElse(400)
    val bootstrapServers = KafkaSettings.bootstrapServers
    println(s"Producing $rounds rounds of demo traffic to $bootstrapServers")

    Using.resource(new KafkaProducer[String, String](producerProperties(bootstrapServers))) { producer =>
      DemoScenario.riskProfiles(suspect).foreach { risk =>
        send(producer, FraudTopology.CustomerRiskTopic, risk.customerId.value, EventJson.writeCustomerRisk(risk))
      }

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
      producer.flush()
    }
    println("Done.")
  }

  /** Sends the order and its payment, both keyed by order id so they join. */
  private def publish(producer: KafkaProducer[String, String], attempt: DemoScenario.Attempt, at: Long): Unit = {
    val key = attempt.order.id.value
    send(producer, FraudTopology.OrdersTopic, key, EventJson.writeOrder(attempt.order), at)
    send(producer, FraudTopology.PaymentsTopic, key, EventJson.writePayment(attempt.payment), at)
  }

  private def send(
      producer: KafkaProducer[String, String],
      topic: String,
      key: String,
      value: String,
      at: Long = System.currentTimeMillis()
  ): Unit =
    producer.send(new ProducerRecord(topic, null, java.lang.Long.valueOf(at), key, value))

  private def producerProperties(bootstrapServers: String): Properties = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties
  }
}
