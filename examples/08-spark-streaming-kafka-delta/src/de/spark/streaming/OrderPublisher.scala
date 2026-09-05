package de.spark.streaming

import de.common.gen.DataGenerator
import de.common.json.Codecs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.util.Using

/**
 * Fills the orders topic with the shared online-shop events so the streaming job has something to read.
 *
 * The generator is seeded, so every run publishes the identical sequence of orders. That is what makes the numbers in
 * the README reproducible on your machine.
 */
object OrderPublisher {

  /**
   * Publishes `count` orders, pausing `pauseMillis` between them so the stream looks like a trickle of live traffic
   * rather than one giant burst.
   *
   * The order id is used as the record key. Kafka routes all records with the same key to the same partition, which
   * guarantees that two versions of one order are never processed out of order by two different tasks.
   */
  def publish(
      bootstrapServers: String,
      topic: String,
      count: Int,
      pauseMillis: Long,
      log: String => Unit
  ): Unit = {
    val settings = new Properties()
    settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // "all" waits for every in-sync replica to store the record before the send
    // is reported successful. Anything weaker can lose records on broker failure.
    settings.put(ProducerConfig.ACKS_CONFIG, "all")

    val generator = new DataGenerator()

    Using.resource(new KafkaProducer[String, String](settings)) { producer =>
      (1 to count).foreach { index =>
        val order = generator.nextOrder()
        producer.send(new ProducerRecord(topic, order.id.value, Codecs.order(order)))
        if (index % 100 == 0) log(s"published $index of $count orders")
        if (pauseMillis > 0) Thread.sleep(pauseMillis)
      }
      producer.flush()
    }
    log(s"published $count orders to topic '$topic'")
  }
}
