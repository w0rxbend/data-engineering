package de.flink.s3sink.job

import de.common.gen.DataGenerator
import de.flink.s3sink.core.{JobConfig, OrderJson}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

/**
 * A tiny helper that fills the Kafka topic with `Order` events so the example has something to process. It is not part
 * of the Flink job.
 *
 * Events are generated with a fixed seed, so two runs produce the same orders and the files in object storage can be
 * compared between runs.
 *
 * The generator hands out timestamps roughly half a second apart. To make one hour of event time pass quickly,
 * `EVENT_TIME_SPEEDUP` multiplies the distance between consecutive events without touching the wall clock.
 */
object OrderProducer {

  private val DefaultOrderCount = 500
  private val DefaultSpeedup    = 12000L

  def main(args: Array[String]): Unit = {
    val arguments = JobConfig.parseArguments(args.toSeq)
    val config    = JobConfig.from(arguments, sys.env)
    val settings  = sys.env ++ arguments

    val orderCount = settings.get("ORDER_COUNT").map(_.trim.toInt).getOrElse(DefaultOrderCount)
    val speedup    = settings.get("EVENT_TIME_SPEEDUP").map(_.trim.toLong).getOrElse(DefaultSpeedup)

    val properties = new Properties()
    properties.put("bootstrap.servers", config.bootstrapServers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("acks", "all")

    val producer = new KafkaProducer[String, String](properties)
    try {
      val generator = new DataGenerator(seed = 42L)
      val origin    = generator.nextOrder().placedAtEpochMillis

      (1 to orderCount).foreach { _ =>
        val generated = generator.nextOrder()
        // Stretch the generated timeline so a handful of event-time windows
        // are covered by a few hundred records.
        val order = generated.copy(
          placedAtEpochMillis = origin + (generated.placedAtEpochMillis - origin) * speedup
        )
        producer.send(new ProducerRecord(config.topic, order.customerId.value, OrderJson.encodeOrder(order)))
      }
      producer.flush()
      println(s"Published $orderCount Order events to topic '${config.topic}' at ${config.bootstrapServers}")
    } finally producer.close()
  }
}
