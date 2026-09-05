package de.flink.cep.sla.job

import de.common.domain.Shipment
import de.common.gen.DataGenerator
import de.flink.cep.sla.core.{JobConfig, ShipmentJson, ShipmentTimeline}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

/**
 * Fills the shipment topic so the example has something to match. It is not part of the Flink job.
 *
 * Events are generated with a fixed seed and the faults are arithmetic, so two runs produce the same alerts.
 *
 * Milestones of all orders are published in event-time order, exactly as a real carrier feed would arrive: the
 * `Delivered` scan of an early order lands between the `Created` scans of much later ones. That interleaving is what
 * lets the job report a breach at all -- the deadline of an early order only passes once later events push the
 * watermark past it.
 */
object ShipmentProducer {

  private val DefaultOrderCount = 200
  private val DefaultSpeedup    = 20000L

  def main(args: Array[String]): Unit = {
    val arguments = JobConfig.parseArguments(args.toSeq)
    val config    = JobConfig.from(arguments, sys.env)
    val settings  = sys.env ++ arguments

    val orderCount = settings.get("ORDER_COUNT").map(_.trim.toInt).getOrElse(DefaultOrderCount)
    val speedup    = settings.get("EVENT_TIME_SPEEDUP").map(_.trim.toLong).getOrElse(DefaultSpeedup)

    val milestones = timeline(orderCount, speedup)
    publish(milestones, config)

    val counts = milestones.groupBy(_.status.toString).map { case (status, events) => s"$status=${events.size}" }
    println(
      s"Published ${milestones.size} shipment events (${counts.toList.sorted.mkString(", ")}) " +
        s"to topic '${config.shipmentTopic}' at ${config.bootstrapServers}"
    )
  }

  /** The full batch of milestones, faults applied, sorted by event time. */
  def timeline(orderCount: Int, speedup: Long): List[Shipment] = {
    val generator = new DataGenerator(seed = 42L)
    val origin    = generator.nextOrder().placedAtEpochMillis
    val faults    = ShipmentTimeline.Faults.default

    val perOrder = (1 to orderCount).toList.map { index =>
      val order = ShipmentTimeline.stretch(generator.nextOrder(), origin, speedup)
      ShipmentTimeline.milestonesFor(index, generator.shipmentsFor(order), faults)
    }

    perOrder.flatten.sortBy(_.occurredAtEpochMillis)
  }

  private def publish(milestones: List[Shipment], config: JobConfig): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", config.bootstrapServers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("acks", "all")

    val producer = new KafkaProducer[String, String](properties)
    try {
      milestones.foreach { shipment =>
        producer.send(
          new ProducerRecord(config.shipmentTopic, shipment.orderId.value, ShipmentJson.encodeShipment(shipment))
        )
      }
      producer.flush()
    } finally producer.close()
  }
}
