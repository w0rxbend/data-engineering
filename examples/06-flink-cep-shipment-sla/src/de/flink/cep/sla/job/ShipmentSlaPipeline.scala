package de.flink.cep.sla.job

import de.flink.cep.sla.core.{JobConfig, ShipmentRecords, ShipmentSlaPatterns, SlaPolicy}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.cep.CEP
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.time.Duration

/**
 * Wires the pipeline together: Apache Kafka in, pattern matching in the middle, alerts back out to Kafka.
 *
 * This is the only place that knows about both the domain and the framework. It contains no business decision of its
 * own -- every rule it applies comes from `JobConfig` or from the pure functions in `de.flink.cep.sla.core`.
 */
object ShipmentSlaPipeline {

  /** The two result streams of the matcher: promises kept, and promises broken. */
  final case class AlertStreams(
      completions: DataStream[ShipmentRecords.Alert],
      breaches: DataStream[ShipmentRecords.Alert]
  )

  def kafkaSource(config: JobConfig): KafkaSource[ShipmentRecords.Event] =
    KafkaSource
      .builder[ShipmentRecords.Event]()
      .setBootstrapServers(config.bootstrapServers)
      .setTopics(config.shipmentTopic)
      .setGroupId(config.consumerGroupId)
      // Always start at the beginning of the topic, so the example is repeatable:
      // re-submitting the job replays every shipment event rather than continuing
      // from wherever the consumer group happened to stop.
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new ShipmentDeserializationSchema)
      .build()

  /**
   * The watermark strategy tells Flink how far behind the newest event time the "everything older than this has
   * arrived" mark may lag.
   *
   * For CEP that mark is what makes a breach *happen*: a partial match is declared timed out exactly when the watermark
   * passes the end of its `within` window. A larger allowance tolerates more disorder in the topic -- a delivery scan
   * that overtakes the dispatch scan, say -- at the price of alerting a little later.
   */
  def watermarkStrategy(config: JobConfig): WatermarkStrategy[ShipmentRecords.Event] =
    WatermarkStrategy
      .forBoundedOutOfOrderness[ShipmentRecords.Event](Duration.ofMillis(config.maxOutOfOrdernessMillis))
      .withTimestampAssigner(new ShipmentEventTimeAssigner)
      // Without this, a topic partition that receives no traffic would hold the
      // watermark back for every partition and no breach would ever be reported.
      .withIdleness(Duration.ofMinutes(1))

  /**
   * Runs both SLA patterns over a stream of shipment milestones.
   *
   * The stream is keyed by order first, so each order gets its own independent state machine, and the two patterns then
   * run side by side over the same keyed stream.
   */
  def detect(events: DataStream[ShipmentRecords.Event], policy: SlaPolicy): AlertStreams = {
    val byOrder = events.keyBy(new OrderKeySelector, Types.STRING)

    val dispatchMatches = CEP
      .pattern(byOrder, ShipmentSlaPatterns.dispatchPattern(policy))
      .inEventTime()
      .process(new DispatchSlaFunction(policy), ShipmentRecords.alertTypeInformation)
      .name("dispatch-sla-matcher")
      .uid("dispatch-sla-matcher")

    val deliveryMatches = CEP
      .pattern(byOrder, ShipmentSlaPatterns.deliveryPattern(policy))
      .inEventTime()
      .process(new DeliverySlaFunction(policy), ShipmentRecords.alertTypeInformation)
      .name("delivery-sla-matcher")
      .uid("delivery-sla-matcher")

    AlertStreams(
      completions = dispatchMatches.union(deliveryMatches),
      breaches = dispatchMatches
        .getSideOutput(SlaOutputTags.breaches)
        .union(deliveryMatches.getSideOutput(SlaOutputTags.breaches))
    )
  }

  def build(env: StreamExecutionEnvironment, config: JobConfig): Unit = {
    env.enableCheckpointing(config.checkpointIntervalMillis)

    val events = env
      .fromSource(kafkaSource(config), watermarkStrategy(config), "shipments-kafka-source")
      .uid("shipments-kafka-source")

    val alerts = detect(events, config.policy)

    alerts.breaches
      .sinkTo(AlertKafkaSink(config.bootstrapServers, config.breachTopic))
      .name("sla-breach-kafka-sink")
      .uid("sla-breach-kafka-sink")

    alerts.completions
      .sinkTo(AlertKafkaSink(config.bootstrapServers, config.completionTopic))
      .name("sla-completion-kafka-sink")
      .uid("sla-completion-kafka-sink")
    ()
  }
}
