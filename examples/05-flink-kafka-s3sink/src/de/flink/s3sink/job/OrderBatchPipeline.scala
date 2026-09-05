package de.flink.s3sink.job

import de.flink.s3sink.core.JobConfig
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.time.Duration

/**
 * Wires the pipeline together: Kafka in, windowing in the middle, files out.
 *
 * This is the only place that knows about both the domain and the framework. It is kept free of decisions -- every rule
 * it applies comes either from `JobConfig` or from the pure functions in `de.flink.s3sink.core`.
 */
object OrderBatchPipeline {

  def kafkaSource(config: JobConfig): KafkaSource[OrderRecords.Incoming] =
    KafkaSource
      .builder[OrderRecords.Incoming]()
      .setBootstrapServers(config.bootstrapServers)
      .setTopics(config.topic)
      .setGroupId(config.consumerGroupId)
      // Always start at the beginning of the topic, so the example is repeatable:
      // re-submitting the job replays every order rather than continuing from
      // wherever the consumer group happened to stop. A production job would use
      // `OffsetsInitializer.committedOffsets(...)` to resume instead. Note that
      // Flink's own checkpoints, not the committed group offsets, are what make a
      // restart after a failure exactly-once.
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new OrderDeserializationSchema)
      .build()

  /**
   * The watermark strategy tells Flink how far behind the newest event time the "definitely complete" mark may lag.
   * Anything later than that is considered a late arrival. A larger allowance tolerates more disorder in the topic at
   * the cost of holding windows open longer.
   */
  def watermarkStrategy(config: JobConfig): WatermarkStrategy[OrderRecords.Incoming] =
    WatermarkStrategy
      .forBoundedOutOfOrderness[OrderRecords.Incoming](Duration.ofMillis(config.maxOutOfOrdernessMillis))
      .withTimestampAssigner(new OrderEventTimeAssigner)
      // Without this, a topic partition that receives no traffic would hold the
      // watermark back for every partition and no window would ever close.
      .withIdleness(Duration.ofMinutes(1))

  def build(env: StreamExecutionEnvironment, config: JobConfig): Unit = {
    env.enableCheckpointing(config.checkpointIntervalMillis)

    env
      .fromSource(kafkaSource(config), watermarkStrategy(config), "orders-kafka-source")
      .uid("orders-kafka-source")
      .keyBy(new CustomerKeySelector, Types.STRING)
      .process(new CustomerBatchProcessFunction(config.windowSizeMillis), OrderRecords.outgoingTypeInformation)
      .name("customer-order-window")
      .uid("customer-order-window")
      .sinkTo(BatchFileSink(config.outputUri))
      .name("customer-batch-file-sink")
      .uid("customer-batch-file-sink")
    ()
  }
}
