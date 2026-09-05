package de.flink.s3sink.job

import de.flink.s3sink.core.JobConfig
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * The composition root: the one place that reads the environment, creates the Flink execution environment and starts
 * the job. Everything it calls is already configured or pure.
 */
object Main {

  def main(args: Array[String]): Unit = {
    val config = JobConfig.fromEnvironment(args)
    val env    = StreamExecutionEnvironment.getExecutionEnvironment

    println(s"Reading Order events from topic '${config.topic}' at ${config.bootstrapServers}")
    println(s"Writing customer batches to ${config.outputUri}")
    println(s"Window size ${config.windowSizeMillis} ms, checkpoint every ${config.checkpointIntervalMillis} ms")

    OrderBatchPipeline.build(env, config)
    env.execute("orders-to-object-storage")
    ()
  }
}
