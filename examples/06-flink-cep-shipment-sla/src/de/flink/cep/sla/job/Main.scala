package de.flink.cep.sla.job

import de.flink.cep.sla.core.JobConfig
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * The composition root: the one place that reads the environment, creates the Flink execution environment and starts
 * the job. Everything it calls is already configured or pure.
 */
object Main {

  def main(args: Array[String]): Unit = {
    val config = JobConfig.fromEnvironment(args)
    val env    = StreamExecutionEnvironment.getExecutionEnvironment

    println(s"Reading Shipment events from topic '${config.shipmentTopic}' at ${config.bootstrapServers}")
    println(s"Breach alerts go to '${config.breachTopic}', kept promises to '${config.completionTopic}'")
    println(
      s"Promise: dispatch within ${config.policy.dispatchWithinMillis} ms, " +
        s"deliver within ${config.policy.deliverWithinMillis} ms"
    )

    ShipmentSlaPipeline.build(env, config)
    env.execute("shipment-sla-monitoring")
    ()
  }
}
