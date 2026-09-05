package de.spark.streaming

/**
 * Everything the job needs to know about the world around it.
 *
 * Keeping these values in one immutable case class - instead of reading environment variables wherever they happen to
 * be needed - means the streaming logic can be exercised with any configuration, and that a reader sees the complete
 * list of knobs in one place.
 *
 * @param bootstrapServers
 *   `host:port` of the Apache Kafka broker
 * @param ordersTopic
 *   topic the shop publishes orders to
 * @param startingOffsets
 *   where a *brand new* query starts reading: `"earliest"` replays the whole topic, `"latest"` picks up only records
 *   that arrive from now on. It is ignored once a checkpoint exists, because the checkpoint then decides.
 * @param ordersTablePath
 *   location of the Delta table holding one row per order
 * @param revenueTablePath
 *   location of the Delta table holding the windowed revenue aggregate
 * @param checkpointRoot
 *   directory under which each query stores its own checkpoint
 * @param windowDuration
 *   width of one revenue window, in Spark interval syntax
 * @param watermarkDelay
 *   how late an order may arrive and still be counted
 * @param triggerInterval
 *   how often a micro-batch is started
 * @param runFor
 *   how long the job runs before it shuts down cleanly; a demo that never stops is awkward to try out
 */
final case class JobConfig(
    bootstrapServers: String,
    ordersTopic: String,
    startingOffsets: String,
    ordersTablePath: String,
    revenueTablePath: String,
    checkpointRoot: String,
    windowDuration: String,
    watermarkDelay: String,
    triggerInterval: String,
    runFor: java.time.Duration
) {

  /** Each streaming query needs its own checkpoint directory; two queries must never share one. */
  def checkpointFor(queryName: String): String = s"$checkpointRoot/$queryName"
}

object JobConfig {

  val Default: JobConfig = JobConfig(
    bootstrapServers = "localhost:10892",
    ordersTopic = "orders",
    startingOffsets = "earliest",
    ordersTablePath = "out/08-spark-streaming/orders",
    revenueTablePath = "out/08-spark-streaming/revenue-by-window",
    checkpointRoot = "out/08-spark-streaming/checkpoints",
    windowDuration = "5 minutes",
    watermarkDelay = "10 minutes",
    triggerInterval = "5 seconds",
    runFor = java.time.Duration.ofMinutes(2)
  )

  /**
   * Overrides the defaults from environment variables, so the same build runs against the docker stack and against a
   * different broker without a recompile.
   */
  def fromEnvironment(env: Map[String, String]): JobConfig = {
    def read(name: String, fallback: String): String = env.getOrElse(name, fallback)

    Default.copy(
      bootstrapServers = read("KAFKA_BOOTSTRAP_SERVERS", Default.bootstrapServers),
      ordersTopic = read("ORDERS_TOPIC", Default.ordersTopic),
      startingOffsets = read("STARTING_OFFSETS", Default.startingOffsets),
      ordersTablePath = read("ORDERS_TABLE_PATH", Default.ordersTablePath),
      revenueTablePath = read("REVENUE_TABLE_PATH", Default.revenueTablePath),
      checkpointRoot = read("CHECKPOINT_ROOT", Default.checkpointRoot),
      runFor = java.time.Duration.ofSeconds(read("RUN_FOR_SECONDS", "120").toLong)
    )
  }
}
