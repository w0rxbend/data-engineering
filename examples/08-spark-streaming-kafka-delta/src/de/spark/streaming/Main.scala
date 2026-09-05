package de.spark.streaming

/**
 * Entry point of example 08.
 *
 * Two commands:
 *   - `produce [count]` fills the Kafka topic with generated orders;
 *   - `stream` (the default) runs the Structured Streaming job until its configured time budget is used up.
 *
 * Running the two as separate commands rather than one program keeps the example honest: the streaming job has no idea
 * where its input comes from, and you can restart it while the producer keeps running to watch it resume from its
 * checkpoint.
 */
object Main {

  def main(args: Array[String]): Unit = {
    val config = JobConfig.fromEnvironment(sys.env)
    args.toList match {
      case "produce" :: rest   => produce(config, rest.headOption.map(_.toInt).getOrElse(500))
      case Nil | "stream" :: _ => stream(config)
      case other :: _          =>
        Console.err.println(s"unknown command '$other'; expected 'produce' or 'stream'")
        sys.exit(2)
    }
  }

  private def produce(config: JobConfig, count: Int): Unit =
    OrderPublisher.publish(
      bootstrapServers = config.bootstrapServers,
      topic = config.ordersTopic,
      count = count,
      pauseMillis = 5L,
      log = println
    )

  private def stream(config: JobConfig): Unit = {
    val spark = SparkSessions.local("de-08-spark-streaming-kafka-delta")
    spark.sparkContext.setLogLevel("WARN")
    configureObjectStorageIfRequested(spark)
    spark.streams.addListener(new ProgressReporter(println))

    println(s"reading topic '${config.ordersTopic}' from ${config.bootstrapServers}")
    println(s"orders table  -> ${config.ordersTablePath}")
    println(s"revenue table -> ${config.revenueTablePath}")

    val queries = StreamingJob.start(spark, config)
    try {
      // A demo has to end on its own. A production job would call
      // spark.streams.awaitAnyTermination() and run until it is stopped.
      queries.foreach(_.awaitTermination(config.runFor.toMillis))
      queries.foreach(query => println(s"final progress: ${Option(query.lastProgress).fold("none")(_.toString)}"))
    } finally {
      queries.foreach(_.stop())
      spark.stop()
    }

    println("stopped; run the same command again and both queries resume from their checkpoints")
  }

  /**
   * Reads the MinIO credentials from the environment. They are only needed when the table paths point at `s3a://`; a
   * run against local directories ignores them.
   */
  private def configureObjectStorageIfRequested(spark: org.apache.spark.sql.SparkSession): Unit =
    sys.env.get("S3_ENDPOINT").foreach { endpoint =>
      SparkSessions.configureS3(
        spark,
        endpoint = endpoint,
        accessKey = sys.env.getOrElse("S3_ACCESS_KEY", "minioadmin"),
        secretKey = sys.env.getOrElse("S3_SECRET_KEY", "minioadmin")
      )
      println(s"object storage endpoint: $endpoint")
    }
}
