package de.flink.s3sink.core

/**
 * Everything the job needs to know about its surroundings, in one value.
 *
 * The original version of this job hard-coded the broker address, the topic and the bucket. That made it impossible to
 * run the same jar against a local stack and against a real cluster. Settings are now resolved from, in order of
 * increasing priority: built-in defaults, environment variables, command-line arguments.
 *
 * @param bootstrapServers
 *   `host:port` list of Apache Kafka brokers
 * @param topic
 *   Kafka topic carrying `Order` events
 * @param consumerGroupId
 *   Kafka consumer group; Kafka stores the read position per group, so two jobs with the same group share the work
 *   instead of duplicating it
 * @param outputUri
 *   base location of the sink, for example `s3://orders/customer-batches`
 * @param checkpointIntervalMillis
 *   how often Flink snapshots its state
 * @param windowSizeMillis
 *   length of one event-time window
 * @param maxOutOfOrdernessMillis
 *   how late an event may be and still be counted
 */
final case class JobConfig(
    bootstrapServers: String,
    topic: String,
    consumerGroupId: String,
    outputUri: String,
    checkpointIntervalMillis: Long,
    windowSizeMillis: Long,
    maxOutOfOrdernessMillis: Long
)

object JobConfig {

  val default: JobConfig = JobConfig(
    bootstrapServers = "localhost:9092",
    topic = "orders",
    consumerGroupId = "flink-s3-sink",
    outputUri = "s3://orders/customer-batches",
    checkpointIntervalMillis = 15000L,
    windowSizeMillis = 3600000L,
    maxOutOfOrdernessMillis = 5000L
  )

  /** Environment variable name for each setting. */
  private val KafkaBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS"
  private val KafkaTopic            = "KAFKA_TOPIC"
  private val KafkaGroupId          = "KAFKA_GROUP_ID"
  private val OutputUri             = "OUTPUT_URI"
  private val CheckpointInterval    = "CHECKPOINT_INTERVAL_MS"
  private val WindowSize            = "WINDOW_SIZE_MS"
  private val MaxOutOfOrderness     = "MAX_OUT_OF_ORDERNESS_MS"

  /**
   * Turns `--kafka-topic orders --window-size-ms 60000` into
   * `Map("KAFKA_TOPIC" -> "orders", "WINDOW_SIZE_MS" -> "60000")`, so a command-line flag and an environment variable
   * name the same setting. Both `--flag value` and `--flag=value` are accepted.
   */
  def parseArguments(args: Seq[String]): Map[String, String] = {
    def normalise(flag: String): String = flag.stripPrefix("--").replace('-', '_').toUpperCase

    def loop(remaining: List[String], acc: Map[String, String]): Map[String, String] = remaining match {
      case Nil                                    => acc
      case flag :: rest if !flag.startsWith("--") => loop(rest, acc)
      case flag :: rest if flag.contains("=")     =>
        val (name, value) = flag.span(_ != '=')
        loop(rest, acc + (normalise(name) -> value.drop(1)))
      case flag :: value :: rest if !value.startsWith("--") => loop(rest, acc + (normalise(flag) -> value))
      case _ :: rest                                        => loop(rest, acc)
    }

    loop(args.toList, Map.empty)
  }

  /** Resolves the configuration; `arguments` win over `env`, `env` over defaults. */
  def from(arguments: Map[String, String], env: Map[String, String]): JobConfig = {
    val settings = env ++ arguments

    def string(name: String, fallback: String): String =
      settings.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(fallback)

    def positiveMillis(name: String, fallback: Long): Long =
      settings.get(name).map(_.trim).filter(_.nonEmpty) match {
        case None      => fallback
        case Some(raw) =>
          val parsed =
            try raw.toLong
            catch {
              case _: NumberFormatException =>
                throw new IllegalArgumentException(s"$name must be a whole number of milliseconds, but was '$raw'")
            }
          if (parsed <= 0L) throw new IllegalArgumentException(s"$name must be greater than zero, but was $parsed")
          parsed
      }

    JobConfig(
      bootstrapServers = string(KafkaBootstrapServers, default.bootstrapServers),
      topic = string(KafkaTopic, default.topic),
      consumerGroupId = string(KafkaGroupId, default.consumerGroupId),
      outputUri = string(OutputUri, default.outputUri),
      checkpointIntervalMillis = positiveMillis(CheckpointInterval, default.checkpointIntervalMillis),
      windowSizeMillis = positiveMillis(WindowSize, default.windowSizeMillis),
      maxOutOfOrdernessMillis = positiveMillis(MaxOutOfOrderness, default.maxOutOfOrdernessMillis)
    )
  }

  /** Convenience wrapper for `Main`, the only place that touches the real environment. */
  def fromEnvironment(args: Array[String]): JobConfig =
    from(parseArguments(args.toSeq), sys.env)
}
