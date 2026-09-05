package de.flink.cep.sla.core

/**
 * Everything the job needs to know about its surroundings, in one value.
 *
 * Settings are resolved from, in order of increasing priority: built-in defaults, environment variables, command-line
 * arguments. The same jar therefore runs against the local Docker stack and against a real cluster without a rebuild.
 *
 * @param bootstrapServers
 *   `host:port` list of Apache Kafka brokers
 * @param shipmentTopic
 *   Kafka topic carrying `Shipment` milestones
 * @param breachTopic
 *   Kafka topic the SLA breach alerts are written to
 * @param completionTopic
 *   Kafka topic the "promise kept" statements are written to
 * @param consumerGroupId
 *   Kafka consumer group; Kafka stores the read position per group, so two jobs with the same group share the work
 *   instead of duplicating it
 * @param policy
 *   the delivery promise being monitored
 * @param maxOutOfOrdernessMillis
 *   how far out of order shipment events may arrive and still be matched
 * @param checkpointIntervalMillis
 *   how often Flink snapshots the state of the pattern matcher
 */
final case class JobConfig(
    bootstrapServers: String,
    shipmentTopic: String,
    breachTopic: String,
    completionTopic: String,
    consumerGroupId: String,
    policy: SlaPolicy,
    maxOutOfOrdernessMillis: Long,
    checkpointIntervalMillis: Long
)

object JobConfig {

  val default: JobConfig = JobConfig(
    bootstrapServers = "localhost:9092",
    shipmentTopic = "shipments",
    breachTopic = "shipment-sla-breaches",
    completionTopic = "shipment-sla-completions",
    consumerGroupId = "flink-cep-shipment-sla",
    policy = SlaPolicy.default,
    maxOutOfOrdernessMillis = 5000L,
    checkpointIntervalMillis = 15000L
  )

  /** Environment variable name for each setting. */
  private val KafkaBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS"
  private val ShipmentTopic         = "SHIPMENT_TOPIC"
  private val BreachTopic           = "BREACH_TOPIC"
  private val CompletionTopic       = "COMPLETION_TOPIC"
  private val KafkaGroupId          = "KAFKA_GROUP_ID"
  private val DispatchWithin        = "DISPATCH_WITHIN_MS"
  private val DeliverWithin         = "DELIVER_WITHIN_MS"
  private val MaxOutOfOrderness     = "MAX_OUT_OF_ORDERNESS_MS"
  private val CheckpointInterval    = "CHECKPOINT_INTERVAL_MS"

  /**
   * Turns `--shipment-topic shipments --deliver-within-ms 60000` into
   * `Map("SHIPMENT_TOPIC" -> "shipments", "DELIVER_WITHIN_MS" -> "60000")`, so a command-line flag and an environment
   * variable name the same setting. Both `--flag value` and `--flag=value` are accepted.
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
      shipmentTopic = string(ShipmentTopic, default.shipmentTopic),
      breachTopic = string(BreachTopic, default.breachTopic),
      completionTopic = string(CompletionTopic, default.completionTopic),
      consumerGroupId = string(KafkaGroupId, default.consumerGroupId),
      policy = SlaPolicy(
        dispatchWithinMillis = positiveMillis(DispatchWithin, default.policy.dispatchWithinMillis),
        deliverWithinMillis = positiveMillis(DeliverWithin, default.policy.deliverWithinMillis)
      ),
      maxOutOfOrdernessMillis = positiveMillis(MaxOutOfOrderness, default.maxOutOfOrdernessMillis),
      checkpointIntervalMillis = positiveMillis(CheckpointInterval, default.checkpointIntervalMillis)
    )
  }

  /** Convenience wrapper for `Main`, the only place that touches the real environment. */
  def fromEnvironment(args: Array[String]): JobConfig = from(parseArguments(args.toSeq), sys.env)
}
