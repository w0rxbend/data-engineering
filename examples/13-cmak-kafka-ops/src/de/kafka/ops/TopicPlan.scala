package de.kafka.ops

/**
 * The shape an operator wants a topic to have.
 *
 * A "topic" is a named, append-only log in Apache Kafka. Two numbers decide how it behaves in production:
 *
 *   - `partitions`: how many independent sub-logs the topic is split into. Partitions are the unit of parallelism - one
 *     partition is read by at most one member of a consumer group - so this number caps how fast the pipeline can be
 *     consumed.
 *   - `replicationFactor`: how many brokers keep a copy of each partition. A replication factor of three survives the
 *     loss of one broker without losing data and without going read-only.
 *
 * @param configs
 *   per-topic settings, given as the string keys Kafka itself uses (for example `retention.ms`). They are kept as plain
 *   strings so that this type stays free of any Kafka class and can be unit-tested without a cluster.
 */
final case class TopicPlan(
    name: String,
    partitions: Int,
    replicationFactor: Short,
    configs: Map[String, String]
) {
  require(partitions > 0, s"topic $name needs at least one partition")
  require(replicationFactor > 0, s"topic $name needs at least one replica")

  /** Number of partition copies stored across the whole cluster: what the disks actually have to hold. */
  def totalReplicaCount: Int = partitions * replicationFactor.toInt
}

object TopicPlan {

  /** Seven days, expressed in milliseconds, which is the unit Kafka's `retention.ms` uses. */
  val sevenDaysMillis: Long = 7L * 24L * 60L * 60L * 1000L

  /**
   * The three topics of the shared online-shop order pipeline.
   *
   * `orders` is the busiest stream and gets six partitions. `payments` and `shipments` follow the same order
   * identifiers but carry fewer events, so three partitions are enough. All three are replicated three times, which is
   * exactly the number of brokers this example runs.
   */
  val orderPipeline: List[TopicPlan] = List(
    TopicPlan(
      name = "shop.orders",
      partitions = 6,
      replicationFactor = 3,
      configs = Map(
        "retention.ms"   -> sevenDaysMillis.toString,
        "cleanup.policy" -> "delete",
        // Refuse to elect a replica that has fallen behind: correctness over availability.
        "unclean.leader.election.enable" -> "false",
        // With three replicas, requiring two in-sync copies lets one broker die while
        // producers using `acks=all` keep writing.
        "min.insync.replicas" -> "2"
      )
    ),
    TopicPlan(
      name = "shop.payments",
      partitions = 3,
      replicationFactor = 3,
      configs = Map(
        "retention.ms"        -> sevenDaysMillis.toString,
        "cleanup.policy"      -> "delete",
        "min.insync.replicas" -> "2"
      )
    ),
    TopicPlan(
      name = "shop.shipments",
      partitions = 3,
      replicationFactor = 3,
      configs = Map(
        "retention.ms"        -> sevenDaysMillis.toString,
        "cleanup.policy"      -> "delete",
        "min.insync.replicas" -> "2"
      )
    )
  )

  /**
   * Checks a plan against the cluster it is about to be applied to.
   *
   * Returns one human-readable message per problem found, and an empty list when the plan is safe. Catching these on
   * paper is cheaper than watching a topic creation fail halfway through.
   */
  def validateAgainstCluster(plan: TopicPlan, brokerCount: Int): List[String] = {
    val tooFewBrokers =
      Option.when(plan.replicationFactor.toInt > brokerCount)(
        s"topic ${plan.name} asks for ${plan.replicationFactor} replicas but the cluster has only $brokerCount broker(s)"
      )

    val minInSyncTooHigh =
      plan.configs
        .get("min.insync.replicas")
        .flatMap(_.toIntOption)
        .filter(_ > plan.replicationFactor.toInt)
        .map(value =>
          s"topic ${plan.name} sets min.insync.replicas=$value, which is above its replication factor " +
            s"of ${plan.replicationFactor}; producers using acks=all would never succeed"
        )

    List(tooFewBrokers, minInSyncTooHigh).flatten
  }
}
