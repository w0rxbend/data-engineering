package de.kafka.ops

/**
 * A guided tour of day-two Apache Kafka operations, done from code.
 *
 * "Day two" is the part that starts after the first deployment: the pipeline exists, and the job is now to keep it
 * healthy. Each step below is something an operator does in a console such as CMAK, printed so that the two views can
 * be compared side by side.
 *
 * Run it with `./mill examples.13-cmak-kafka-ops.run` once the Docker stack from `docker/docker-compose.yml` is up. The
 * bootstrap servers can be overridden with a single argument.
 */
object Main {

  private val DefaultBootstrapServers = "localhost:11301,localhost:11302,localhost:11303"

  private val OrdersTopic = "shop.orders"
  private val LagGroup    = "shop.orders.reporting"

  def main(args: Array[String]): Unit = {
    val bootstrapServers = args.headOption.getOrElse(DefaultBootstrapServers)
    println(s"connecting to $bootstrapServers")

    val ops = KafkaOps.connect(bootstrapServers)
    try {
      val brokers = showCluster(ops)
      createTopics(ops, brokers.size)
      showTraffic(bootstrapServers)
      showLag(ops)
      showRetentionChange(ops)
      showReplicationHealth(ops)
      showReassignment(ops, brokers)
    } finally ops.close()
  }

  /** Step one: who is in the cluster. Everything else depends on the answer. */
  private def showCluster(ops: KafkaOps): List[Int] = {
    val brokers = ops.brokerIds()
    println(Reports.heading("1. cluster membership"))
    println(s"  brokers: ${brokers.mkString(", ")}")
    val topics = ops.userTopicNames()
    println(s"  existing user topics: ${
        if (topics.isEmpty) { "none" }
        else { topics.mkString(", ") }
      }")
    println()
    brokers
  }

  /** Step two: create the order-pipeline topics with the partition and replication settings they need. */
  private def createTopics(ops: KafkaOps, brokerCount: Int): Unit = {
    println(Reports.heading("2. topics of the order pipeline"))
    val plans = TopicPlan.orderPipeline

    val problems = plans.flatMap(plan => TopicPlan.validateAgainstCluster(plan, brokerCount))
    problems.foreach(problem => println(s"  refusing to apply: $problem"))

    if (problems.isEmpty) {
      plans.foreach(plan => println(Reports.topicPlan(plan)))
      val created = ops.createMissingTopics(plans)
      println(
        if (created.isEmpty) { "  every topic already existed; nothing was changed" }
        else { s"  created: ${created.mkString(", ")}" }
      )
    }
    println()
  }

  /** Step three: write orders and read some of them back, so the lag view has something to show. */
  private def showTraffic(bootstrapServers: String): Unit = {
    println(Reports.heading("3. traffic on the pipeline"))
    val produced = PipelineTraffic.produceOrders(bootstrapServers, OrdersTopic, count = 500)
    println(s"  produced $produced order(s) to $OrdersTopic")
    val consumed = PipelineTraffic.consumePartially(bootstrapServers, OrdersTopic, LagGroup, recordLimit = 120)
    println(s"  consumer group '$LagGroup' read and committed $consumed record(s), then left")
    println()
  }

  /** Step four: the question an on-call engineer is actually paged about. */
  private def showLag(ops: KafkaOps): Unit = {
    println(Reports.heading("4. consumer group lag"))
    val groups = ops.consumerGroupIds()
    println(s"  consumer groups on this cluster: ${
        if (groups.isEmpty) { "none" }
        else { groups.mkString(", ") }
      }")
    groups.foreach(group => println(Reports.groupLag(ops.groupLag(group))))
    println()
  }

  /** Step five: read a topic's configuration and change one setting without disturbing the rest. */
  private def showRetentionChange(ops: KafkaOps): Unit = {
    println(Reports.heading("5. retention change"))
    val before = ops.topicConfig(OrdersTopic)
    println(s"  retention.ms before: ${before.getOrElse("retention.ms", "unset")}")

    val threeDaysMillis = 3L * 24L * 60L * 60L * 1000L
    ops.alterTopicConfig(OrdersTopic, Map("retention.ms" -> threeDaysMillis.toString))

    val after = ops.topicConfig(OrdersTopic)
    println(s"  retention.ms after:  ${after.getOrElse("retention.ms", "unset")}")
    println(s"  min.insync.replicas is untouched: ${after.getOrElse("min.insync.replicas", "unset")}")
    println()
  }

  /** Step six: is every partition still fully replicated? This is the line to watch while a broker is stopped. */
  private def showReplicationHealth(ops: KafkaOps): Unit = {
    println(Reports.heading("6. replication health"))
    val states = ops.describeReplication(TopicPlan.orderPipeline.map(_.name))
    println(Reports.replicationReport(ReplicationHealth.report(states)))
    println()
  }

  /**
   * Step seven: plan a partition move, print it, and apply it.
   *
   * The plan is computed by a pure function, so what is printed is exactly what is sent. On a cluster that is already
   * evenly spread the plan comes out empty, and nothing is sent at all.
   */
  private def showReassignment(ops: KafkaOps, brokers: List[Int]): Unit = {
    println(Reports.heading("7. partition reassignment"))
    val current = ops.currentAssignment(OrdersTopic)
    val changes = ReassignmentPlanner.planExpansion(current, brokers)
    println(Reports.reassignmentPlan(OrdersTopic, current, changes))

    ops.startReassignment(OrdersTopic, changes)
    val inProgress = ops.reassignmentsInProgress()
    println(
      if (inProgress.isEmpty) { "  the cluster reports no partition movement outstanding" }
      else { s"  still moving: ${inProgress.keys.toList.map(_.toString).sorted.mkString(", ")}" }
    )
    println()
  }
}
