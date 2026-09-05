package de.kafka.ops

/**
 * Turns the analysis types into the text this example prints.
 *
 * Rendering is kept apart from both the calculation and the cluster access so that the console output can be asserted
 * on in a unit test, character for character, without a broker anywhere near it.
 */
object Reports {

  private val Indent = "  "

  /** A short title with a rule underneath, used to separate the steps of the tour. */
  def heading(title: String): String = s"$title\n${"-" * title.length}"

  def topicPlan(plan: TopicPlan): String = {
    val configs = plan.configs.toList.sorted.map { case (key, value) => s"$Indent$Indent$key = $value" }
    val header  = s"$Indent${plan.name}: ${plan.partitions} partitions x ${plan.replicationFactor} replicas " +
      s"(${plan.totalReplicaCount} partition copies in total)"
    (header +: configs).mkString("\n")
  }

  /** One line per partition of a consumer group, plus a total. */
  def groupLag(lag: GroupLag): String = {
    val header = s"${Indent}consumer group '${lag.group}' is ${lag.totalLag} record(s) behind"
    val rows   = lag.partitions.map { entry =>
      val committed = entry.committedOffset.fold("never committed")(_.toString)
      f"$Indent$Indent${entry.ref.toString}%-24s end=${entry.endOffset}%-8d committed=$committed%-15s lag=${entry.lag}%d"
    }
    val worst = lag.worstPartition
      .filter(_.lag > 0)
      .map(entry => s"${Indent}${Indent}furthest behind: ${entry.ref} (${entry.lag} record(s))")
    ((header +: rows) ++ worst.toList).mkString("\n")
  }

  def replicaState(state: PartitionReplicaState): String = {
    val leader = state.leader.fold("none (offline)")(_.toString)
    val flag   =
      if (state.isOffline) { " <- offline" }
      else if (state.isUnderReplicated) {
        s" <- under-replicated, missing broker(s) ${state.missingReplicas.mkString(",")}"
      } else { "" }
    f"$Indent${state.ref.toString}%-24s leader=$leader%-14s replicas=${state.replicas.mkString(",")}%-10s " +
      s"in-sync=${state.inSyncReplicas.mkString(",")}$flag"
  }

  /** The cluster-wide health summary: a verdict line, then the partitions that earned attention. */
  def replicationReport(report: ReplicationReport): String = {
    val verdict =
      if (report.isHealthy) {
        s"${Indent}all ${report.partitionCount} partition(s) are fully replicated"
      } else {
        s"$Indent${report.underReplicated.size} of ${report.partitionCount} partition(s) are under-replicated, " +
          s"${report.offline.size} offline"
      }

    val perBroker = report.partitionsPerBroker.toList.sorted.map { case (broker, count) =>
      s"$Indent${Indent}broker $broker holds $count partition copy/copies"
    }

    val problems = (report.offline ++ report.underReplicated).distinct.map(replicaState)

    val skew = Option
      .when(report.leaderSkew.nonEmpty)(
        s"$Indent${report.leaderSkew.size} partition(s) are not led by their preferred replica; " +
          "a preferred-leader election would even that out"
      )
      .toList

    ((verdict +: perBroker) ++ problems ++ skew).mkString("\n")
  }

  /** A reassignment shown as "partition: old brokers -> new brokers", so it can be reviewed before it is applied. */
  def reassignmentPlan(
      topic: String,
      current: Map[Int, List[Int]],
      changes: Map[Int, List[Int]]
  ): String =
    if (changes.isEmpty) {
      s"${Indent}topic $topic is already spread the way the planner would spread it; nothing to move"
    } else {
      val rows = changes.toList.sortBy(_._1).map { case (partition, replicas) =>
        val before = current.getOrElse(partition, Nil).mkString(",")
        s"$Indent$Indent$topic-$partition: $before -> ${replicas.mkString(",")}"
      }
      (s"${Indent}moving ${changes.size} partition(s) of $topic:" +: rows).mkString("\n")
    }
}
