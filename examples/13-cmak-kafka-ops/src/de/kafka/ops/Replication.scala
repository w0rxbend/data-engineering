package de.kafka.ops

/**
 * The replication state of a single partition, as a broker reports it.
 *
 * @param leader
 *   the broker currently accepting writes for this partition. `None` means the partition is offline: no replica is able
 *   to lead, so producers and consumers both fail on it.
 * @param replicas
 *   every broker that is supposed to hold a copy, in preference order. The first entry is the "preferred leader": the
 *   broker Kafka elects when leadership is rebalanced.
 * @param inSyncReplicas
 *   the subset of `replicas` that has caught up with the leader. Kafka calls this the ISR (in-sync replica set).
 */
final case class PartitionReplicaState(
    ref: PartitionRef,
    leader: Option[Int],
    replicas: List[Int],
    inSyncReplicas: List[Int]
) {

  /** True when at least one copy has fallen behind or is missing: the cluster is one failure closer to data loss. */
  def isUnderReplicated: Boolean = inSyncReplicas.size < replicas.size

  /** True when no replica can serve the partition at all. */
  def isOffline: Boolean = leader.isEmpty

  /** True when the leader is not the first entry of `replicas`, so leadership is unevenly spread. */
  def leaderIsNotPreferred: Boolean =
    (leader, replicas.headOption) match {
      case (Some(current), Some(preferred)) => current != preferred
      case _                                => false
    }

  /** Replicas listed for the partition that are not in sync. */
  def missingReplicas: List[Int] = replicas.filterNot(inSyncReplicas.contains)
}

/** A cluster-wide summary of partition health, ready to be printed or turned into alerts. */
final case class ReplicationReport(
    partitionCount: Int,
    underReplicated: List[PartitionReplicaState],
    offline: List[PartitionReplicaState],
    leaderSkew: List[PartitionReplicaState],
    partitionsPerBroker: Map[Int, Int]
) {

  /** True when nothing needs an operator's attention. */
  def isHealthy: Boolean = underReplicated.isEmpty && offline.isEmpty
}

/** Pure analysis of partition states. The code that reads those states from a cluster lives in `KafkaOps`. */
object ReplicationHealth {

  def report(states: List[PartitionReplicaState]): ReplicationReport = {
    val sorted = states.sortBy(state => (state.ref.topic, state.ref.partition))
    ReplicationReport(
      partitionCount = sorted.size,
      underReplicated = sorted.filter(_.isUnderReplicated),
      offline = sorted.filter(_.isOffline),
      leaderSkew = sorted.filter(_.leaderIsNotPreferred),
      partitionsPerBroker = countReplicasPerBroker(sorted)
    )
  }

  /** How many partition copies each broker is asked to store: the quantity a reassignment tries to even out. */
  def countReplicasPerBroker(states: List[PartitionReplicaState]): Map[Int, Int] =
    states.flatMap(_.replicas).groupMapReduce(identity)(_ => 1)(_ + _)
}

/**
 * Works out where partition copies should live.
 *
 * This mirrors the algorithm Kafka itself uses when it creates a topic: walk the brokers round-robin for the first
 * replica of each partition, then place the remaining replicas at a shifting distance from that broker. The shift is
 * what stops every partition from having the same neighbour, so that losing one broker spreads its follower load over
 * all the others instead of dumping it on a single machine.
 *
 * Having it here as a pure function means a reassignment can be reviewed - printed, diffed, asserted on in a test -
 * before it is sent to a live cluster.
 */
object ReassignmentPlanner {

  /**
   * Assignment for a whole topic.
   *
   * @param startIndex
   *   which broker the first partition starts on. Kafka randomises this per topic so that several small topics do not
   *   all pile their partition 0 onto the same broker; a test passes a fixed value to get a predictable result.
   * @param replicaShift
   *   how far the first follower sits behind its leader. Kafka randomises this per topic as well, for the same reason;
   *   it decides which broker is the second copy of partition 0.
   * @return
   *   partition number to the list of broker identifiers that should hold it, leader first.
   */
  def assign(
      partitionCount: Int,
      replicationFactor: Int,
      brokerIds: List[Int],
      startIndex: Int = 0,
      replicaShift: Int = 0
  ): Map[Int, List[Int]] = {
    require(partitionCount > 0, "a topic needs at least one partition")
    require(replicationFactor > 0, "a partition needs at least one replica")
    require(
      replicationFactor <= brokerIds.size,
      s"cannot place $replicationFactor replicas on ${brokerIds.size} broker(s)"
    )

    val brokers     = brokerIds.sorted
    val brokerCount = brokers.size

    (0 until partitionCount).map { partition =>
      val leaderIndex = (partition + startIndex) % brokerCount
      // Every full round through the brokers moves the followers one step further
      // away from their leader.
      val followerShift   = replicaShift + (partition / brokerCount)
      val followerIndices = (0 until replicationFactor - 1).map { replica =>
        (leaderIndex + 1 + (followerShift + replica) % (brokerCount - 1)) % brokerCount
      }
      partition -> (leaderIndex +: followerIndices).map(brokers).toList
    }.toMap
  }

  /**
   * The subset of an assignment that actually differs from what the cluster has today.
   *
   * Sending only the changed partitions to the broker keeps a reassignment as small as possible: every partition in the
   * request copies its whole log to the new broker, so an accidental no-op entry is expensive.
   */
  def changesOnly(
      current: Map[Int, List[Int]],
      desired: Map[Int, List[Int]]
  ): Map[Int, List[Int]] =
    desired.filter { case (partition, replicas) => !current.get(partition).contains(replicas) }

  /**
   * Plan for spreading an existing topic over a broker set that has grown.
   *
   * Two details keep the plan small. The replication factor is taken from the topic as it stands, so rebalancing
   * placement never silently changes the durability guarantee. And the layout is chosen rather than assumed: when Kafka
   * creates a topic it picks *two* random numbers - which broker partition 0 leads on, and how far behind the leader
   * the first follower sits - so comparing against a single fixed layout would report every partition as "wrong" and
   * copy the entire topic across the cluster for no benefit. Every combination of the two is tried and the one that
   * moves the fewest partitions wins, which gives the same balance for the least data movement.
   */
  def planExpansion(current: Map[Int, List[Int]], brokerIds: List[Int]): Map[Int, List[Int]] = {
    require(current.nonEmpty, "cannot plan a reassignment for a topic with no partitions")
    val replicationFactor = current.values.map(_.size).max
    val shifts            = if (brokerIds.size > 1) { 0 until brokerIds.size - 1 }
    else { 0 to 0 }
    val candidates = for {
      startIndex   <- brokerIds.indices
      replicaShift <- shifts
    } yield changesOnly(current, assign(current.size, replicationFactor, brokerIds, startIndex, replicaShift))
    candidates.minBy(_.size)
  }
}
