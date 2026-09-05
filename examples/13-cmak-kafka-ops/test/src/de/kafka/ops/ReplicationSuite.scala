package de.kafka.ops

/**
 * The replication analysis and the reassignment planner, checked with hand-written partition states.
 *
 * These are the fakes that replace a cluster: a `PartitionReplicaState` is exactly what `KafkaOps.describeReplication`
 * would have produced, so the analysis can be exercised without Docker.
 */
final class ReplicationSuite extends munit.FunSuite {

  private def state(partition: Int, leader: Option[Int], replicas: List[Int], isr: List[Int]) =
    PartitionReplicaState(PartitionRef("shop.orders", partition), leader, replicas, isr)

  private val healthy = state(0, Some(1), List(1, 2, 3), List(1, 2, 3))
  private val lostOne = state(1, Some(2), List(1, 2, 3), List(2, 3))
  private val offline = state(2, None, List(1, 2, 3), List())

  test("a partition whose in-sync set matches its replica list is healthy") {
    assert(!healthy.isUnderReplicated)
    assert(!healthy.isOffline)
    assertEquals(healthy.missingReplicas, Nil)
  }

  test("a partition missing an in-sync replica is under-replicated and names the missing broker") {
    assert(lostOne.isUnderReplicated)
    assertEquals(lostOne.missingReplicas, List(1))
  }

  test("a partition without a leader is offline") {
    assert(offline.isOffline)
    assert(offline.isUnderReplicated)
  }

  test("leadership sitting on a follower is flagged as skew") {
    assert(lostOne.leaderIsNotPreferred)
    assert(!healthy.leaderIsNotPreferred)
  }

  test("the report separates offline from merely under-replicated partitions") {
    val report = ReplicationHealth.report(List(healthy, lostOne, offline))
    assertEquals(report.partitionCount, 3)
    assertEquals(report.offline.map(_.ref.partition), List(2))
    assertEquals(report.underReplicated.map(_.ref.partition), List(1, 2))
    assert(!report.isHealthy)
  }

  test("a cluster with nothing wrong reports itself healthy") {
    assert(ReplicationHealth.report(List(healthy)).isHealthy)
  }

  test("replica copies are counted per broker") {
    val report = ReplicationHealth.report(List(healthy, lostOne))
    assertEquals(report.partitionsPerBroker, Map(1 -> 2, 2 -> 2, 3 -> 2))
  }

  test("the planner spreads leadership evenly round-robin over the brokers") {
    val assignment = ReassignmentPlanner.assign(partitionCount = 6, replicationFactor = 3, brokerIds = List(1, 2, 3))
    assertEquals(assignment.toList.sortBy(_._1).map(_._2.head), List(1, 2, 3, 1, 2, 3))
  }

  test("every partition gets exactly as many distinct replicas as the replication factor asks for") {
    val assignment = ReassignmentPlanner.assign(partitionCount = 9, replicationFactor = 3, brokerIds = List(1, 2, 3, 4))
    assignment.values.foreach { replicas =>
      assertEquals(replicas.size, 3)
      assertEquals(replicas.distinct.size, 3)
    }
  }

  test("the follower shift stops every partition from reusing the same replica pair") {
    val assignment = ReassignmentPlanner.assign(partitionCount = 6, replicationFactor = 2, brokerIds = List(1, 2, 3))
    assertEquals(assignment(0), List(1, 2))
    assertEquals(assignment(3), List(1, 3))
  }

  test("a start index rotates the whole assignment, as Kafka does per topic") {
    val fromZero = ReassignmentPlanner.assign(3, 2, List(1, 2, 3), startIndex = 0)
    val fromOne  = ReassignmentPlanner.assign(3, 2, List(1, 2, 3), startIndex = 1)
    assertEquals(fromOne(0), fromZero(1))
  }

  test("asking for more replicas than there are brokers is rejected before anything is sent") {
    intercept[IllegalArgumentException](ReassignmentPlanner.assign(1, 4, List(1, 2, 3)))
  }

  test("only partitions whose replica list actually differs are part of a change set") {
    val current = Map(0 -> List(1, 2), 1 -> List(2, 3))
    val desired = Map(0 -> List(1, 2), 1 -> List(2, 4))
    assertEquals(ReassignmentPlanner.changesOnly(current, desired), Map(1 -> List(2, 4)))
  }

  test("a topic already spread over the brokers produces an empty plan") {
    val current = ReassignmentPlanner.assign(6, 3, List(1, 2, 3))
    assertEquals(ReassignmentPlanner.planExpansion(current, List(1, 2, 3)), Map.empty[Int, List[Int]])
  }

  test("a balanced topic that merely starts on another broker is left alone") {
    val current = ReassignmentPlanner.assign(6, 3, List(1, 2, 3), startIndex = 2)
    assertEquals(ReassignmentPlanner.planExpansion(current, List(1, 2, 3)), Map.empty[Int, List[Int]])
  }

  test("a balanced topic whose followers sit at another distance is also left alone") {
    // Kafka randomises two numbers when it creates a topic: which broker leads partition 0,
    // and how far behind the leader the first follower sits. A live three-broker cluster
    // really does hand out 1,3,2 / 2,1,3 / 3,2,1 as often as 1,2,3 / 2,3,1 / 3,1,2, and both
    // are equally balanced, so neither may produce a plan.
    val current = ReassignmentPlanner.assign(6, 3, List(1, 2, 3), startIndex = 0, replicaShift = 1)
    assertEquals(current(0), List(1, 3, 2))
    assertEquals(ReassignmentPlanner.planExpansion(current, List(1, 2, 3)), Map.empty[Int, List[Int]])
  }

  test("adding a broker moves some partitions and keeps the replication factor") {
    val current = ReassignmentPlanner.assign(6, 3, List(1, 2, 3))
    val changes = ReassignmentPlanner.planExpansion(current, List(1, 2, 3, 4))
    assert(changes.nonEmpty, "a fourth broker should attract partitions")
    assert(changes.values.exists(_.contains(4)), "the new broker should end up holding partitions")
    changes.values.foreach(replicas => assertEquals(replicas.size, 3))
  }
}
