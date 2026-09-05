package de.kafka.ops

/** The printed output is part of what this example teaches, so it is asserted on rather than eyeballed. */
final class ReportsSuite extends munit.FunSuite {

  test("a heading is underlined to exactly its own width") {
    assertEquals(Reports.heading("6. replication health"), "6. replication health\n---------------------")
  }

  test("a topic plan shows its shape and its settings, sorted so the output is stable") {
    val plan     = TopicPlan("shop.orders", 6, 3, Map("retention.ms" -> "1000", "cleanup.policy" -> "delete"))
    val rendered = Reports.topicPlan(plan)
    assert(rendered.contains("6 partitions x 3 replicas"), rendered)
    assert(rendered.indexOf("cleanup.policy") < rendered.indexOf("retention.ms"), rendered)
  }

  test("a lag report names the group, the total and the partition furthest behind") {
    val lag = ConsumerLag.forGroup(
      "shop.orders.reporting",
      List(
        PartitionOffsets(PartitionRef("shop.orders", 0), 500, Some(100)),
        PartitionOffsets(PartitionRef("shop.orders", 1), 20, Some(20))
      )
    )
    val rendered = Reports.groupLag(lag)
    assert(rendered.contains("'shop.orders.reporting' is 400 record(s) behind"), rendered)
    assert(rendered.contains("furthest behind: shop.orders-0 (400 record(s))"), rendered)
  }

  test("a caught up group is not told which partition is furthest behind") {
    val lag = ConsumerLag.forGroup("g", List(PartitionOffsets(PartitionRef("shop.orders", 0), 20, Some(20))))
    assert(!Reports.groupLag(lag).contains("furthest behind"), Reports.groupLag(lag))
  }

  test("a partition that never committed says so instead of printing a misleading offset") {
    val lag = ConsumerLag.forGroup("g", List(PartitionOffsets(PartitionRef("shop.orders", 0), 20, None)))
    assert(Reports.groupLag(lag).contains("never committed"), Reports.groupLag(lag))
  }

  test("a healthy cluster gets one summary line and no problem rows") {
    val states = List(
      PartitionReplicaState(PartitionRef("shop.orders", 0), Some(1), List(1, 2, 3), List(1, 2, 3)),
      PartitionReplicaState(PartitionRef("shop.orders", 1), Some(2), List(2, 3, 1), List(2, 3, 1))
    )
    val rendered = Reports.replicationReport(ReplicationHealth.report(states))
    assert(rendered.contains("all 2 partition(s) are fully replicated"), rendered)
    assert(!rendered.contains("under-replicated"), rendered)
  }

  test("a stopped broker shows up as an under-replicated partition naming the missing broker") {
    val states = List(
      PartitionReplicaState(PartitionRef("shop.orders", 0), Some(2), List(1, 2, 3), List(2, 3))
    )
    val rendered = Reports.replicationReport(ReplicationHealth.report(states))
    assert(rendered.contains("1 of 1 partition(s) are under-replicated"), rendered)
    assert(rendered.contains("missing broker(s) 1"), rendered)
  }

  test("an offline partition is labelled offline rather than under-replicated") {
    val state = PartitionReplicaState(PartitionRef("shop.orders", 0), None, List(1), Nil)
    assert(Reports.replicaState(state).contains("<- offline"), Reports.replicaState(state))
  }

  test("a reassignment is printed as old brokers arrow new brokers") {
    val current  = Map(0 -> List(1, 2, 3), 1 -> List(2, 3, 1))
    val changes  = Map(1 -> List(2, 3, 4))
    val rendered = Reports.reassignmentPlan("shop.orders", current, changes)
    assert(rendered.contains("moving 1 partition(s) of shop.orders"), rendered)
    assert(rendered.contains("shop.orders-1: 2,3,1 -> 2,3,4"), rendered)
  }

  test("an empty reassignment says plainly that nothing has to move") {
    val rendered = Reports.reassignmentPlan("shop.orders", Map(0 -> List(1, 2)), Map.empty)
    assert(rendered.contains("nothing to move"), rendered)
  }
}
