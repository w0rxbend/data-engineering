package de.kafka.ops

final class TopicPlanSuite extends munit.FunSuite {

  private val ordersPlan = TopicPlan.orderPipeline.find(_.name == "shop.orders").get

  test("the order pipeline covers the three topics of the shared shop domain") {
    assertEquals(TopicPlan.orderPipeline.map(_.name), List("shop.orders", "shop.payments", "shop.shipments"))
  }

  test("every topic of the pipeline is replicated three times") {
    TopicPlan.orderPipeline.foreach(plan => assertEquals(plan.replicationFactor, 3: Short))
  }

  test("the total number of partition copies is partitions times replicas") {
    assertEquals(ordersPlan.totalReplicaCount, 18)
  }

  test("a plan with zero partitions cannot be built at all") {
    intercept[IllegalArgumentException](TopicPlan("shop.orders", 0, 3, Map.empty))
  }

  test("a plan that fits its cluster raises no complaint") {
    assertEquals(TopicPlan.validateAgainstCluster(ordersPlan, brokerCount = 3), Nil)
  }

  test("asking for more replicas than the cluster has brokers is reported") {
    val problems = TopicPlan.validateAgainstCluster(ordersPlan, brokerCount = 2)
    assertEquals(problems.size, 1)
    assert(problems.head.contains("only 2 broker"), problems.head)
  }

  test("a min.insync.replicas above the replication factor is reported as unsatisfiable") {
    val impossible = ordersPlan.copy(configs = ordersPlan.configs.updated("min.insync.replicas", "4"))
    val problems   = TopicPlan.validateAgainstCluster(impossible, brokerCount = 3)
    assertEquals(problems.size, 1)
    assert(problems.head.contains("acks=all"), problems.head)
  }

  test("retention is expressed in milliseconds, which is the unit Kafka expects") {
    assertEquals(ordersPlan.configs("retention.ms"), "604800000")
  }
}
