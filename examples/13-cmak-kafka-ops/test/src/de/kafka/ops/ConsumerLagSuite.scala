package de.kafka.ops

/** Lag is a subtraction with two awkward edge cases; both are pinned down here. */
final class ConsumerLagSuite extends munit.FunSuite {

  private val ordersZero = PartitionRef("shop.orders", 0)
  private val ordersOne  = PartitionRef("shop.orders", 1)

  test("lag is the distance between the end of the log and the committed offset") {
    val lag = ConsumerLag.forPartition(PartitionOffsets(ordersZero, endOffset = 100, committedOffset = Some(40)))
    assertEquals(lag.lag, 60L)
  }

  test("a group that never committed is behind by the whole partition") {
    val lag = ConsumerLag.forPartition(PartitionOffsets(ordersZero, endOffset = 100, committedOffset = None))
    assertEquals(lag.lag, 100L)
  }

  test("a committed offset ahead of the end offset reports zero rather than negative lag") {
    val lag = ConsumerLag.forPartition(PartitionOffsets(ordersZero, endOffset = 100, committedOffset = Some(103)))
    assertEquals(lag.lag, 0L)
  }

  test("a fully caught up partition has no lag") {
    val lag = ConsumerLag.forPartition(PartitionOffsets(ordersZero, endOffset = 100, committedOffset = Some(100)))
    assertEquals(lag.lag, 0L)
  }

  test("group lag adds the partitions up and names the worst one") {
    val group = ConsumerLag.forGroup(
      "shop.orders.reporting",
      List(
        PartitionOffsets(ordersOne, endOffset = 500, committedOffset = Some(100)),
        PartitionOffsets(ordersZero, endOffset = 200, committedOffset = Some(190))
      )
    )
    assertEquals(group.totalLag, 410L)
    assertEquals(group.worstPartition.map(_.ref), Some(ordersOne))
  }

  test("group lag is sorted by topic and partition, so two runs print identically") {
    val group = ConsumerLag.forGroup(
      "shop.orders.reporting",
      List(
        PartitionOffsets(PartitionRef("shop.payments", 0), 10, Some(0)),
        PartitionOffsets(ordersOne, 10, Some(0)),
        PartitionOffsets(ordersZero, 10, Some(0))
      )
    )
    assertEquals(group.partitions.map(_.ref.toString), List("shop.orders-0", "shop.orders-1", "shop.payments-0"))
  }

  test("partitions without a committed offset are listed separately") {
    val group = ConsumerLag.forGroup(
      "shop.orders.reporting",
      List(
        PartitionOffsets(ordersZero, endOffset = 10, committedOffset = Some(10)),
        PartitionOffsets(ordersOne, endOffset = 10, committedOffset = None)
      )
    )
    assertEquals(group.uncommittedPartitions.map(_.ref), List(ordersOne))
  }

  test("a cluster snapshot retains partitions Kafka omitted from the committed-offset map") {
    val group = ConsumerLag.fromClusterSnapshot(
      "shop.orders.reporting",
      endOffsets = Map(ordersZero -> 20L, ordersOne -> 30L),
      committedOffsets = Map(ordersZero -> 12L)
    )

    assertEquals(group.partitions.map(_.ref), List(ordersZero, ordersOne))
    assertEquals(group.partitions.map(_.committedOffset), List(Some(12L), None))
    assertEquals(group.totalLag, 38L)
  }

  test("a truncated multi-partition poll commits only the records inside the limit") {
    val selection = PollSelection.upTo(
      List(
        PolledOffset(ordersZero, 10L),
        PolledOffset(ordersZero, 11L),
        PolledOffset(ordersOne, 20L),
        PolledOffset(ordersOne, 21L)
      ),
      recordLimit = 3
    )

    assertEquals(selection.recordsAccepted, 3)
    assertEquals(selection.nextOffsets, Map(ordersZero -> 12L, ordersOne -> 21L))
  }
}
