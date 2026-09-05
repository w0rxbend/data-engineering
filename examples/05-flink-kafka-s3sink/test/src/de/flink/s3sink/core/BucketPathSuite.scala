package de.flink.s3sink.core

import de.common.domain.{CustomerId, Money, OrderId}

final class BucketPathSuite extends munit.FunSuite {

  private def batchAt(customerId: String, windowStartMillis: Long): CustomerOrderBatch =
    CustomerOrderBatch(
      customerId = CustomerId(customerId),
      windowStartMillis = windowStartMillis,
      windowEndMillis = windowStartMillis + 3600000L,
      orderIds = List(OrderId("order-0000001")),
      total = Money.eur(1000L)
    )

  test("the directory is Hive-style and derived from the window start in UTC") {
    // 2023-11-14T22:00:00Z
    assertEquals(
      BucketPath.forBatch(batchAt("cust-0042", 1699999200000L)),
      "customer_id=cust-0042/dt=2023-11-14/hour=22"
    )
  }

  test("the same window always yields the same directory regardless of when it is closed") {
    val first  = BucketPath.forBatch(batchAt("cust-0042", 1699999200000L))
    val second = BucketPath.forBatch(batchAt("cust-0042", 1699999200000L))
    assertEquals(first, second)
  }

  test("characters that would break the directory layout are replaced") {
    assertEquals(
      BucketPath.forBatch(batchAt("cust/42 =x", 0L)),
      "customer_id=cust-42--x/dt=1970-01-01/hour=00"
    )
  }

  test("an empty customer identifier still produces a usable directory") {
    assert(BucketPath.forBatch(batchAt("   ", 0L)).startsWith("customer_id=unknown/"))
  }
}
