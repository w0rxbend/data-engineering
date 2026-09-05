package de.flink.s3sink.core

import de.common.domain._

final class BatchWindowingSuite extends munit.FunSuite {

  private val oneHour = 3600000L

  private def order(id: String, customer: String, placedAt: Long, cents: Long): Order =
    Order(
      id = OrderId(id),
      customerId = CustomerId(customer),
      lines = List(OrderLine(Sku("SKU-COFFEE"), 1, Money.eur(cents))),
      placedAtEpochMillis = placedAt,
      country = "DE"
    )

  test("orders of one window are summed and the others stay buffered") {
    val buffered = List(
      BatchWindowing.assign(order("order-1", "cust-1", 10L, 500L), oneHour),
      BatchWindowing.assign(order("order-2", "cust-1", 20L, 250L), oneHour),
      BatchWindowing.assign(order("order-3", "cust-1", oneHour + 5L, 999L), oneHour)
    )

    val closed = BatchWindowing.close(CustomerId("cust-1"), buffered, windowStartMillis = 0L, oneHour)

    val batch = closed.batch.getOrElse(fail("expected the first window to produce a batch"))
    assertEquals(batch.orderIds, List(OrderId("order-1"), OrderId("order-2")))
    assertEquals(batch.orderCount, 2)
    assertEquals(batch.total, Money.eur(750L))
    assertEquals(batch.windowStartMillis, 0L)
    assertEquals(batch.windowEndMillis, oneHour)
    assertEquals(closed.remaining.map(_.order.id), List(OrderId("order-3")))
  }

  test("closing a window with nothing in it produces no record") {
    val closed = BatchWindowing.close(CustomerId("cust-1"), Nil, windowStartMillis = 0L, oneHour)
    assertEquals(closed.batch, None)
    assertEquals(closed.remaining, Nil)
  }

  test("an order is assigned to the window that contains its event time") {
    val assigned = BatchWindowing.assign(order("order-1", "cust-1", oneHour * 3 + 17L, 100L), oneHour)
    assertEquals(assigned.windowStartMillis, oneHour * 3)
  }

  test("a multi-line order contributes its full total") {
    val multiLine = Order(
      id = OrderId("order-9"),
      customerId = CustomerId("cust-1"),
      lines = List(
        OrderLine(Sku("SKU-COFFEE"), 2, Money.eur(500L)),
        OrderLine(Sku("SKU-MUG"), 1, Money.eur(250L))
      ),
      placedAtEpochMillis = 5L,
      country = "DE"
    )
    val closed = BatchWindowing.close(
      CustomerId("cust-1"),
      List(BatchWindowing.assign(multiLine, oneHour)),
      windowStartMillis = 0L,
      oneHour
    )
    assertEquals(closed.batch.map(_.total), Some(Money.eur(1250L)))
  }

  test("mixing currencies inside one batch is refused with a clear message") {
    val euros   = order("order-1", "cust-1", 1L, 100L)
    val dollars = order("order-2", "cust-1", 2L, 100L).copy(
      lines = List(OrderLine(Sku("SKU-MUG"), 1, Money(100L, "USD")))
    )
    val failure = intercept[IllegalArgumentException] {
      BatchAccumulator.fold(List(euros, dollars))
    }
    assert(failure.getMessage.contains("cannot mix currencies"), failure.getMessage)
  }
}
