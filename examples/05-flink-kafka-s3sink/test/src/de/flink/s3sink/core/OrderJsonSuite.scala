package de.flink.s3sink.core

import de.common.domain.{CustomerId, Money, OrderId}
import de.common.gen.DataGenerator

final class OrderJsonSuite extends munit.FunSuite {

  test("an order survives a round trip through the shared wire format") {
    val order = new DataGenerator(seed = 7L).nextOrder()
    assertEquals(OrderJson.decode(OrderJson.encodeOrder(order).getBytes("UTF-8")), Right(order))
  }

  test("every generated order decodes, so producer and consumer agree") {
    val orders = new DataGenerator(seed = 11L).orders(50)
    orders.foreach { order =>
      assert(OrderJson.decode(OrderJson.encodeOrder(order).getBytes("UTF-8")).isRight, s"failed for $order")
    }
  }

  test("a malformed payload becomes a value, not an exception") {
    val failure = OrderJson.decode("this is not json".getBytes("UTF-8"))
    assert(failure.isLeft)
    assertEquals(failure.left.toOption.map(_.payload), Some("this is not json"))
  }

  test("a JSON object missing a required field is reported as a failure") {
    assert(OrderJson.decode("""{"id":"order-1"}""".getBytes("UTF-8")).isLeft)
  }

  test("the batch record is rendered as a single JSON line") {
    val batch = CustomerOrderBatch(
      customerId = CustomerId("cust-0001"),
      windowStartMillis = 0L,
      windowEndMillis = 3600000L,
      orderIds = List(OrderId("order-1"), OrderId("order-2")),
      total = Money.eur(2500L)
    )
    assertEquals(
      OrderJson.encodeBatch(batch),
      """{"customerId":"cust-0001","windowStart":0,"windowEnd":3600000,"orderCount":2,""" +
        """"orderIds":["order-1","order-2"],"totalCents":2500,"currency":"EUR"}"""
    )
  }
}
