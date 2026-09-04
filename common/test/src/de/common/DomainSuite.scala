package de.common

import de.common.domain._
import de.common.gen.DataGenerator
import de.common.json.Codecs

class DomainSuite extends munit.FunSuite {

  test("order total sums every line") {
    val order = Order(
      OrderId("order-1"),
      CustomerId("cust-1"),
      List(
        OrderLine(Sku("SKU-MUG"), 2, Money.eur(500)),
        OrderLine(Sku("SKU-FILTER"), 1, Money.eur(250))
      ),
      placedAtEpochMillis = 0L,
      country = "DE"
    )
    assertEquals(order.total, Money.eur(1250))
  }

  test("adding two amounts in different currencies is rejected") {
    intercept[IllegalArgumentException](Money.eur(100) + Money(100, "USD"))
  }

  test("the generator is reproducible for a given seed") {
    val left  = new DataGenerator(seed = 7L).orders(20)
    val right = new DataGenerator(seed = 7L).orders(20)
    assertEquals(left, right)
  }

  test("orders are rendered as JSON that keeps every line") {
    val order = new DataGenerator(seed = 1L).nextOrder()
    val json  = Codecs.order(order)
    assert(json.startsWith("{\"id\":"), json)
    assertEquals(json.split("\"sku\"", -1).length - 1, order.lines.size)
  }

  test("payment status round trips through its textual form") {
    PaymentStatus.all.foreach { status =>
      assertEquals(PaymentStatus.fromString(status.toString), Some(status))
    }
  }
}
