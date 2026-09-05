package de.kafka.eos

import de.common.domain.{CustomerId, Money, Order, OrderId, OrderLine, PaymentStatus, Sku}
import munit.FunSuite

/** Exercises the business rule on its own; no Kafka, no docker. */
final class SettlementTest extends FunSuite {

  private val chargedAt = 1700000123456L

  private def order(lines: List[OrderLine]): Order =
    Order(
      id = OrderId("order-0000001"),
      customerId = CustomerId("cust-0001"),
      lines = lines,
      placedAtEpochMillis = 1700000000000L,
      country = "DE"
    )

  private def line(cents: Long, quantity: Int = 1, currency: String = "EUR"): OrderLine =
    OrderLine(Sku("SKU-COFFEE"), quantity, Money(cents, currency))

  test("captures the sum of all order lines") {
    val settled = Settlement.settle(order(List(line(500), line(250, quantity = 2))), chargedAt)
    assertEquals(settled.map(_.amount), Right(Money.eur(1000L)))
  }

  test("stamps the payment with the supplied time rather than the system clock") {
    val settled = Settlement.settle(order(List(line(500))), chargedAt)
    assertEquals(settled.map(_.occurredAtEpochMillis), Right(chargedAt))
  }

  test("marks a settled payment as captured") {
    val settled = Settlement.settle(order(List(line(500))), chargedAt)
    assertEquals(settled.map(_.status), Right(PaymentStatus.Captured: PaymentStatus))
  }

  test("refuses an order without lines") {
    val settled = Settlement.settle(order(Nil), chargedAt)
    assertEquals(settled, Left(SettlementRejection.NothingToCharge(OrderId("order-0000001"))))
  }

  test("refuses an order that totals nothing") {
    val settled = Settlement.settle(order(List(line(0))), chargedAt)
    assertEquals(settled, Left(SettlementRejection.NonPositiveTotal(OrderId("order-0000001"), 0L)))
  }

  test("refuses a currency the payment provider does not settle") {
    val settled = Settlement.settle(order(List(line(500, currency = "USD"))), chargedAt)
    assertEquals(settled, Left(SettlementRejection.UnsupportedCurrency(OrderId("order-0000001"), "USD")))
  }
}
