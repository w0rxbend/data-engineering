package de.kafkastreams.fraud

import de.common.domain.{CustomerId, Money, OrderId, PaymentStatus}

/** The business rule on its own, with no Kafka anywhere in sight. */
class FraudRulesSuite extends munit.FunSuite {

  private val customer = CustomerId("cust-0007")

  private def declinedPayment(cents: Long): PaidOrder =
    PaidOrder(
      orderId = OrderId("order-1"),
      customerId = customer,
      country = "DE",
      amount = Money.eur(cents),
      status = PaymentStatus.Declined,
      occurredAtEpochMillis = 0L
    )

  test("a tally accumulates the number and the value of declined attempts") {
    val tally = List(199L, 250L, 100L).foldLeft(DeclineTally.empty)((acc, c) => acc.add(declinedPayment(c)))
    assertEquals(tally, DeclineTally(3, 549L))
  }

  test("a window below the threshold produces no alert") {
    val result = FraudRules.alertFor(customer, DeclineTally(2, 400L), 0L, 600000L, threshold = 3)
    assertEquals(result, None)
  }

  test("a window at the threshold produces an alert carrying the window bounds") {
    val result = FraudRules.alertFor(customer, DeclineTally(3, 597L), 0L, 600000L, threshold = 3)
    assertEquals(
      result,
      Some(FraudAlert(customer, 3, 597L, 0L, 600000L, CustomerRisk.unknownTier))
    )
  }

  test("only declined payments are counted as suspicious") {
    val captured = declinedPayment(199L).copy(status = PaymentStatus.Captured)
    assert(declinedPayment(199L).isDeclined)
    assert(!captured.isDeclined)
  }
}
