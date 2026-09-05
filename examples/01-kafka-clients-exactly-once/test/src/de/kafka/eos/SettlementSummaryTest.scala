package de.kafka.eos

import de.common.domain.{Money, OrderId, Payment, PaymentStatus}
import munit.FunSuite

/** The one line the service prints per batch. */
final class SettlementSummaryTest extends FunSuite {

  private val payment = Payment(OrderId("order-a"), Money.eur(1250L), PaymentStatus.Captured, 1700000123456L)

  test("says nothing at all when the poll returned no orders") {
    assertEquals(SettlementService.summarise(BatchOutcome.Empty), None)
  }

  test("names every payment a committed batch charged") {
    assertEquals(
      SettlementService.summarise(BatchOutcome.Committed(List(payment))),
      Some("committed 1 payment(s): order-a=12.50 EUR")
    )
  }

  test("explains why an aborted batch charged nobody") {
    val failure = SettlementFailure.Unreadable(SourceOffset("orders", 2, 7L), "not valid JSON: boom")
    assertEquals(
      SettlementService.summarise(BatchOutcome.Aborted(List(failure))),
      Some("aborted the transaction, nothing charged: record at orders-2@7 not valid JSON: boom")
    )
  }
}
