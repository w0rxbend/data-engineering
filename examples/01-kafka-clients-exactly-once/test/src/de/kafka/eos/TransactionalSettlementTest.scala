package de.kafka.eos

import de.common.domain.{CustomerId, Money, Order, OrderId, OrderLine, Sku}
import de.common.json.Codecs
import munit.FunSuite

/**
 * The heart of the example: the transactional loop, driven against a recording stand-in instead of a broker.
 */
final class TransactionalSettlementTest extends FunSuite {

  private val chargedAt = 1700000123456L
  private val now       = () => chargedAt

  private def orderRecord(offset: Long, id: String, cents: Long): ConsumedRecord =
    ConsumedRecord(
      source = SourceOffset("orders", 0, offset),
      payload = Codecs.order(
        Order(
          id = OrderId(id),
          customerId = CustomerId("cust-0001"),
          lines = List(OrderLine(Sku("SKU-COFFEE"), 1, Money.eur(cents))),
          placedAtEpochMillis = 1700000000000L,
          country = "DE"
        )
      )
    )

  test("opens no transaction when the poll returned nothing") {
    val transaction = new RecordingTransaction
    assertEquals(TransactionalSettlement.settleBatch(Nil, now, transaction), BatchOutcome.Empty)
    assertEquals(transaction.steps, Nil)
  }

  test("emits one payment per order and commits them together") {
    val transaction = new RecordingTransaction
    val batch       = List(orderRecord(0L, "order-a", 500L), orderRecord(1L, "order-b", 700L))

    val outcome = TransactionalSettlement.settleBatch(batch, now, transaction)

    assertEquals(
      outcome match {
        case BatchOutcome.Committed(payments) => payments.map(_.orderId.value)
        case other                            => fail(s"expected a commit, got $other")
      },
      List("order-a", "order-b")
    )
  }

  test("commits the offset after the last record of each partition") {
    val transaction = new RecordingTransaction
    val batch       = List(orderRecord(4L, "order-a", 500L), orderRecord(5L, "order-b", 700L))

    TransactionalSettlement.settleBatch(batch, now, transaction)

    assertEquals(
      transaction.steps.collect { case TransactionStep.Committed(consumed) => consumed },
      List(List(SourceOffset("orders", 0, 4L), SourceOffset("orders", 0, 5L)))
    )
  }

  test("aborts the whole transaction when one record cannot be read") {
    val transaction = new RecordingTransaction
    val batch       = List(
      orderRecord(0L, "order-a", 500L),
      ConsumedRecord(SourceOffset("orders", 0, 1L), "this is not an order")
    )

    val outcome = TransactionalSettlement.settleBatch(batch, now, transaction)

    assert(outcome.isInstanceOf[BatchOutcome.Aborted], s"expected an abort, got $outcome")
    assert(transaction.steps.contains(TransactionStep.Aborted))
  }

  test("discards payments already emitted before the failing record") {
    val transaction = new RecordingTransaction
    val batch       = List(
      orderRecord(0L, "order-a", 500L),
      ConsumedRecord(SourceOffset("orders", 0, 1L), "this is not an order")
    )

    TransactionalSettlement.settleBatch(batch, now, transaction)

    // The payment for order-a was sent, but no commit ever followed it, so a
    // read_committed consumer would never see it.
    assert(transaction.steps.exists(_.isInstanceOf[TransactionStep.Emitted]))
    assert(!transaction.steps.exists(_.isInstanceOf[TransactionStep.Committed]))
  }

  test("stops at the first failure instead of settling the rest of the batch") {
    val transaction = new RecordingTransaction
    val batch       = List(
      ConsumedRecord(SourceOffset("orders", 0, 0L), "this is not an order"),
      orderRecord(1L, "order-b", 700L)
    )

    val outcome = TransactionalSettlement.settleBatch(batch, now, transaction)

    assertEquals(
      outcome match {
        case BatchOutcome.Aborted(failures) => failures.size
        case other                          => fail(s"expected an abort, got $other")
      },
      1
    )
  }

  test("aborts when the business rule refuses an order") {
    val transaction = new RecordingTransaction
    val outcome     = TransactionalSettlement.settleBatch(List(orderRecord(0L, "order-a", 0L)), now, transaction)

    assertEquals(
      outcome match {
        case BatchOutcome.Aborted(failures) => failures.map(_.describe.contains("not a chargeable amount"))
        case other                          => fail(s"expected an abort, got $other")
      },
      List(true)
    )
  }

  test("rewinds the reader to the start of an aborted batch so nothing is silently skipped") {
    val transaction = new RecordingTransaction
    val batch       = List(
      orderRecord(4L, "order-a", 500L),
      ConsumedRecord(SourceOffset("orders", 0, 5L), "this is not an order")
    )

    TransactionalSettlement.settleBatch(batch, now, transaction)

    assertEquals(
      transaction.steps.collect { case TransactionStep.RewoundTo(consumed) => consumed },
      List(List(SourceOffset("orders", 0, 4L), SourceOffset("orders", 0, 5L)))
    )
  }
}
