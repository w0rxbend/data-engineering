package de.flink.s3sink.job

import de.common.domain._
import de.flink.s3sink.core.OrderJson
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness

import scala.collection.JavaConverters._

/**
 * Exercises the windowing operator with Apache Flink's test harness.
 *
 * The harness runs a single operator in the current thread with a watermark that the test advances by hand. No cluster,
 * no Kafka and no object storage are involved, so this suite runs as fast as an ordinary unit test.
 */
final class CustomerBatchProcessFunctionSuite extends munit.FunSuite {

  private val oneHour = 3600000L

  private def order(id: String, customer: String, placedAt: Long, cents: Long): Order =
    Order(
      id = OrderId(id),
      customerId = CustomerId(customer),
      lines = List(OrderLine(Sku("SKU-COFFEE"), 1, Money.eur(cents))),
      placedAtEpochMillis = placedAt,
      country = "DE"
    )

  private def record(order: Order): OrderRecords.Incoming =
    OrderRecords.incoming(order.customerId.value, order.placedAtEpochMillis, OrderJson.encodeOrder(order))

  private def withHarness[A](
      body: KeyedOneInputStreamOperatorTestHarness[String, OrderRecords.Incoming, OrderRecords.Outgoing] => A
  ): A = {
    val operator = new KeyedProcessOperator(new CustomerBatchProcessFunction(oneHour))
    val harness  = new KeyedOneInputStreamOperatorTestHarness[String, OrderRecords.Incoming, OrderRecords.Outgoing](
      operator,
      new CustomerKeySelector,
      Types.STRING
    )
    harness.setup()
    harness.open()
    try body(harness)
    finally harness.close()
  }

  /** The JSON lines the operator has emitted so far. */
  private def emittedLines(
      harness: KeyedOneInputStreamOperatorTestHarness[String, OrderRecords.Incoming, OrderRecords.Outgoing]
  ): List[(String, String)] =
    harness.extractOutputStreamRecords().asScala.toList.map(r => (r.getValue.f0, r.getValue.f1))

  test("nothing is emitted before the watermark passes the end of the window") {
    withHarness { harness =>
      harness.processElement(record(order("order-1", "cust-1", 10L, 500L)), 10L)
      harness.processWatermark(oneHour - 2L)
      assertEquals(emittedLines(harness), Nil)
    }
  }

  test("one record per customer and window is emitted once the watermark passes") {
    withHarness { harness =>
      harness.processElement(record(order("order-1", "cust-1", 10L, 500L)), 10L)
      harness.processElement(record(order("order-2", "cust-1", 20L, 250L)), 20L)
      harness.processElement(record(order("order-3", "cust-2", 30L, 100L)), 30L)
      harness.processWatermark(oneHour)

      val emitted = emittedLines(harness).sortBy(_._1)
      assertEquals(emitted.size, 2)
      assertEquals(emitted.head._1, "customer_id=cust-1/dt=1970-01-01/hour=00")
      assert(emitted.head._2.contains("\"totalCents\":750"), emitted.head._2)
      assert(emitted(1)._2.contains("\"customerId\":\"cust-2\""), emitted(1)._2)
    }
  }

  test("orders of a later window are not swept into the closing one") {
    withHarness { harness =>
      harness.processElement(record(order("order-1", "cust-1", 10L, 500L)), 10L)
      harness.processElement(record(order("order-2", "cust-1", oneHour + 10L, 900L)), oneHour + 10L)
      harness.processWatermark(oneHour)

      val firstWindow = emittedLines(harness)
      assertEquals(firstWindow.size, 1)
      assert(firstWindow.head._2.contains("\"totalCents\":500"), firstWindow.head._2)

      harness.processWatermark(oneHour * 2)
      val bothWindows = emittedLines(harness)
      assertEquals(bothWindows.size, 2)
      assertEquals(bothWindows(1)._1, "customer_id=cust-1/dt=1970-01-01/hour=01")
      assert(bothWindows(1)._2.contains("\"totalCents\":900"), bothWindows(1)._2)
    }
  }

  test("buffered orders survive a checkpoint and restore") {
    val snapshot = withHarness { harness =>
      harness.processElement(record(order("order-1", "cust-1", 10L, 500L)), 10L)
      harness.snapshot(1L, 1L)
    }

    withHarness { harness =>
      harness.close()
      val operator = new KeyedProcessOperator(new CustomerBatchProcessFunction(oneHour))
      val restored = new KeyedOneInputStreamOperatorTestHarness[String, OrderRecords.Incoming, OrderRecords.Outgoing](
        operator,
        new CustomerKeySelector,
        Types.STRING
      )
      restored.setup()
      restored.initializeState(snapshot)
      restored.open()
      try {
        restored.processWatermark(oneHour)
        val emitted = emittedLines(restored)
        assertEquals(emitted.size, 1)
        assert(emitted.head._2.contains("\"totalCents\":500"), emitted.head._2)
      } finally restored.close()
    }
  }
}
