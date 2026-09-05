package de.spark.lakehouse

import de.spark.lakehouse.core.BronzeRecords.{OrderRow, PaymentRow, ShipmentRow}
import de.spark.lakehouse.core.MedallionTransforms
import org.apache.spark.sql.DataFrame

/**
 * Tests for the business logic of the lakehouse.
 *
 * Every test builds a handful of rows in memory, runs one transformation and checks the answer. Because the
 * transformations take and return `DataFrame`s and touch no storage, none of this needs Delta Lake, object storage or a
 * container.
 */
class MedallionTransformsSuite extends SparkSuite {

  private def order(
      orderId: String,
      customerId: String = "cust-0001",
      country: String = "de",
      totalCents: Long = 1000L,
      placedAt: Long = 1700000000000L,
      ingestedAt: Long = 1700000100000L
  ): OrderRow =
    OrderRow(orderId, customerId, country, 1, totalCents, "EUR", placedAt, ingestedAt)

  private def payment(
      orderId: String,
      status: String,
      amountCents: Long = 1000L,
      occurredAt: Long = 1700000050000L
  ): PaymentRow =
    PaymentRow(orderId, amountCents, "EUR", status, occurredAt, 1700000100000L)

  private def ordersFrame(rows: Seq[OrderRow]): DataFrame = {
    import spark.implicits._
    rows.toDF()
  }

  private def paymentsFrame(rows: Seq[PaymentRow]): DataFrame = {
    import spark.implicits._
    rows.toDF()
  }

  private def shipmentsFrame(rows: Seq[ShipmentRow]): DataFrame = {
    import spark.implicits._
    rows.toDF()
  }

  test("cleanOrders drops rows that are unusable") {
    val bronze = ordersFrame(
      Seq(
        order("order-1"),
        order("order-2", customerId = ""),
        order("order-3", totalCents = 0L)
      )
    )

    val kept = MedallionTransforms.cleanOrders(bronze).select("order_id").collect().map(_.getString(0)).toSet

    assertEquals(kept, Set("order-1"))
  }

  test("cleanOrders keeps only the most recently ingested copy of a duplicated order") {
    val bronze = ordersFrame(
      Seq(
        order("order-1", country = "de", ingestedAt = 1000L),
        order("order-1", country = "pl", ingestedAt = 2000L)
      )
    )

    val silver = MedallionTransforms.cleanOrders(bronze).collect()

    assertEquals(silver.length, 1)
    assertEquals(silver.head.getAs[String]("country"), "PL")
  }

  test("cleanOrders derives the calendar day of the order in Coordinated Universal Time") {
    // 1700000000000 milliseconds after 1970-01-01 is 2023-11-14T22:13:20Z.
    val silver = MedallionTransforms.cleanOrders(ordersFrame(Seq(order("order-1", placedAt = 1700000000000L))))

    assertEquals(silver.select("order_date").head().getDate(0).toString, "2023-11-14")
  }

  test("cleanPayments keeps the latest event per order and rejects unknown statuses") {
    val bronze = paymentsFrame(
      Seq(
        payment("order-1", "Authorized", occurredAt = 1000L),
        payment("order-1", "Captured", occurredAt = 2000L),
        payment("order-2", "Refunded", occurredAt = 3000L)
      )
    )

    val silver = MedallionTransforms
      .cleanPayments(bronze)
      .collect()
      .map(row => row.getAs[String]("order_id") -> row.getAs[String]("payment_status"))
      .toMap

    assertEquals(silver, Map("order-1" -> "Captured"))
  }

  test("cleanShipments reports the latest milestone of each parcel") {
    val bronze = shipmentsFrame(
      Seq(
        ShipmentRow("order-1", "Created", 1000L, 5000L),
        ShipmentRow("order-1", "Dispatched", 2000L, 5000L),
        ShipmentRow("order-1", "Delivered", 3000L, 5000L)
      )
    )

    val silver = MedallionTransforms.cleanShipments(bronze).collect()

    assertEquals(silver.length, 1)
    assertEquals(silver.head.getAs[String]("shipment_status"), "Delivered")
  }

  test("dailyRevenueByCountry counts captured payments only") {
    val orders = MedallionTransforms.cleanOrders(
      ordersFrame(
        Seq(
          order("order-1", customerId = "cust-1", country = "DE", totalCents = 1000L),
          order("order-2", customerId = "cust-2", country = "DE", totalCents = 4000L),
          order("order-3", customerId = "cust-3", country = "PL", totalCents = 700L)
        )
      )
    )
    val payments = MedallionTransforms.cleanPayments(
      paymentsFrame(
        Seq(
          payment("order-1", "Captured", amountCents = 1000L),
          payment("order-2", "Declined", amountCents = 4000L),
          payment("order-3", "Captured", amountCents = 700L)
        )
      )
    )

    val revenueByCountry = MedallionTransforms
      .dailyRevenueByCountry(orders, payments)
      .collect()
      .map(row => row.getAs[String]("country") -> row.getAs[Long]("revenue_cents"))
      .toMap

    assertEquals(revenueByCountry, Map("DE" -> 1000L, "PL" -> 700L))
  }

  test("customerLifetimeValue sums a customer's collected payments and averages them") {
    val orders = MedallionTransforms.cleanOrders(
      ordersFrame(
        Seq(
          order("order-1", customerId = "cust-1", totalCents = 1000L),
          order("order-2", customerId = "cust-1", totalCents = 3000L)
        )
      )
    )
    val payments = MedallionTransforms.cleanPayments(
      paymentsFrame(
        Seq(
          payment("order-1", "Captured", amountCents = 1000L),
          payment("order-2", "Captured", amountCents = 3000L)
        )
      )
    )

    val row = MedallionTransforms.customerLifetimeValue(orders, payments).head()

    assertEquals(row.getAs[String]("customer_id"), "cust-1")
    assertEquals(row.getAs[Long]("lifetime_value_cents"), 4000L)
    assertEquals(row.getAs[Long]("order_count"), 2L)
    assertEquals(row.getAs[Long]("average_order_value_cents"), 2000L)
  }

  test("customerDimension reports the country of the customer's most recent order") {
    val orders = MedallionTransforms.cleanOrders(
      ordersFrame(
        Seq(
          order("order-1", customerId = "cust-1", country = "DE", placedAt = 1000L),
          order("order-2", customerId = "cust-1", country = "PL", placedAt = 2000L)
        )
      )
    )

    val row = MedallionTransforms.customerDimension(orders).head()

    assertEquals(row.getAs[String]("country"), "PL")
    assertEquals(row.getAs[Long]("last_seen_epoch_millis"), 2000L)
  }
}
