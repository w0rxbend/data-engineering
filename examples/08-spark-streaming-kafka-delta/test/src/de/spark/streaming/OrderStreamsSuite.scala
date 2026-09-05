package de.spark.streaming

import de.common.domain._
import de.common.json.Codecs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.MemoryStream

import java.sql.Timestamp

/** Tests for the pure transformations, run against a local Apache Spark session and in-memory data only. */
final class OrderStreamsSuite extends SparkSuite {

  import OrderStreamsSuite._

  test("parseOrders extracts the order fields and sums the line totals") {
    val order  = orderAt("order-1", "DE", at("12:01"), Seq((2, 500L), (1, 1250L)))
    val parsed = OrderStreams.parseOrders(rawRecords(Seq(order))).collect()

    assertEquals(parsed.length, 1)
    val row = parsed.head
    assertEquals(row.getAs[String]("orderId"), "order-1")
    assertEquals(row.getAs[String]("country"), "DE")
    assertEquals(row.getAs[String]("currency"), "EUR")
    assertEquals(row.getAs[Int]("lineCount"), 2)
    // 2 * 500 + 1 * 1250
    assertEquals(row.getAs[Long]("totalCents"), 2250L)
    assertEquals(row.getAs[Timestamp](OrderStreams.EventTimeColumn).getTime, at("12:01"))
  }

  test("parseOrders drops records that do not match the schema instead of failing the query") {
    val good    = orderAt("order-1", "DE", at("12:01"), Seq((1, 1000L)))
    val records = rawRecords(Seq(good), extraPayloads = Seq("not json at all", """{"unexpected":true}"""))

    val orderIds = OrderStreams.parseOrders(records).collect().map(_.getAs[String]("orderId")).toSet
    assertEquals(orderIds, Set("order-1"))
  }

  test("revenuePerWindow buckets orders into fixed five-minute event-time windows") {
    val orders = Seq(
      orderAt("order-1", "DE", at("12:01"), Seq((1, 1000L))),
      orderAt("order-2", "DE", at("12:04"), Seq((1, 500L))),
      orderAt("order-3", "DE", at("12:06"), Seq((1, 700L))),
      orderAt("order-4", "PL", at("12:02"), Seq((1, 300L)))
    )

    val revenue = OrderStreams
      .revenuePerWindow(OrderStreams.parseOrders(rawRecords(orders)), "5 minutes", "10 minutes")
      .collect()
      .map(windowSummary)
      .toSet

    assertEquals(
      revenue,
      Set(
        ("12:00", "DE", 1500L, 2L),
        ("12:05", "DE", 700L, 1L),
        ("12:00", "PL", 300L, 1L)
      )
    )
  }

  test("deduplicateByOrderId keeps only the newest version of a republished order") {
    val orders = Seq(
      orderAt("order-1", "DE", at("12:01"), Seq((1, 1000L))),
      orderAt("order-1", "DE", at("12:03"), Seq((1, 1800L))),
      orderAt("order-2", "PL", at("12:02"), Seq((1, 300L)))
    )

    val kept = OrderStreams
      .deduplicateByOrderId(OrderStreams.parseOrders(rawRecords(orders)))
      .collect()
      .map(row => row.getAs[String]("orderId") -> row.getAs[Long]("totalCents"))
      .toMap

    assertEquals(kept, Map("order-1" -> 1800L, "order-2" -> 300L))
  }

  test("a closed window is emitted once in append mode and later stragglers are ignored") {
    import spark.implicits._
    implicit val sqlContext: org.apache.spark.sql.SQLContext = spark.sqlContext
    val source                                               = MemoryStream[String]

    val revenue = OrderStreams.revenuePerWindow(
      OrderStreams.parseOrders(source.toDF()),
      windowDuration = "5 minutes",
      watermarkDelay = "10 minutes"
    )

    val query = revenue.writeStream
      .format("memory")
      .queryName("revenue_append_test")
      .outputMode("append")
      .start()

    try {
      // Batch 1: two orders inside the 12:00-12:05 window. The window is still
      // open, so append mode emits nothing yet.
      source.addData(payloads(orderAt("order-1", "DE", at("12:01"), Seq((1, 1000L)))))
      source.addData(payloads(orderAt("order-2", "DE", at("12:04"), Seq((1, 500L)))))
      query.processAllAvailable()
      assertEquals(spark.table("revenue_append_test").count(), 0L, "an open window must not be emitted")

      // Batch 2 pushes the newest event time to 12:20, so the watermark moves to
      // 12:10 - past the end of the 12:00-12:05 window.
      source.addData(payloads(orderAt("order-3", "DE", at("12:20"), Seq((1, 900L)))))
      query.processAllAvailable()

      // Spark computes the new watermark at the end of a batch and evicts closed
      // windows during the next one, so one more batch is needed to see the row.
      source.addData(payloads(orderAt("order-4", "DE", at("12:30"), Seq((1, 100L)))))
      query.processAllAvailable()

      val closed = spark.table("revenue_append_test").collect().map(windowSummary).toSet
      assertEquals(closed, Set(("12:00", "DE", 1500L, 2L)))

      // A straggler older than the watermark is dropped: the already-emitted
      // window is never corrected.
      source.addData(payloads(orderAt("order-5", "DE", at("12:02"), Seq((1, 9999L)))))
      source.addData(payloads(orderAt("order-6", "DE", at("12:40"), Seq((1, 100L)))))
      query.processAllAvailable()

      val stillClosed = spark
        .table("revenue_append_test")
        .collect()
        .map(windowSummary)
        .filter { case (start, _, _, _) => start == "12:00" }
        .toSet
      assertEquals(stillClosed, Set(("12:00", "DE", 1500L, 2L)), "late data must not reopen a closed window")
    } finally {
      query.stop()
      spark.catalog.dropTempView("revenue_append_test")
    }
  }

  /** Wraps order payloads in a one-column `DataFrame` shaped like the Kafka source's `value` column. */
  private def rawRecords(orders: Seq[Order], extraPayloads: Seq[String] = Seq.empty): DataFrame = {
    import spark.implicits._
    (orders.map(Codecs.order) ++ extraPayloads).toDF("value")
  }

  private def payloads(orders: Order*): Seq[String] = orders.map(Codecs.order)
}

object OrderStreamsSuite {

  /** 2024-01-01, the day every test in this suite pretends to be. */
  private val DayStartEpochMillis = 1704067200000L

  /** `at("12:01")` is 2024-01-01T12:01:00Z in milliseconds since the epoch. */
  def at(hourAndMinute: String): Long = {
    val Array(hours, minutes) = hourAndMinute.split(":").map(_.toInt)
    DayStartEpochMillis + (hours * 60L + minutes) * 60000L
  }

  /** Builds an order whose lines are `(quantity, unit price in cents)` pairs. */
  def orderAt(id: String, country: String, epochMillis: Long, lines: Seq[(Int, Long)]): Order =
    Order(
      id = OrderId(id),
      customerId = CustomerId("cust-0001"),
      lines = lines.map { case (quantity, cents) => OrderLine(Sku("SKU-MUG"), quantity, Money.eur(cents)) }.toList,
      placedAtEpochMillis = epochMillis,
      country = country
    )

  /** Turns a revenue row into a compact tuple: window start as `HH:mm`, country, revenue, order count. */
  def windowSummary(row: Row): (String, String, Long, Long) = {
    val startMillis    = row.getAs[Timestamp]("windowStart").getTime
    val minutesIntoDay = (startMillis - DayStartEpochMillis) / 60000L
    val label          = f"${minutesIntoDay / 60}%02d:${minutesIntoDay % 60}%02d"
    (label, row.getAs[String]("country"), row.getAs[Long]("revenueCents"), row.getAs[Long]("orderCount"))
  }
}
