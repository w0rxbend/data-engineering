package de.spark.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * The pure core of this example.
 *
 * Every function here takes a `DataFrame` and returns a `DataFrame`. None of them reads from Apache Kafka, writes to
 * Delta Lake or starts a query, which is what makes them testable: `OrderStreamsSuite` feeds them an in-memory stream
 * and asserts on the result without a single container running. All the wiring to the outside world lives in
 * [[StreamingJob]].
 *
 * The same function works on a streaming `DataFrame` and on a batch one - in Apache Spark both are described by the
 * identical API, and only the source and the sink decide which of the two you get.
 */
object OrderStreams {

  /** Column name of the event-time timestamp derived from the order's `placedAt` field. */
  val EventTimeColumn = "placedAt"

  /**
   * Turns the raw Kafka records into typed order rows.
   *
   * A record read from Kafka arrives as a row with fixed columns; the payload sits in a binary column named `value`.
   * The three steps below are the standard shape of every Kafka-to-anything Spark job:
   *
   *   1. cast `value` from bytes to a string, because the producers in this repository write UTF-8 JSON;
   *   1. `from_json` parses that string against [[OrderSchema.order]]. A record that does not fit the schema becomes
   *      `null` rather than failing the whole query - `from_json` defaults to `PERMISSIVE` mode;
   *   1. drop the records that did not parse, so one malformed message cannot poison the table. In production the
   *      dropped rows would go to a "dead letter" table instead of being discarded.
   *
   * The third step needs more care than it first appears. Permissive mode has two distinct failure shapes: text that is
   * not JSON at all yields a `null` struct, while valid JSON that simply lacks the expected fields yields a struct
   * whose fields are all `null`. Checking the struct itself would let the second shape through and write a row with a
   * `null` order id into the table, so the filter checks the fields the rest of the job cannot work without.
   *
   * The order total is computed with the SQL `aggregate` higher-order function, which folds over the `lines` array
   * inside the row. Doing it in SQL rather than in Scala keeps the work inside Spark's optimizer and avoids
   * deserializing every row into a JVM object.
   */
  def parseOrders(kafkaRecords: DataFrame): DataFrame = {
    val parsed = kafkaRecords
      .select(from_json(col("value").cast("string"), OrderSchema.order).as("order"))
      .filter(col("order.id").isNotNull && col("order.placedAt").isNotNull)

    parsed.select(
      col("order.id").as("orderId"),
      col("order.customerId").as("customerId"),
      col("order.country").as("country"),
      expr("aggregate(order.lines, 0L, (acc, line) -> acc + line.unitPrice.cents * line.quantity)")
        .as("totalCents"),
      element_at(col("order.lines"), 1).getField("unitPrice").getField("currency").as("currency"),
      size(col("order.lines")).as("lineCount"),
      // Event time: when the order actually happened, as recorded by the shop.
      // It is NOT the time Spark read the record, and that distinction is the
      // entire reason watermarks exist.
      expr("timestamp_millis(order.placedAt)").as(EventTimeColumn)
    )
  }

  /**
   * Revenue and order count per country, per fixed event-time window.
   *
   * Two ideas are at work here.
   *
   * `withWatermark` tells Spark how long to wait for stragglers. With a ten-minute watermark, once Spark has seen an
   * order stamped 12:40 it assumes no order older than 12:30 will still arrive, so every window that ended before 12:30
   * can be finalised and its state dropped. Without a watermark, Spark would have to keep the state of every window it
   * has ever seen, forever, and memory would grow without bound. The price is that an order arriving later than the
   * watermark allows is silently ignored, so the delay is a direct trade between completeness and cost.
   *
   * `window` buckets each row into a fixed, non-overlapping slot of `windowDuration`. An order stamped 12:07 lands in
   * the 12:05-12:10 window regardless of when Spark received it, which is what makes the result reproducible: replay
   * the same topic tomorrow and you get exactly the same numbers.
   *
   * @param orders
   *   rows produced by [[parseOrders]]
   * @param windowDuration
   *   width of one window, in Spark interval syntax, for example `"5 minutes"`
   * @param watermarkDelay
   *   how late an order may be and still be counted, for example `"10 minutes"`
   */
  def revenuePerWindow(orders: DataFrame, windowDuration: String, watermarkDelay: String): DataFrame =
    orders
      .withWatermark(EventTimeColumn, watermarkDelay)
      .groupBy(window(col(EventTimeColumn), windowDuration), col("country"))
      .agg(
        sum("totalCents").as("revenueCents"),
        count(lit(1)).as("orderCount")
      )
      .select(
        col("window.start").as("windowStart"),
        col("window.end").as("windowEnd"),
        col("country"),
        col("revenueCents"),
        col("orderCount")
      )

  /**
   * Keeps the newest row per order id.
   *
   * Kafka guarantees that a record is delivered at least once, and a shop may legitimately republish a corrected order
   * under the same id. Both cases produce several rows with the same `orderId` inside a single micro-batch. A Delta
   * `MERGE` requires each source row to match at most one target row, so the duplicates have to be collapsed *before*
   * the merge runs - otherwise Delta fails the batch with a "multiple source rows matched" error.
   */
  def deduplicateByOrderId(orders: DataFrame): DataFrame = {
    val latest = orders
      .groupBy(col("orderId"))
      .agg(max(col(EventTimeColumn)).as(EventTimeColumn))
    orders.join(latest, Seq("orderId", EventTimeColumn), "leftsemi").dropDuplicates("orderId")
  }
}
