package de.spark.lakehouse.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * The business logic of the lakehouse, expressed as pure functions from `DataFrame` to `DataFrame`.
 *
 * "Pure" here means: no file paths, no storage credentials, no Delta Lake calls, no `SparkSession` created inside.
 * Every function takes data in and hands data back, which is what makes the whole pipeline testable with a throwaway
 * Apache Spark session and a handful of in-memory rows. All the wiring - where the tables live, which object store to
 * talk to, when to commit - lives in the `job` package instead.
 *
 * The vocabulary comes from the medallion architecture, the most common way to organise a lakehouse:
 *
 *   - '''bronze''' is raw data landed exactly as it arrived, warts and all. Duplicates and rubbish rows stay, because
 *     bronze is the replayable record of what the source systems actually sent.
 *   - '''silver''' is cleaned and conformed: one row per business key, validated values, tidy column names.
 *   - '''gold''' is aggregated for consumption: the numbers a dashboard or a finance report reads.
 *
 * Column names change on the way from bronze to silver: bronze mirrors the producing system (`camelCase`, because it
 * comes straight from Scala case classes) while silver and gold use `snake_case`, the convention analysts and SQL tools
 * expect.
 */
object MedallionTransforms {

  /** Payment states that mean money actually moved. A merely authorised payment has not been collected yet. */
  private val CollectedPaymentStatuses = Seq("Captured")

  /**
   * Cleans the raw orders table.
   *
   * Three things happen, in this order:
   *
   *   1. rows that cannot be used are dropped - a missing order or customer identifier, or a non-positive total, means
   *      the record is broken rather than merely unusual;
   *   1. duplicates are collapsed. The same order can land twice because an upstream producer retried, so the row with
   *      the newest `ingestedAtEpochMillis` wins and the older copy is discarded;
   *   1. columns are renamed to `snake_case` and a real `order_date` is derived from the epoch-millisecond timestamp so
   *      that downstream aggregation can group by day without repeating the date arithmetic.
   */
  def cleanOrders(bronzeOrders: DataFrame): DataFrame = {
    val usable = bronzeOrders
      .where(col("orderId").isNotNull && length(trim(col("orderId"))) > 0)
      .where(col("customerId").isNotNull && length(trim(col("customerId"))) > 0)
      .where(col("totalCents") > 0)

    deduplicateByKey(usable, keyColumn = "orderId")
      .select(
        col("orderId").as("order_id"),
        col("customerId").as("customer_id"),
        upper(col("country")).as("country"),
        col("lineCount").as("line_count"),
        col("totalCents").as("total_cents"),
        col("currency").as("currency"),
        col("placedAtEpochMillis").as("placed_at_epoch_millis"),
        toUtcDate(col("placedAtEpochMillis")).as("order_date"),
        col("ingestedAtEpochMillis").as("ingested_at_epoch_millis")
      )
  }

  /**
   * Cleans the raw payments table.
   *
   * An order can produce several payment events over its life (authorised, then captured, or declined). Silver keeps
   * only the most recent one per order, which is the current state of that payment. Unknown status strings are dropped
   * rather than passed on, so a typo upstream cannot quietly become a new category in a report.
   */
  def cleanPayments(bronzePayments: DataFrame): DataFrame = {
    val knownStatuses = de.common.domain.PaymentStatus.all.map(_.toString)

    val usable = bronzePayments
      .where(col("orderId").isNotNull && length(trim(col("orderId"))) > 0)
      .where(col("status").isin(knownStatuses: _*))

    latestEventPerKey(usable, keyColumn = "orderId", eventTimeColumn = "occurredAtEpochMillis")
      .select(
        col("orderId").as("order_id"),
        col("amountCents").as("amount_cents"),
        col("currency").as("currency"),
        col("status").as("payment_status"),
        col("occurredAtEpochMillis").as("paid_at_epoch_millis")
      )
  }

  /**
   * Cleans the raw shipments table down to the latest known milestone per order, which answers "where is this parcel
   * right now" without the caller having to sift through the history.
   */
  def cleanShipments(bronzeShipments: DataFrame): DataFrame = {
    val knownStatuses = de.common.domain.ShipmentStatus.all.map(_.toString)

    val usable = bronzeShipments
      .where(col("orderId").isNotNull && length(trim(col("orderId"))) > 0)
      .where(col("status").isin(knownStatuses: _*))

    latestEventPerKey(usable, keyColumn = "orderId", eventTimeColumn = "occurredAtEpochMillis")
      .select(
        col("orderId").as("order_id"),
        col("status").as("shipment_status"),
        col("occurredAtEpochMillis").as("shipped_at_epoch_millis")
      )
  }

  /**
   * Gold table one: how much money the shop actually collected, per calendar day and per country.
   *
   * Only captured payments count. An authorised payment is a promise, a declined one is a failure, and counting either
   * as revenue is the classic way a lakehouse ends up disagreeing with the finance department.
   */
  def dailyRevenueByCountry(silverOrders: DataFrame, silverPayments: DataFrame): DataFrame =
    collectedOrders(silverOrders, silverPayments)
      .groupBy(col("order_date"), col("country"))
      .agg(
        sum(col("amount_cents")).as("revenue_cents"),
        count(lit(1)).as("order_count"),
        countDistinct(col("customer_id")).as("customer_count")
      )
      .orderBy(col("order_date"), col("country"))

  /**
   * Gold table two: customer lifetime value, the total amount a customer has ever paid us.
   *
   * Alongside the total it reports how many orders the customer placed and the window between their first and last
   * order, the three numbers a retention analysis starts from.
   */
  def customerLifetimeValue(silverOrders: DataFrame, silverPayments: DataFrame): DataFrame =
    collectedOrders(silverOrders, silverPayments)
      .groupBy(col("customer_id"))
      .agg(
        sum(col("amount_cents")).as("lifetime_value_cents"),
        count(lit(1)).as("order_count"),
        min(col("order_date")).as("first_order_date"),
        max(col("order_date")).as("last_order_date")
      )
      .withColumn(
        "average_order_value_cents",
        (col("lifetime_value_cents") / col("order_count")).cast("long")
      )
      .orderBy(col("lifetime_value_cents").desc, col("customer_id"))

  /**
   * The customer dimension: one row per customer describing who they are right now.
   *
   * A dimension is not an aggregate for a report, it is a lookup table that other tables join against, and it is
   * maintained incrementally: today's batch of orders produces a small set of rows that is merged into the existing
   * dimension rather than replacing it. `last_seen_epoch_millis` is what makes that merge safe, because it lets the
   * merge condition reject a batch that is older than what the table already holds.
   */
  def customerDimension(silverOrders: DataFrame): DataFrame =
    silverOrders
      .groupBy(col("customer_id"))
      .agg(
        max_by(col("country"), col("placed_at_epoch_millis")).as("country"),
        count(lit(1)).as("order_count"),
        sum(col("total_cents")).as("total_ordered_cents"),
        max(col("placed_at_epoch_millis")).as("last_seen_epoch_millis")
      )

  /**
   * The join both gold tables share: silver orders enriched with the payment that settled them, restricted to the
   * payments where money genuinely changed hands.
   */
  private def collectedOrders(silverOrders: DataFrame, silverPayments: DataFrame): DataFrame =
    silverOrders
      .join(
        silverPayments.where(col("payment_status").isin(CollectedPaymentStatuses: _*)),
        Seq("order_id"),
        "inner"
      )

  /**
   * Keeps one row per key: the copy that was ingested most recently.
   *
   * This is a window function rather than a `groupBy`. `groupBy` would collapse the rows and force every column to be
   * wrapped in an aggregate; a window numbers the rows within each key while leaving them intact, so keeping row number
   * one preserves the whole record. The event timestamp is a tie-break so the result is deterministic even when two
   * copies were ingested in the same millisecond.
   */
  private def deduplicateByKey(input: DataFrame, keyColumn: String): DataFrame = {
    val newestFirst = Window
      .partitionBy(col(keyColumn))
      .orderBy(col("ingestedAtEpochMillis").desc, col("placedAtEpochMillis").desc)

    input
      .withColumn("copy_number", row_number().over(newestFirst))
      .where(col("copy_number") === 1)
      .drop("copy_number")
  }

  /** Keeps the latest event per key by business event time, the "current state" of a multi-step process. */
  private def latestEventPerKey(input: DataFrame, keyColumn: String, eventTimeColumn: String): DataFrame = {
    val newestFirst = Window.partitionBy(col(keyColumn)).orderBy(col(eventTimeColumn).desc)

    input
      .withColumn("event_number", row_number().over(newestFirst))
      .where(col("event_number") === 1)
      .drop("event_number")
  }

  /**
   * Turns milliseconds since 1970-01-01 into a calendar date.
   *
   * `timestamp_millis` interprets the number as an instant, and `to_date` then asks which calendar day that instant
   * falls on. Which day that is depends on a time zone, and Apache Spark uses the session time zone for the answer.
   * Both the job and the tests pin `spark.sql.session.timeZone` to Coordinated Universal Time (UTC) so that the day a
   * sale is booked on never depends on where the machine running the job happens to be.
   */
  private def toUtcDate(epochMillis: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    to_date(timestamp_millis(epochMillis))
}
