package de.spark.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

/**
 * Wires the pure transformations in [[OrderStreams]] to the outside world: Apache Kafka on the way in, Delta Lake on
 * the way out.
 *
 * This is the only file that knows a broker or a table path exists. Everything it does is plumbing, which is exactly
 * the point - the interesting logic sits in functions that can be tested without any of it.
 */
object StreamingJob {

  /** Name of the query that appends raw orders to the orders table. */
  val OrdersQueryName = "orders-to-delta"

  /** Name of the query that maintains the windowed revenue table. */
  val RevenueQueryName = "revenue-by-window"

  /**
   * Opens the Kafka source.
   *
   * `startingOffsets` only applies to a query that has never run before. As soon as a checkpoint directory exists,
   * Spark reads the offsets it committed there and continues from them, ignoring this setting entirely. That is the
   * behaviour you want - it is what lets you stop the job, deploy a new version and restart without re-reading or
   * skipping data - but it also explains a classic confusion: changing `startingOffsets` to `"earliest"` on an existing
   * job appears to do nothing. Delete the checkpoint to actually replay.
   *
   * `failOnDataLoss` stays at its default of `true`: if Kafka has already deleted records the checkpoint still points
   * at (retention expired while the job was down), the query fails loudly instead of quietly skipping orders.
   */
  def readOrdersTopic(spark: SparkSession, config: JobConfig): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.bootstrapServers)
      .option("subscribe", config.ordersTopic)
      .option("startingOffsets", config.startingOffsets)
      .load()

  /**
   * The upsert query: every order ends up in the orders table exactly once, keyed by its id.
   *
   * The output mode is not configurable here, and cannot be: `foreachBatch` receives whatever the query produces, and
   * for a query without aggregation that is `append` - each input row is emitted once, when it is read. Delta then
   * decides through `MERGE` whether that row becomes an insert or an update.
   */
  def startOrdersUpsert(orders: DataFrame, config: JobConfig): StreamingQuery =
    orders.writeStream
      .queryName(OrdersQueryName)
      .foreachBatch(DeltaOrderUpsert.intoTable(config.ordersTablePath))
      .option("checkpointLocation", config.checkpointFor(OrdersQueryName))
      .trigger(Trigger.ProcessingTime(config.triggerInterval))
      .start()

  /**
   * The aggregation query: revenue per country per event-time window.
   *
   * Choosing the output mode is the decision that trips people up most often, so it is worth spelling out what the
   * three of them mean for *this* query:
   *
   *   - `complete` rewrites the entire result table on every batch. It is the only mode that works without a watermark,
   *     and it is unusable in a long-running stream because the result grows for as long as the job runs;
   *   - `update` emits every window whose value changed in this batch, including windows that are still open. A
   *     dashboard sees a five-minute window's revenue rise as orders trickle in. The row for a window is written
   *     several times, so the sink has to be able to overwrite by key;
   *   - `append` emits a window exactly once, and only when the watermark has moved past its end so the value can no
   *     longer change. Nothing is ever rewritten, which is why it is the only mode a plain append-only Delta table can
   *     accept - at the cost of the result appearing one watermark late.
   *
   * This query uses `append`, because the table is meant to be a durable, immutable record of closed windows. Switch
   * the constant below to `"update"` and the table would need to be written with a merge, exactly like the orders table
   * above.
   */
  def startRevenueAggregation(orders: DataFrame, config: JobConfig): StreamingQuery = {
    val revenue = OrderStreams.revenuePerWindow(orders, config.windowDuration, config.watermarkDelay)

    revenue.writeStream
      .queryName(RevenueQueryName)
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", config.checkpointFor(RevenueQueryName))
      .option("path", config.revenueTablePath)
      .trigger(Trigger.ProcessingTime(config.triggerInterval))
      .start()
  }

  /** Starts both queries against the same parsed order stream. */
  def start(spark: SparkSession, config: JobConfig): Seq[StreamingQuery] = {
    val orders = OrderStreams.parseOrders(readOrdersTopic(spark, config))
    Seq(startOrdersUpsert(orders, config), startRevenueAggregation(orders, config))
  }
}
