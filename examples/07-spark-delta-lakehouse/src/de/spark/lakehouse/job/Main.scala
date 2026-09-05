package de.spark.lakehouse.job

import de.spark.lakehouse.core.{LakehouseLayout, MedallionTransforms}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

/**
 * Runs the whole lakehouse pipeline once and narrates what Delta Lake did at each step.
 *
 * The file is deliberately a script: read configuration, build a session, call the pure transformations from
 * `de.spark.lakehouse.core`, hand the results to the storage adapter, print. All the reasoning lives in the functions
 * it calls, so a reader can follow the story here and drill into the logic only where it interests them.
 */
object Main {

  def main(args: Array[String]): Unit = {
    val config = JobConfig.fromEnvironment(sys.env)
    val spark  = SparkSessions.create("de-07-spark-delta-lakehouse", config)
    try run(spark, config)
    finally spark.stop()
  }

  private def run(spark: SparkSession, config: JobConfig): Unit = {
    val lake   = new DeltaLakehouse(spark)
    val layout = config.layout

    banner(s"Warehouse root: ${layout.root}")

    val bronzeCounts = landBronze(spark, lake, layout, config.orderCount)
    val silverCounts = buildSilver(spark, lake, layout)
    buildGold(spark, lake, layout)
    maintainCustomerDimension(spark, lake, layout)
    demonstrateSchemaRules(spark, lake, layout)
    demonstrateTimeTravel(lake, layout)
    demonstrateMaintenance(lake, layout)
    showTransactionLog(lake, layout)

    banner("Summary")
    println(f"bronze orders landed (duplicates and broken rows included): ${bronzeCounts}%,d")
    println(f"silver orders after cleaning and de-duplication:            ${silverCounts}%,d")
    println(s"Browse the files under ${layout.root} to see what a lakehouse actually is on disk.")
  }

  /**
   * Step 1 - bronze. The raw feeds are appended exactly as they arrived. Nothing is filtered, because bronze is the
   * record of what the source systems sent and has to stay replayable.
   */
  private def landBronze(spark: SparkSession, lake: DeltaLakehouse, layout: LakehouseLayout, orderCount: Int): Long = {
    import spark.implicits._

    banner("Step 1 of 8 - bronze: land the raw feeds")
    val batch = SourceData.generate(orderCount, ingestedAtEpochMillis = System.currentTimeMillis())

    lake.overwrite(batch.orders.toDF(), layout.bronzeOrders)
    lake.overwrite(batch.payments.toDF(), layout.bronzePayments)
    lake.overwrite(batch.shipments.toDF(), layout.bronzeShipments)

    println(f"orders    ${batch.orders.size}%,d rows -> ${layout.bronzeOrders}")
    println(f"payments  ${batch.payments.size}%,d rows -> ${layout.bronzePayments}")
    println(f"shipments ${batch.shipments.size}%,d rows -> ${layout.bronzeShipments}")
    batch.orders.size.toLong
  }

  /** Step 2 - silver. The pure transformations do the work; this step only decides where the results are stored. */
  private def buildSilver(spark: SparkSession, lake: DeltaLakehouse, layout: LakehouseLayout): Long = {
    banner("Step 2 of 8 - silver: clean, de-duplicate and conform")

    val silverOrders    = MedallionTransforms.cleanOrders(spark.read.format("delta").load(layout.bronzeOrders))
    val silverPayments  = MedallionTransforms.cleanPayments(spark.read.format("delta").load(layout.bronzePayments))
    val silverShipments = MedallionTransforms.cleanShipments(spark.read.format("delta").load(layout.bronzeShipments))

    lake.overwrite(silverOrders, layout.silverOrders)
    lake.overwrite(silverPayments, layout.silverPayments)
    lake.overwrite(silverShipments, layout.silverShipments)

    val orderCount = spark.read.format("delta").load(layout.silverOrders).count()
    println(f"orders    $orderCount%,d rows -> ${layout.silverOrders}")
    println(s"payments  -> ${layout.silverPayments}")
    println(s"shipments -> ${layout.silverShipments}")
    orderCount
  }

  /** Step 3 - gold. Two business tables a dashboard could read directly. */
  private def buildGold(spark: SparkSession, lake: DeltaLakehouse, layout: LakehouseLayout): Unit = {
    banner("Step 3 of 8 - gold: business aggregates")

    val orders   = spark.read.format("delta").load(layout.silverOrders)
    val payments = spark.read.format("delta").load(layout.silverPayments)

    val dailyRevenue = MedallionTransforms.dailyRevenueByCountry(orders, payments)
    val lifetime     = MedallionTransforms.customerLifetimeValue(orders, payments)

    lake.overwrite(dailyRevenue, layout.goldDailyRevenue)
    lake.overwrite(lifetime, layout.goldCustomerLifetime)

    println("daily revenue per country (first rows):")
    spark.read.format("delta").load(layout.goldDailyRevenue).show(10, truncate = false)

    println("highest customer lifetime value:")
    spark.read.format("delta").load(layout.goldCustomerLifetime).show(5, truncate = false)
  }

  /**
   * Step 4 - `MERGE INTO`. The customer dimension is built once, then a batch of corrections is merged in. Watch the
   * row count: corrections update existing customers instead of adding new ones.
   */
  private def maintainCustomerDimension(spark: SparkSession, lake: DeltaLakehouse, layout: LakehouseLayout): Unit = {
    import spark.implicits._

    banner("Step 4 of 8 - MERGE INTO: a slowly changing customer dimension")

    val silverOrders = spark.read.format("delta").load(layout.silverOrders)
    val initial      = MedallionTransforms.customerDimension(silverOrders)
    val afterInitial = lake.mergeCustomerDimension(initial, layout.silverCustomers)
    println(f"after the first load the dimension holds $afterInitial%,d customers")

    val correctedOrders = MedallionTransforms.cleanOrders(
      SourceData.corrections(orderCount = 10, ingestedAtEpochMillis = System.currentTimeMillis()).toDF()
    )
    val correctionRows = MedallionTransforms.customerDimension(correctedOrders)
    val afterMerge     = lake.mergeCustomerDimension(correctionRows, layout.silverCustomers)

    println(f"after merging ${correctionRows.count()}%,d corrections it holds $afterMerge%,d customers")
    println("the merged customers now report the corrected country:")
    spark.read
      .format("delta")
      .load(layout.silverCustomers)
      .where(col("country") === "NL")
      .show(10, truncate = false)
  }

  /**
   * Step 5 - schema enforcement, then schema evolution. First a write with an incompatible column is rejected, then the
   * same kind of change is accepted because it was asked for explicitly.
   */
  private def demonstrateSchemaRules(spark: SparkSession, lake: DeltaLakehouse, layout: LakehouseLayout): Unit = {
    import spark.implicits._

    banner("Step 5 of 8 - schema enforcement and schema evolution")

    val extraColumn = SourceData
      .generate(orderCount = 5, ingestedAtEpochMillis = System.currentTimeMillis())
      .orders
      .toDF()
      .withColumn("sales_channel", lit("mobile-app"))

    val rejection = scala.util.Try(lake.append(extraColumn, layout.bronzeOrders))
    rejection match {
      case scala.util.Failure(error) =>
        println("schema enforcement rejected a write that introduced an unexpected column:")
        println(s"  ${firstLineOf(error.getMessage)}")
      case scala.util.Success(_) =>
        println("the write was accepted, which means the table already knew this column")
    }

    lake.appendWithSchemaEvolution(extraColumn, layout.bronzeOrders)
    val evolved = spark.read.format("delta").load(layout.bronzeOrders)
    println(s"after an explicit mergeSchema write the table has ${evolved.schema.fields.length} columns:")
    println(s"  ${evolved.columns.mkString(", ")}")
    println(f"rows carrying the new column: ${evolved.where(col("sales_channel").isNotNull).count()}%,d")
  }

  /** Step 6 - time travel. The same table, read as it was several commits ago. */
  private def demonstrateTimeTravel(lake: DeltaLakehouse, layout: LakehouseLayout): Unit = {
    banner("Step 6 of 8 - time travel")

    val newest = lake.latestVersion(layout.bronzeOrders)
    println(s"the bronze orders table is at version $newest")
    println("its commit history:")
    lake.history(layout.bronzeOrders).select("version", "operation").show(10, truncate = false)

    (0L to newest).foreach { version =>
      val rows = lake.readAtVersion(layout.bronzeOrders, version).count()
      println(f"  version $version%2d held $rows%,d rows")
    }

    // Travelling by timestamp uses the newest commit rather than the oldest on purpose. `timestampAsOf` returns the
    // latest version committed at or *before* the given moment, and some object stores record file modification times
    // with only whole-second precision, so an older commit's timestamp can round down to just before that commit and
    // resolve to nothing. The newest commit has no such edge, which keeps this demonstration honest on every backend.
    val newestCommitTime = lake.commitTimestampOf(layout.bronzeOrders, newest)
    val asOfNewestCommit = lake.readAtTimestamp(layout.bronzeOrders, newestCommitTime).count()
    println(f"reading with timestampAsOf $newestCommitTime returns $asOfNewestCommit%,d rows")
  }

  /** Step 7 - table maintenance: compact the small files, then delete the ones no version needs any more. */
  private def demonstrateMaintenance(lake: DeltaLakehouse, layout: LakehouseLayout): Unit = {
    banner("Step 7 of 8 - OPTIMIZE and VACUUM")

    val (before, after) = lake.optimize(layout.bronzeOrders)
    println(s"OPTIMIZE compacted the current version from $before data files to $after")

    lake.vacuum(layout.bronzeOrders, retentionHours = 0.0)
    println("VACUUM with zero retention removed every file no longer referenced by the current version")
    println("(zero retention is a demonstration setting; production keeps the default of seven days so that")
    println(" time travel and long-running readers keep working)")
  }

  /** Step 8 - the thing that makes all of the above possible. */
  private def showTransactionLog(lake: DeltaLakehouse, layout: LakehouseLayout): Unit = {
    banner("Step 8 of 8 - what is actually on disk")

    val logPath = layout.transactionLogOf(layout.bronzeOrders)
    println(s"$logPath contains:")
    lake.transactionLogFiles(logPath).foreach(name => println(s"  $name"))
    println("each numbered .json file is one commit, and its .crc sibling is a checksum of that commit;")
    println("every tenth commit also writes a .checkpoint.parquet that summarises the log so far, so a")
    println("reader does not have to replay it from the beginning")
  }

  private def firstLineOf(message: String): String =
    Option(message).map(_.linesIterator.next()).getOrElse("no message")

  private def banner(title: String): Unit = {
    println()
    println("=" * 100)
    println(title)
    println("=" * 100)
  }
}
