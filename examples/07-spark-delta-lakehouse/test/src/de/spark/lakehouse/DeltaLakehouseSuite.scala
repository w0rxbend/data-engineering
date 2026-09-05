package de.spark.lakehouse

import de.spark.lakehouse.core.BronzeRecords.OrderRow
import de.spark.lakehouse.core.MedallionTransforms
import de.spark.lakehouse.job.DeltaLakehouse
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

import scala.concurrent.duration._

/**
 * Tests for the Delta Lake features the example advertises: upserts, schema rules, time travel and maintenance.
 *
 * These write real Delta tables, but only into a temporary directory on the local disk. Delta Lake does not care
 * whether the path is a local folder or object storage, so the same assertions hold when the warehouse lives in MinIO -
 * which is exactly why no container is needed to test them.
 */
class DeltaLakehouseSuite extends SparkSuite {

  // Real Delta maintenance runs several Spark jobs and can exceed MUnit's 30-second default on CI runners.
  override def munitTimeout: Duration = 2.minutes

  private def order(
      orderId: String,
      customerId: String = "cust-1",
      country: String = "DE",
      totalCents: Long = 1000L,
      placedAt: Long = 1700000000000L
  ): OrderRow =
    OrderRow(orderId, customerId, country, 1, totalCents, "EUR", placedAt, 1700000100000L)

  private def ordersFrame(rows: Seq[OrderRow]): DataFrame = {
    import spark.implicits._
    rows.toDF()
  }

  private def dimensionOf(rows: Seq[OrderRow]): DataFrame =
    MedallionTransforms.customerDimension(MedallionTransforms.cleanOrders(ordersFrame(rows)))

  tempDirectory.test("an append is visible as a new version and the old version stays readable") { directory =>
    val lake = new DeltaLakehouse(spark)
    val path = directory.resolve("orders").toString

    lake.overwrite(ordersFrame(Seq(order("order-1"))), path)
    lake.append(ordersFrame(Seq(order("order-2"))), path)

    assertEquals(lake.latestVersion(path), 1L)
    assertEquals(lake.readAtVersion(path, 0).count(), 1L)
    assertEquals(lake.readAtVersion(path, 1).count(), 2L)
  }

  tempDirectory.test("timestampAsOf resolves to the version committed at that moment") { directory =>
    val lake = new DeltaLakehouse(spark)
    val path = directory.resolve("orders").toString

    lake.overwrite(ordersFrame(Seq(order("order-1"))), path)
    lake.append(ordersFrame(Seq(order("order-2"))), path)

    val firstCommitTime = lake.commitTimestampOf(path, version = 0L)

    assertEquals(lake.readAtTimestamp(path, firstCommitTime).count(), 1L)
  }

  tempDirectory.test("MERGE INTO updates existing customers instead of duplicating them") { directory =>
    val lake = new DeltaLakehouse(spark)
    val path = directory.resolve("customers").toString

    val initial = dimensionOf(Seq(order("order-1", customerId = "cust-1", country = "DE", placedAt = 1000L)))
    assertEquals(lake.mergeCustomerDimension(initial, path), 1L)

    val correction = dimensionOf(Seq(order("order-2", customerId = "cust-1", country = "NL", placedAt = 2000L)))
    val newArrival = dimensionOf(Seq(order("order-3", customerId = "cust-2", country = "PL", placedAt = 3000L)))

    assertEquals(lake.mergeCustomerDimension(correction, path), 1L)
    assertEquals(lake.mergeCustomerDimension(newArrival, path), 2L)

    val countries = spark.read
      .format("delta")
      .load(path)
      .collect()
      .map(row => row.getAs[String]("customer_id") -> row.getAs[String]("country"))
      .toMap

    assertEquals(countries, Map("cust-1" -> "NL", "cust-2" -> "PL"))
  }

  tempDirectory.test("MERGE INTO ignores a batch that is older than what the table already holds") { directory =>
    val lake = new DeltaLakehouse(spark)
    val path = directory.resolve("customers").toString

    lake.mergeCustomerDimension(dimensionOf(Seq(order("order-1", country = "NL", placedAt = 2000L))), path)
    lake.mergeCustomerDimension(dimensionOf(Seq(order("order-2", country = "DE", placedAt = 1000L))), path)

    assertEquals(spark.read.format("delta").load(path).head().getAs[String]("country"), "NL")
  }

  tempDirectory.test("schema enforcement rejects an unexpected column, mergeSchema accepts it") { directory =>
    val lake = new DeltaLakehouse(spark)
    val path = directory.resolve("orders").toString

    lake.overwrite(ordersFrame(Seq(order("order-1"))), path)
    val widened = ordersFrame(Seq(order("order-2"))).withColumn("sales_channel", lit("mobile-app"))

    intercept[Exception](lake.append(widened, path))
    assertEquals(spark.read.format("delta").load(path).columns.length, 8)

    lake.appendWithSchemaEvolution(widened, path)
    val evolved = spark.read.format("delta").load(path)

    assertEquals(evolved.columns.length, 9)
    assertEquals(evolved.count(), 2L)
    // The row written before the column existed is backfilled with null rather than rewritten.
    assertEquals(evolved.where(col("sales_channel").isNull).count(), 1L)
  }

  tempDirectory.test("OPTIMIZE compacts small files and VACUUM removes the ones no longer referenced") { directory =>
    val lake = new DeltaLakehouse(spark)
    val path = directory.resolve("orders").toString

    lake.overwrite(ordersFrame(Seq(order("order-1"))), path)
    (2 to 6).foreach(index => lake.append(ordersFrame(Seq(order(s"order-$index"))), path))

    val (before, after) = lake.optimize(path)
    assert(before > after, s"expected compaction to reduce the file count, went from $before to $after")

    lake.vacuum(path, retentionHours = 0.0)

    assertEquals(spark.read.format("delta").load(path).count(), 6L)
    assertEquals(lake.dataFileCount(path), after)
  }

  tempDirectory.test("every commit leaves a numbered file in the transaction log") { directory =>
    val lake = new DeltaLakehouse(spark)
    val path = directory.resolve("orders").toString

    lake.overwrite(ordersFrame(Seq(order("order-1"))), path)
    lake.append(ordersFrame(Seq(order("order-2"))), path)

    val commits = lake.transactionLogFiles(path + "/_delta_log").filter(_.endsWith(".json"))

    assertEquals(commits.take(2), Seq("00000000000000000000.json", "00000000000000000001.json"))
  }

  tempDirectory.test("the transaction log of a path that holds no table is empty") { directory =>
    val lake = new DeltaLakehouse(spark)

    assertEquals(lake.transactionLogFiles(directory.resolve("nothing/_delta_log").toString), Seq.empty[String])
  }
}
