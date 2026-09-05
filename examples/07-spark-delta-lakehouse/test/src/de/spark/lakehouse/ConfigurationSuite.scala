package de.spark.lakehouse

import de.spark.lakehouse.core.LakehouseLayout
import de.spark.lakehouse.job.JobConfig
import munit.FunSuite

/**
 * Tests for the two pieces of the example that need no engine at all: where tables live, and how the job reads its
 * settings. Keeping these free of Apache Spark is the point - configuration mistakes should be catchable in
 * milliseconds.
 */
class ConfigurationSuite extends FunSuite {

  test("the layout places every table under the warehouse root, by layer") {
    val layout = LakehouseLayout("/tmp/warehouse")

    assertEquals(layout.bronzeOrders, "/tmp/warehouse/bronze/orders")
    assertEquals(layout.silverCustomers, "/tmp/warehouse/silver/customers")
    assertEquals(layout.goldDailyRevenue, "/tmp/warehouse/gold/daily_revenue_by_country")
    assertEquals(layout.transactionLogOf(layout.bronzeOrders), "/tmp/warehouse/bronze/orders/_delta_log")
  }

  test("a trailing slash on the warehouse root does not produce a doubled separator") {
    assertEquals(LakehouseLayout("s3a://lakehouse/warehouse/").bronzeOrders, "s3a://lakehouse/warehouse/bronze/orders")
  }

  test("with no environment the job runs against a local directory and no object store") {
    val config = JobConfig.fromEnvironment(Map.empty)

    assertEquals(config.layout.root, JobConfig.DefaultWarehouseRoot)
    assertEquals(config.objectStore, None)
    assertEquals(config.orderCount, JobConfig.DefaultOrderCount)
  }

  test("an s3a warehouse root switches on the object store settings") {
    val config = JobConfig.fromEnvironment(
      Map(
        "LAKEHOUSE_ROOT" -> "s3a://lakehouse/warehouse",
        "S3_ENDPOINT"    -> "http://minio:9000",
        "S3_ACCESS_KEY"  -> "reader",
        "S3_SECRET_KEY"  -> "secret"
      )
    )

    assertEquals(config.objectStore.map(_.endpoint), Some("http://minio:9000"))
    assertEquals(config.objectStore.map(_.accessKey), Some("reader"))
    assertEquals(config.objectStore.map(_.pathStyleAccess), Some(true))
  }

  test("an order count that is not a positive number falls back to the default") {
    assertEquals(
      JobConfig.fromEnvironment(Map("ORDER_COUNT" -> "not a number")).orderCount,
      JobConfig.DefaultOrderCount
    )
    assertEquals(JobConfig.fromEnvironment(Map("ORDER_COUNT" -> "-5")).orderCount, JobConfig.DefaultOrderCount)
    assertEquals(JobConfig.fromEnvironment(Map("ORDER_COUNT" -> " 25 ")).orderCount, 25)
  }
}
