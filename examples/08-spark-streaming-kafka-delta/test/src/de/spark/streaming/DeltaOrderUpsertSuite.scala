package de.spark.streaming

import de.common.domain.Order
import de.common.json.Codecs
import org.apache.spark.sql.DataFrame

import java.nio.file.Files

/**
 * Tests the Delta Lake upsert against a table in a temporary local directory.
 *
 * Delta Lake is a file format plus a transaction log, not a server, so a "table" here is a folder. That is what makes
 * this test possible without any container: the exact same `MERGE` runs against `s3a://` in the docker stack.
 */
final class DeltaOrderUpsertSuite extends SparkSuite {

  import OrderStreamsSuite._

  test("the second version of an order replaces the first instead of adding a row") {
    val tablePath = Files.createTempDirectory("de-08-orders-table").resolve("orders").toString
    val upsert    = DeltaOrderUpsert.intoTable(tablePath)

    upsert(
      parsed(
        orderAt("order-1", "DE", at("12:01"), Seq((1, 1000L))),
        orderAt("order-2", "PL", at("12:02"), Seq((1, 300L)))
      ),
      0L
    )
    assertEquals(storedTotals(tablePath), Map("order-1" -> 1000L, "order-2" -> 300L))

    // A corrected order-1 arrives in a later micro-batch.
    upsert(parsed(orderAt("order-1", "DE", at("12:05"), Seq((1, 1800L)))), 1L)
    assertEquals(storedTotals(tablePath), Map("order-1" -> 1800L, "order-2" -> 300L))
  }

  test("duplicates inside one micro-batch collapse to the newest version") {
    val tablePath = Files.createTempDirectory("de-08-orders-dupes").resolve("orders").toString

    DeltaOrderUpsert.intoTable(tablePath)(
      parsed(
        orderAt("order-1", "DE", at("12:01"), Seq((1, 1000L))),
        orderAt("order-1", "DE", at("12:03"), Seq((1, 1800L)))
      ),
      0L
    )

    assertEquals(storedTotals(tablePath), Map("order-1" -> 1800L))
  }

  private def parsed(orders: Order*): DataFrame = {
    import spark.implicits._
    OrderStreams.parseOrders(orders.map(Codecs.order).toDF("value"))
  }

  private def storedTotals(tablePath: String): Map[String, Long] =
    spark.read
      .format("delta")
      .load(tablePath)
      .collect()
      .map(row => row.getAs[String]("orderId") -> row.getAs[Long]("totalCents"))
      .toMap
}
