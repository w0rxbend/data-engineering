package de.trino.lakehouse

import de.common.domain.Order
import de.common.gen.DataGenerator

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

/**
 * Builds the SQL that creates and fills the Delta Lake table holding the shop's orders.
 *
 * Trino can write Delta Lake tables itself, so the lakehouse side of this example needs no Apache Spark and no
 * pre-existing files: the statements produced here are ordinary SQL sent over the same JDBC connection as the queries.
 *
 * Every function is pure - orders in, SQL strings out - so the exact statements are asserted on in the unit tests and
 * no cluster is needed to check them.
 */
object DeltaSeed {

  /** Bucket in the MinIO object store that the Docker Compose stack creates for the lakehouse. */
  val warehouseLocation = "s3://lakehouse/shop"

  val schemaName = "delta.shop"

  val tableName = s"$schemaName.orders"

  /** How many rows go into one `INSERT`. Fewer, larger inserts mean fewer Delta transaction log entries. */
  private val rowsPerInsert = 50

  private val timestampFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC)

  /**
   * The full seeding script: create the schema, replace the table, then load the orders.
   *
   * `DROP TABLE IF EXISTS` makes running the seeder twice produce the same result as running it once, which is what
   * makes the example safe to re-run while experimenting.
   */
  def script(orders: List[Order]): List[String] =
    List(
      s"CREATE SCHEMA IF NOT EXISTS $schemaName WITH (location = '$warehouseLocation')",
      s"DROP TABLE IF EXISTS $tableName",
      createTable
    ) ++ insertStatements(orders)

  val createTable: String =
    s"""CREATE TABLE $tableName (
       |  order_id varchar,
       |  customer_id varchar,
       |  country varchar,
       |  placed_at timestamp(3),
       |  line_count integer,
       |  total_cents bigint,
       |  currency varchar
       |)""".stripMargin

  def insertStatements(orders: List[Order]): List[String] =
    orders
      .grouped(rowsPerInsert)
      .map(batch => s"INSERT INTO $tableName VALUES\n${batch.map(row).mkString(",\n")}")
      .toList

  /** One `VALUES` tuple for an order. */
  def row(order: Order): String = {
    val total = order.total
    val cells = List(
      quoted(order.id.value),
      quoted(order.customerId.value),
      quoted(order.country),
      s"TIMESTAMP '${formatTimestamp(order.placedAtEpochMillis)}'",
      order.lines.size.toString,
      total.cents.toString,
      quoted(total.currency)
    )
    cells.mkString("  (", ", ", ")")
  }

  def formatTimestamp(epochMillis: Long): String = timestampFormat.format(Instant.ofEpochMilli(epochMillis))

  /** SQL string literal. A quote inside the text is escaped by doubling it, as the SQL standard prescribes. */
  private def quoted(text: String): String = "'" + text.replace("'", "''") + "'"

  /** The deterministic batch of orders this example loads, so every run of the seeder produces the same table. */
  def sampleOrders(count: Int): List[Order] = new DataGenerator().orders(count)
}
