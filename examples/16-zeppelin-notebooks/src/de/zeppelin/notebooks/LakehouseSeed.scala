package de.zeppelin.notebooks

import de.common.domain.Order
import de.common.gen.DataGenerator

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

/**
 * Builds the SQL that creates and fills the Delta Lake tables the notebooks read.
 *
 * Delta Lake is a table format: a folder of Apache Parquet files plus a transaction log that records which of those
 * files make up the current version of the table. Trino can write that format directly, so seeding the lakehouse needs
 * no Apache Spark job - only a stream of ordinary SQL statements over one JDBC connection.
 *
 * Every function here is pure: orders in, SQL strings out. That is what lets the unit tests assert on the exact
 * statements without a container anywhere in sight.
 */
object LakehouseSeed {

  /** Folder in the MinIO object store that the Docker Compose stack creates for the lakehouse. */
  val warehouseLocation = "s3://lakehouse/shop"

  val schemaName = "delta.shop"

  val ordersTable = s"$schemaName.orders"

  val orderLinesTable = s"$schemaName.order_lines"

  /**
   * Where a table's files go.
   *
   * Left to itself Trino invents a folder name with a random suffix (`orders-371e4947...`), which is exactly right for
   * a warehouse nobody reads by hand and exactly wrong here: the Apache Spark paragraph in the notebook loads the table
   * by path, so the path has to be predictable. Naming the location explicitly pins it.
   */
  def locationOf(table: String): String = s"$warehouseLocation/${table.substring(table.lastIndexOf('.') + 1)}"

  /** How many rows travel in one `INSERT`. Fewer, larger inserts mean fewer entries in the Delta transaction log. */
  private val rowsPerInsert = 100

  private val timestampFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC)

  /**
   * The complete seeding script, in the order it must run.
   *
   * Running the seeder twice has to leave the same lakehouse as running it once, and the way it gets there is worth a
   * word. The obvious `DROP TABLE` then `CREATE TABLE` does not work here: a Delta Lake table created at a location the
   * writer chose is an *external* table, so dropping it removes the catalogue entry and leaves the files, and the next
   * `CREATE TABLE` then refuses to write on top of them. Creating the tables only if they are missing and emptying them
   * with `DELETE` avoids that entirely.
   */
  def script(orders: List[Order]): List[String] =
    List(
      s"CREATE SCHEMA IF NOT EXISTS $schemaName WITH (location = '$warehouseLocation')",
      createOrdersTable,
      createOrderLinesTable,
      s"DELETE FROM $ordersTable",
      s"DELETE FROM $orderLinesTable"
    ) ++ inserts(ordersTable, orders.map(orderRow)) ++ inserts(orderLinesTable, orders.flatMap(orderLineRows))

  val createOrdersTable: String =
    s"""CREATE TABLE IF NOT EXISTS $ordersTable (
       |  order_id varchar,
       |  customer_id varchar,
       |  country varchar,
       |  placed_at timestamp(3),
       |  line_count integer,
       |  total_cents bigint,
       |  currency varchar
       |) WITH (location = '${locationOf(ordersTable)}')""".stripMargin

  val createOrderLinesTable: String =
    s"""CREATE TABLE IF NOT EXISTS $orderLinesTable (
       |  order_id varchar,
       |  sku varchar,
       |  quantity integer,
       |  unit_price_cents bigint
       |) WITH (location = '${locationOf(orderLinesTable)}')""".stripMargin

  /** Groups already-rendered `VALUES` tuples into a handful of `INSERT` statements. */
  def inserts(table: String, rows: List[String]): List[String] =
    rows.grouped(rowsPerInsert).map(batch => s"INSERT INTO $table VALUES\n${batch.mkString(",\n")}").toList

  /** One `VALUES` tuple describing an order as a whole. */
  def orderRow(order: Order): String = {
    val total = order.total
    tuple(
      List(
        quoted(order.id.value),
        quoted(order.customerId.value),
        quoted(order.country),
        s"TIMESTAMP '${formatTimestamp(order.placedAtEpochMillis)}'",
        order.lines.size.toString,
        total.cents.toString,
        quoted(total.currency)
      )
    )
  }

  /** One `VALUES` tuple per line of an order, so the notebooks can group revenue by article number. */
  def orderLineRows(order: Order): List[String] =
    order.lines.map { line =>
      tuple(
        List(
          quoted(order.id.value),
          quoted(line.sku.value),
          line.quantity.toString,
          line.unitPrice.cents.toString
        )
      )
    }

  def formatTimestamp(epochMillis: Long): String = timestampFormat.format(Instant.ofEpochMilli(epochMillis))

  /** Where the generated history starts: 2023-11-14 in Coordinated Universal Time. */
  val historyStartEpochMillis = 1700000000000L

  private val dayMillis = 86400000L

  /**
   * The deterministic order history this example loads.
   *
   * The shared generator advances its clock by at most a second per event, so a single run of it produces orders that
   * all fall inside the same quarter of an hour - fine for a streaming example, useless for a chart with a date axis.
   * Running one generator per simulated day, each with its own seed and its own starting timestamp, spreads the orders
   * across `days` calendar days while staying completely reproducible.
   */
  def sampleOrders(days: Int, ordersPerDay: Int): List[Order] =
    (0 until days).toList.flatMap { day =>
      new DataGenerator(seed = 42L + day, startEpochMillis = historyStartEpochMillis + day * dayMillis)
        .orders(ordersPerDay)
    }

  private def tuple(cells: List[String]): String = cells.mkString("  (", ", ", ")")

  /** A SQL string literal. A quote inside the text is escaped by doubling it, as the SQL standard prescribes. */
  private def quoted(text: String): String = "'" + text.replace("'", "''") + "'"
}
