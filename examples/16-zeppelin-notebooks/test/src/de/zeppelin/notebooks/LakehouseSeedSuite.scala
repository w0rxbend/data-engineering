package de.zeppelin.notebooks

import de.common.domain.{CustomerId, Money, Order, OrderId, OrderLine, Sku}

import scala.collection.mutable.ListBuffer

final class LakehouseSeedSuite extends munit.FunSuite {

  private val order = Order(
    id = OrderId("order-0000001"),
    customerId = CustomerId("cust-0042"),
    lines = List(OrderLine(Sku("SKU-MUG"), 2, Money.eur(750))),
    placedAtEpochMillis = 1700000000000L,
    country = "DE"
  )

  test("an order row carries its identity, its size and its total in cents") {
    assertEquals(
      LakehouseSeed.orderRow(order),
      "  ('order-0000001', 'cust-0042', 'DE', TIMESTAMP '2023-11-14 22:13:20.000', 1, 1500, 'EUR')"
    )
  }

  test("an order contributes one row per line to the line table") {
    val twoLines = order.copy(lines = order.lines :+ OrderLine(Sku("SKU-KETTLE"), 1, Money.eur(4200)))
    assertEquals(
      LakehouseSeed.orderLineRows(twoLines),
      List("  ('order-0000001', 'SKU-MUG', 2, 750)", "  ('order-0000001', 'SKU-KETTLE', 1, 4200)")
    )
  }

  test("timestamps are rendered in UTC with millisecond precision") {
    assertEquals(LakehouseSeed.formatTimestamp(0L), "1970-01-01 00:00:00.000")
  }

  test("a quote inside a value is escaped by doubling it") {
    val awkward = order.copy(customerId = CustomerId("o'brien"))
    assert(LakehouseSeed.orderRow(awkward).contains("'o''brien'"), LakehouseSeed.orderRow(awkward))
  }

  test("the script creates the tables only if missing and empties them, so re-seeding is safe") {
    val script = LakehouseSeed.script(List(order))
    assert(script.head.startsWith(s"CREATE SCHEMA IF NOT EXISTS ${LakehouseSeed.schemaName}"), script.head)
    assert(script(1).startsWith(s"CREATE TABLE IF NOT EXISTS ${LakehouseSeed.ordersTable}"), script(1))
    assert(script(2).startsWith(s"CREATE TABLE IF NOT EXISTS ${LakehouseSeed.orderLinesTable}"), script(2))
    assertEquals(script(3), s"DELETE FROM ${LakehouseSeed.ordersTable}")
    assertEquals(script(4), s"DELETE FROM ${LakehouseSeed.orderLinesTable}")
    assert(script(5).startsWith(s"INSERT INTO ${LakehouseSeed.ordersTable} VALUES"), script(5))
  }

  test("each table gets an explicit, predictable location, because the notebook loads it by path") {
    assertEquals(LakehouseSeed.locationOf(LakehouseSeed.ordersTable), "s3://lakehouse/shop/orders")
    assert(
      LakehouseSeed.createOrdersTable.endsWith("WITH (location = 's3://lakehouse/shop/orders')"),
      LakehouseSeed.createOrdersTable
    )
  }

  test("rows are inserted in batches rather than one statement per row") {
    assertEquals(LakehouseSeed.inserts("t", List.fill(250)("  (1)")).size, 3)
  }

  test("the generated sample is deterministic, so every run seeds the same lakehouse") {
    assertEquals(LakehouseSeed.sampleOrders(3, 10), LakehouseSeed.sampleOrders(3, 10))
  }

  test("the history spans one calendar day per simulated day, so a date axis has something to show") {
    val days = LakehouseSeed
      .sampleOrders(5, 20)
      .map(order => (order.placedAtEpochMillis - LakehouseSeed.historyStartEpochMillis) / 86400000L)
      .distinct
    assertEquals(days, List(0L, 1L, 2L, 3L, 4L))
  }

  test("running the script sends every statement to the session exactly once") {
    val executed            = ListBuffer.empty[String]
    val session: SqlSession = statement => executed += statement
    val script              = LakehouseSeed.script(LakehouseSeed.sampleOrders(2, 5))
    script.foreach(session.run)
    assertEquals(executed.toList, script)
  }
}
