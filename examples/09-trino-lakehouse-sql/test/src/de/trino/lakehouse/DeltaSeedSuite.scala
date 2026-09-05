package de.trino.lakehouse

import de.common.domain.{CustomerId, Money, Order, OrderId, OrderLine, Sku}

final class DeltaSeedSuite extends munit.FunSuite {

  private val order = Order(
    id = OrderId("order-0000001"),
    customerId = CustomerId("cust-0042"),
    lines = List(OrderLine(Sku("SKU-MUG"), 2, Money.eur(750))),
    placedAtEpochMillis = 1700000000000L,
    country = "DE"
  )

  test("a row carries the order identity, its size and its total in cents") {
    assertEquals(
      DeltaSeed.row(order),
      "  ('order-0000001', 'cust-0042', 'DE', TIMESTAMP '2023-11-14 22:13:20.000', 1, 1500, 'EUR')"
    )
  }

  test("timestamps are rendered in UTC with millisecond precision") {
    assertEquals(DeltaSeed.formatTimestamp(0L), "1970-01-01 00:00:00.000")
  }

  test("a quote inside a value is escaped by doubling it") {
    val awkward = order.copy(customerId = CustomerId("o'brien"))
    assert(DeltaSeed.row(awkward).contains("'o''brien'"), DeltaSeed.row(awkward))
  }

  test("the script creates the schema, replaces the table and then inserts") {
    val script = DeltaSeed.script(List(order))
    assert(script.head.startsWith(s"CREATE SCHEMA IF NOT EXISTS ${DeltaSeed.schemaName}"), script.head)
    assertEquals(script(1), s"DROP TABLE IF EXISTS ${DeltaSeed.tableName}")
    assert(script(2).startsWith(s"CREATE TABLE ${DeltaSeed.tableName}"), script(2))
    assert(script(3).startsWith(s"INSERT INTO ${DeltaSeed.tableName} VALUES"), script(3))
  }

  test("orders are inserted in batches rather than one statement per row") {
    val inserts = DeltaSeed.insertStatements(DeltaSeed.sampleOrders(120))
    assertEquals(inserts.size, 3)
    assertEquals(inserts.map(_.linesIterator.size).sum, 120 + inserts.size)
  }

  test("the generated sample is deterministic, so re-seeding produces the same table") {
    assertEquals(DeltaSeed.sampleOrders(10), DeltaSeed.sampleOrders(10))
  }

  test("running the seeding script sends every statement to the session") {
    val session = new FakeSession()
    DeltaSeed.script(List(order)).foreach(session.run)
    assertEquals(session.executed.size, 4)
  }
}
