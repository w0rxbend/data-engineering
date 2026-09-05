package de.ksqldb

import de.common.domain.*

class KsqlStatementsSuite extends munit.FunSuite {

  private val order = Order(
    id = OrderId("order-1"),
    customerId = CustomerId("cust-7"),
    lines = List(OrderLine(Sku("SKU-MUG"), 2, Money.eur(1250))),
    placedAtEpochMillis = 1700000000000L,
    country = "DE"
  )

  test("a script is split into statements and its comments are dropped") {
    val script =
      """-- a leading comment
        |CREATE STREAM a (id VARCHAR KEY) WITH (KAFKA_TOPIC = 'a'); -- trailing note
        |
        |CREATE STREAM b (id VARCHAR KEY) WITH (KAFKA_TOPIC = 'b');
        |""".stripMargin

    assertEquals(
      KsqlStatements.statementsOf(script),
      List(
        "CREATE STREAM a (id VARCHAR KEY) WITH (KAFKA_TOPIC = 'a');",
        "CREATE STREAM b (id VARCHAR KEY) WITH (KAFKA_TOPIC = 'b');"
      )
    )
  }

  test("an empty script yields no statements") {
    assertEquals(KsqlStatements.statementsOf("-- nothing but a comment\n\n"), Nil)
  }

  test("an order becomes an INSERT with its lines as an array of structs") {
    assertEquals(
      KsqlStatements.insertOrder(order),
      "INSERT INTO orders_raw (id, customerId, lines, placedAt, country) VALUES ('order-1', 'cust-7', " +
        "ARRAY[STRUCT(sku := 'SKU-MUG', quantity := 2, unitPrice := STRUCT(cents := 1250, currency := 'EUR'))], " +
        "1700000000000, 'DE');"
    )
  }

  test("a payment becomes an INSERT carrying its status as text") {
    val payment = Payment(order.id, Money.eur(2500), PaymentStatus.Declined, 1700000060000L)

    assertEquals(
      KsqlStatements.insertPayment(payment),
      "INSERT INTO payments_raw (orderId, amount, status, occurredAt) VALUES ('order-1', " +
        "STRUCT(cents := 2500, currency := 'EUR'), 'Declined', 1700000060000);"
    )
  }

  test("a quote inside a value is doubled, as SQL requires") {
    assertEquals(KsqlStatements.text("O'Neill"), "'O''Neill'")
  }
}
