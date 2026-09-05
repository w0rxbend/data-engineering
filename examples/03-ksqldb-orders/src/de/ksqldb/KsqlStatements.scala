package de.ksqldb

import de.common.domain.{Money, Order, OrderLine, Payment}

/**
 * Turns SQL scripts and domain objects into ksqlDB statements.
 *
 * Everything here is a pure function from data to a `String`: no network, no clock, no randomness. That is what makes
 * the interesting part of this example unit-testable without Docker.
 */
object KsqlStatements {

  /**
   * Splits a `.sql` script into the individual statements ksqlDB expects.
   *
   * The ksqlDB REST endpoint accepts several statements in one request, but reporting per-statement success is far
   * clearer when they are submitted one at a time. Line comments (everything after `--`) are dropped first, because
   * they carry no meaning for the server.
   */
  def statementsOf(script: String): List[String] = {
    val withoutComments = script.linesIterator.map(stripLineComment).mkString("\n")
    withoutComments
      .split(';')
      .iterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_ + ";")
      .toList
  }

  private def stripLineComment(line: String): String = {
    val marker = line.indexOf("--")
    if (marker >= 0) { line.substring(0, marker) }
    else { line }
  }

  /**
   * Renders an order as `INSERT INTO ... VALUES (...)`.
   *
   * Seeding the topics through ksqlDB itself, rather than through a Kafka producer, keeps this example down to a single
   * dependency: an HTTP client.
   */
  def insertOrder(order: Order): String = {
    val lines = order.lines.map(orderLineLiteral).mkString("ARRAY[", ", ", "]")
    s"INSERT INTO orders_raw (id, customerId, lines, placedAt, country) VALUES (" +
      s"${text(order.id.value)}, ${text(order.customerId.value)}, $lines, " +
      s"${order.placedAtEpochMillis}, ${text(order.country)});"
  }

  /** Renders the payment belonging to an order as an `INSERT INTO ... VALUES`. */
  def insertPayment(payment: Payment): String =
    s"INSERT INTO payments_raw (orderId, amount, status, occurredAt) VALUES (" +
      s"${text(payment.orderId.value)}, ${moneyLiteral(payment.amount)}, " +
      s"${text(payment.status.toString)}, ${payment.occurredAtEpochMillis});"

  private def orderLineLiteral(line: OrderLine): String =
    s"STRUCT(sku := ${text(line.sku.value)}, quantity := ${line.quantity}, " +
      s"unitPrice := ${moneyLiteral(line.unitPrice)})"

  private def moneyLiteral(money: Money): String =
    s"STRUCT(cents := ${money.cents}, currency := ${text(money.currency)})"

  /**
   * Wraps a value in single quotes, doubling any quote it contains.
   *
   * Doubling is how SQL escapes a quote inside a literal: the customer name `O'Neill` has to reach the server as
   * `'O''Neill'`.
   */
  def text(raw: String): String = "'" + raw.replace("'", "''") + "'"
}
