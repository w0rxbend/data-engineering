package de.kafka.eos

import de.common.domain.{CustomerId, Money, Order, OrderId, OrderLine, Sku}

import scala.util.Try
import scala.util.control.NonFatal

/**
 * Reads the JSON layout that `de.common.json.Codecs` writes back into an `Order`.
 *
 * The shared `common` module only knows how to *write* JSON (JavaScript Object Notation), because it is compiled for
 * three Scala versions and no single JSON library covers all of them. Each example therefore brings its own reader;
 * this one uses `ujson`, the JSON value type of the uPickle library.
 */
object OrderJson {

  /**
   * @return
   *   the decoded order, or a human-readable description of what was wrong with the record. A malformed record is an
   *   expected event on a real topic, not a bug, so it is returned as a value rather than thrown.
   */
  def decode(raw: String): Either[String, Order] =
    parse(raw).flatMap(readOrder)

  private def parse(raw: String): Either[String, ujson.Value] =
    Try(ujson.read(raw)).toEither.left.map(failure => s"not valid JSON: ${failure.getMessage}")

  private def readOrder(json: ujson.Value): Either[String, Order] =
    guarded {
      Order(
        id = OrderId(json("id").str),
        customerId = CustomerId(json("customerId").str),
        lines = json("lines").arr.toList.map(readOrderLine),
        placedAtEpochMillis = json("placedAt").num.toLong,
        country = json("country").str
      )
    }

  private def readOrderLine(json: ujson.Value): OrderLine =
    OrderLine(
      sku = Sku(json("sku").str),
      quantity = json("quantity").num.toInt,
      unitPrice = readMoney(json("unitPrice"))
    )

  private def readMoney(json: ujson.Value): Money =
    Money(cents = json("cents").num.toLong, currency = json("currency").str)

  /**
   * Turns the exceptions ujson throws for a missing field or a wrong type into a `Left`. The `try`/`catch` sits at this
   * one boundary so that the reading functions above stay free of error plumbing.
   */
  private def guarded(read: => Order): Either[String, Order] =
    try Right(read)
    catch {
      case NonFatal(failure) =>
        Left(s"does not match the order layout: ${failure.getMessage}")
    }
}
