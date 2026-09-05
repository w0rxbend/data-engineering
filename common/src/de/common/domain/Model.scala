package de.common.domain

/**
 * The shared "ubiquitous language" of this repository: one small online-shop domain that every example reuses.
 *
 * Using one vocabulary everywhere means you can compare technologies instead of re-learning a new toy domain per
 * example: the `Order` that an Apache Kafka producer writes in example 01 is the same `Order` that Apache Spark reads
 * in example 07.
 *
 * The code is deliberately written in a Scala dialect that compiles unchanged on Scala 2.12, 2.13 and 3 (plain
 * `case class`es and `sealed trait`s, no `enum`, no significant indentation).
 */
final case class CustomerId(value: String) extends AnyVal

final case class OrderId(value: String) extends AnyVal

final case class Sku(value: String) extends AnyVal

/** A monetary amount kept in minor units (cents) to avoid floating point drift. */
final case class Money(cents: Long, currency: String) {
  def +(other: Money): Money = {
    require(other.currency == currency, s"cannot add $currency to ${other.currency}")
    Money(cents + other.cents, currency)
  }

  def *(quantity: Int): Money = Money(cents * quantity.toLong, currency)

  /** Human readable rendering, for example `12.34 EUR`. */
  override def toString: String = f"${cents / 100.0}%.2f $currency"
}

object Money {
  def eur(cents: Long): Money = Money(cents, "EUR")
}

final case class OrderLine(sku: Sku, quantity: Int, unitPrice: Money) {
  def lineTotal: Money = unitPrice * quantity
}

/**
 * A customer order.
 *
 * @param placedAtEpochMillis
 *   wall-clock time the order was accepted, in milliseconds since 1970-01-01 UTC. Milliseconds since the epoch is the
 *   timestamp format every engine in this repository understands natively.
 */
final case class Order(
    id: OrderId,
    customerId: CustomerId,
    lines: List[OrderLine],
    placedAtEpochMillis: Long,
    country: String
) {
  def total: Money =
    lines.map(_.lineTotal).reduceOption(_ + _).getOrElse(Money.eur(0))
}

/** Outcome of charging a customer for an order. */
sealed trait PaymentStatus extends Product with Serializable

object PaymentStatus {
  case object Authorized extends PaymentStatus
  case object Captured   extends PaymentStatus
  case object Declined   extends PaymentStatus

  val all: List[PaymentStatus] = List(Authorized, Captured, Declined)

  def fromString(raw: String): Option[PaymentStatus] =
    all.find(_.toString.equalsIgnoreCase(raw))
}

final case class Payment(
    orderId: OrderId,
    amount: Money,
    status: PaymentStatus,
    occurredAtEpochMillis: Long
)

/** Where a parcel currently is. */
sealed trait ShipmentStatus extends Product with Serializable

object ShipmentStatus {
  case object Created    extends ShipmentStatus
  case object Dispatched extends ShipmentStatus
  case object Delivered  extends ShipmentStatus

  val all: List[ShipmentStatus] = List(Created, Dispatched, Delivered)

  def fromString(raw: String): Option[ShipmentStatus] =
    all.find(_.toString.equalsIgnoreCase(raw))
}

final case class Shipment(
    orderId: OrderId,
    status: ShipmentStatus,
    occurredAtEpochMillis: Long
)

/** A single page view or click in the web shop. */
final case class ClickEvent(
    customerId: CustomerId,
    page: String,
    sku: Option[Sku],
    occurredAtEpochMillis: Long
)
