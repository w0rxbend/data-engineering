package de.common.gen

import de.common.domain._

import scala.util.Random

/**
 * Produces a stream of plausible online-shop events.
 *
 * The generator is *seeded*, meaning that two runs with the same seed produce
 * exactly the same events. That matters for the examples: a reproducible input
 * stream makes it possible to assert on the output of a Flink or Spark job
 * instead of eyeballing it.
 *
 * @param seed           starting point of the pseudo random number generator
 * @param startEpochMillis timestamp of the first generated event
 */
final class DataGenerator(seed: Long = 42L, startEpochMillis: Long = 1700000000000L) {

  private val random = new Random(seed)

  private val skus = Vector("SKU-COFFEE", "SKU-GRINDER", "SKU-KETTLE", "SKU-FILTER", "SKU-MUG")

  private val countries = Vector("DE", "PL", "UA", "FR", "ES")

  private val pages = Vector("/home", "/search", "/product", "/cart", "/checkout")

  private var clock: Long = startEpochMillis

  /** Advances the internal clock by up to one second and returns the new value. */
  private def tick(): Long = {
    clock += random.nextInt(1000).toLong + 1L
    clock
  }

  private def customerId(): CustomerId = CustomerId(f"cust-${random.nextInt(1000)}%04d")

  private def orderLine(): OrderLine =
    OrderLine(
      sku = Sku(skus(random.nextInt(skus.size))),
      quantity = random.nextInt(3) + 1,
      unitPrice = Money.eur(random.nextInt(9000).toLong + 500L)
    )

  /** One order with one to four lines. */
  def nextOrder(): Order = {
    val lineCount = random.nextInt(4) + 1
    Order(
      id = OrderId(f"order-${random.nextInt(1000000)}%07d"),
      customerId = customerId(),
      lines = List.fill(lineCount)(orderLine()),
      placedAtEpochMillis = tick(),
      country = countries(random.nextInt(countries.size))
    )
  }

  /**
   * The payment belonging to an order. Roughly one in ten payments is declined,
   * which gives the fraud- and alerting-oriented examples something to react to.
   */
  def paymentFor(order: Order): Payment = {
    val status =
      if (random.nextInt(10) == 0) PaymentStatus.Declined
      else if (random.nextBoolean()) PaymentStatus.Authorized
      else PaymentStatus.Captured
    Payment(order.id, order.total, status, order.placedAtEpochMillis + random.nextInt(60000).toLong)
  }

  /** The three shipment milestones of an order, in order. */
  def shipmentsFor(order: Order): List[Shipment] = {
    val base = order.placedAtEpochMillis
    List(
      Shipment(order.id, ShipmentStatus.Created, base + 60000L),
      Shipment(order.id, ShipmentStatus.Dispatched, base + 3600000L + random.nextInt(3600000).toLong),
      Shipment(order.id, ShipmentStatus.Delivered, base + 86400000L + random.nextInt(86400000).toLong)
    )
  }

  def nextClickEvent(): ClickEvent = {
    val page = pages(random.nextInt(pages.size))
    ClickEvent(
      customerId = customerId(),
      page = page,
      sku = if (page == "/product") Some(Sku(skus(random.nextInt(skus.size)))) else None,
      occurredAtEpochMillis = tick()
    )
  }

  /** A finite batch of orders, handy for tests and for seeding a topic. */
  def orders(count: Int): List[Order] = List.fill(count)(nextOrder())

  /** An unbounded stream of orders; take as many as you need. */
  def orderStream: Iterator[Order] = Iterator.continually(nextOrder())

  def clickStream: Iterator[ClickEvent] = Iterator.continually(nextClickEvent())
}
