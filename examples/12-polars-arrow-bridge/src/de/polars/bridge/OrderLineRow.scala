package de.polars.bridge

import de.common.domain.Order

/**
 * One row of the flat table that crosses the language boundary.
 *
 * The shared domain model in `de.common.domain` is nested: an `Order` owns a list of `OrderLine`s. Columnar formats
 * such as Apache Arrow can represent nesting, but every analytics tool is happier with a flat table, so an order with
 * three lines becomes three rows that repeat the order-level attributes. This is exactly the shape a data warehouse
 * calls a "fact table".
 *
 * Money stays in cents (`Long`) rather than becoming a floating point number: summing cents is exact, summing doubles
 * is not.
 */
final case class OrderLineRow(
    orderId: String,
    customerId: String,
    country: String,
    placedAtEpochMillis: Long,
    sku: String,
    quantity: Int,
    unitPriceCents: Long,
    lineTotalCents: Long
)

object OrderLineRow {

  /** Flattens one order into one row per order line. */
  def fromOrder(order: Order): List[OrderLineRow] =
    order.lines.map { line =>
      OrderLineRow(
        orderId = order.id.value,
        customerId = order.customerId.value,
        country = order.country,
        placedAtEpochMillis = order.placedAtEpochMillis,
        sku = line.sku.value,
        quantity = line.quantity,
        unitPriceCents = line.unitPrice.cents,
        lineTotalCents = line.lineTotal.cents
      )
    }

  def fromOrders(orders: Seq[Order]): List[OrderLineRow] = orders.iterator.flatMap(fromOrder).toList
}

/**
 * A tiny dimension table mapping a country code to a sales region.
 *
 * It exists so that the Polars script has something real to join against. Joining a large fact table to a small
 * dimension table is the single most common shape in analytics, and it is worth seeing it cross the boundary.
 */
final case class RegionRow(country: String, region: String)

object RegionRow {

  /** The five countries the shared data generator produces, grouped into two regions. */
  val all: List[RegionRow] = List(
    RegionRow("DE", "DACH"),
    RegionRow("PL", "CEE"),
    RegionRow("UA", "CEE"),
    RegionRow("FR", "WEST"),
    RegionRow("ES", "WEST")
  )

  private val byCountry: Map[String, String] = all.map(row => row.country -> row.region).toMap

  /** The region a country belongs to, or `UNKNOWN` for a country the table does not list. */
  def regionOf(country: String): String = byCountry.getOrElse(country, "UNKNOWN")
}
