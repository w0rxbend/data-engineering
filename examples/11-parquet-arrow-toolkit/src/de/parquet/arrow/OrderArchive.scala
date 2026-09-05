package de.parquet.arrow

import de.common.domain.Order

/**
 * One row of the order archive: a single line of a single order, with the order's own attributes repeated next to it.
 *
 * The shared domain model in `de.common.domain` nests order lines inside an `Order`. Parquet can store nested data, but
 * an archive that is going to be scanned column by column is far easier to reason about when it is flat: every column
 * is then one contiguous run of values on disk, and "read only `sku` and `line_total_cents`" means "read exactly two
 * chunks of bytes". Repeating `order_id` and `country` on every line costs almost nothing, because Parquet stores each
 * column separately and a column of repeated values compresses to nearly nothing (see `DictionaryEncoding` in the
 * README).
 *
 * @param placedAtEpochMillis
 *   when the order was accepted, in milliseconds since 1970-01-01 UTC (Coordinated Universal Time). This is the column
 *   the predicate-pushdown demonstration filters on.
 */
final case class ArchiveRow(
    orderId: String,
    customerId: String,
    country: String,
    placedAtEpochMillis: Long,
    sku: String,
    quantity: Int,
    unitPriceCents: Long,
    lineTotalCents: Long
)

/** Turns shared-domain orders into flat archive rows, and nothing else. */
object OrderArchive {

  /** The archive's column names, in the order they appear in the Parquet schema. */
  val Columns: Vector[String] = Vector(
    "order_id",
    "customer_id",
    "country",
    "placed_at",
    "sku",
    "quantity",
    "unit_price_cents",
    "line_total_cents"
  )

  /** The two columns the projection-pushdown demonstration reads. */
  val ProjectedColumns: Set[String] = Set("sku", "line_total_cents")

  /** Flattens one order into one row per order line. An order with no lines contributes no rows. */
  def rowsFrom(order: Order): Vector[ArchiveRow] =
    order.lines.iterator.map { line =>
      ArchiveRow(
        orderId = order.id.value,
        customerId = order.customerId.value,
        country = order.country,
        placedAtEpochMillis = order.placedAtEpochMillis,
        sku = line.sku.value,
        quantity = line.quantity,
        unitPriceCents = line.unitPrice.cents,
        lineTotalCents = line.lineTotal.cents
      )
    }.toVector

  /**
   * Flattens many orders and sorts the result by timestamp.
   *
   * The sort is the whole reason predicate pushdown works later on. Parquet records the smallest and largest value of
   * every column in every row group, and skips a row group whose range cannot contain a match. If the rows arrived in
   * random timestamp order, every row group would span the entire time range and none could ever be skipped. Sorting on
   * the column you filter by is the cheapest possible physical-design decision in a columnar archive, and it is exactly
   * what "clustering" or "Z-ordering" does in the table formats of examples 07 and 08.
   */
  def rowsFrom(orders: Iterable[Order]): Vector[ArchiveRow] =
    orders.iterator.flatMap(rowsFrom).toVector.sortBy(_.placedAtEpochMillis)

  /** Total revenue per stock keeping unit, the aggregate the projected read computes. */
  def revenueBySku(rows: Iterable[ArchiveRow]): Map[String, Long] =
    rows.groupMapReduce(_.sku)(_.lineTotalCents)(_ + _)
}
