package de.polars.bridge

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import scala.collection.mutable

/**
 * Revenue rolled up to one row per country.
 *
 * @param orderCount
 *   number of distinct orders, not number of order lines
 * @param units
 *   total quantity across all lines
 * @param revenueCents
 *   total line value in cents
 */
final case class RevenueByCountry(
    country: String,
    region: String,
    orderCount: Long,
    units: Long,
    revenueCents: Long
)

object RevenueByCountry {

  /** A stable ordering so that two implementations can be compared row by row. */
  given Ordering[RevenueByCountry] = Ordering.by(_.country)
}

/**
 * The same aggregation written twice, so the two can be timed against each other.
 *
 * Both implementations answer one question: per country, how many distinct orders were placed, how many units were
 * sold, and what did they add up to in cents? The Polars script in `polars/aggregate.py` answers the very same question
 * over the very same bytes, which makes the three numbers directly comparable.
 */
object RevenueAggregation {

  /**
   * The straightforward, object-oriented implementation.
   *
   * It reads ordinary Scala case classes, which means every row has already been turned into eight Java objects before
   * the first addition happens. This is the version most JVM codebases actually contain, and it is the baseline the
   * columnar version has to beat.
   */
  def fromRows(rows: Seq[OrderLineRow]): List[RevenueByCountry] =
    rows
      .groupBy(_.country)
      .map { case (country, linesOfCountry) =>
        RevenueByCountry(
          country = country,
          region = RegionRow.regionOf(country),
          orderCount = linesOfCountry.iterator.map(_.orderId).toSet.size.toLong,
          units = linesOfCountry.iterator.map(_.quantity.toLong).sum,
          revenueCents = linesOfCountry.iterator.map(_.lineTotalCents).sum
        )
      }
      .toList
      .sorted

  /**
   * The columnar implementation: it never builds an `OrderLineRow`.
   *
   * It walks the Arrow file one record batch at a time and reads the numbers straight out of the off-heap column
   * buffers. Only the two string columns still cost an allocation per row; Arrow's dictionary encoding (which stores
   * each distinct country once and repeats a small integer index) would remove even that, at the price of a more
   * involved writer.
   *
   * Because it only ever holds one batch at a time, this version works on a file larger than the Java heap - the same
   * trick Polars calls streaming mode.
   */
  def fromArrowFile(allocator: BufferAllocator, source: Path): List[RevenueByCountry] = {
    val accumulators = mutable.LinkedHashMap.empty[String, CountryAccumulator]
    ArrowIpc.forEachBatch(allocator, source)(accumulate(accumulators))
    accumulators.values.map(_.result()).toList.sorted
  }

  private def accumulate(
      accumulators: mutable.Map[String, CountryAccumulator]
  )(root: VectorSchemaRoot): Unit = {
    val columns  = OrderLineVectors(root)
    val rowCount = root.getRowCount
    var row      = 0
    while (row < rowCount) {
      val country     = new String(columns.country.get(row), StandardCharsets.UTF_8)
      val accumulator = accumulators.getOrElseUpdate(country, new CountryAccumulator(country))
      accumulator.add(
        orderId = new String(columns.orderId.get(row), StandardCharsets.UTF_8),
        quantity = columns.quantity.get(row),
        lineTotalCents = columns.lineTotal.get(row)
      )
      row += 1
    }
  }

  /** Mutable running totals for one country. Private to the aggregation; nothing outside sees the mutation. */
  private final class CountryAccumulator(country: String) {
    private val orderIds     = mutable.HashSet.empty[String]
    private var units        = 0L
    private var revenueCents = 0L

    def add(orderId: String, quantity: Int, lineTotalCents: Long): Unit = {
      orderIds += orderId
      units += quantity.toLong
      revenueCents += lineTotalCents
    }

    def result(): RevenueByCountry =
      RevenueByCountry(country, RegionRow.regionOf(country), orderIds.size.toLong, units, revenueCents)
  }
}
