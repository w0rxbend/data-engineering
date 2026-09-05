package de.presto.hive

import de.common.domain.ClickEvent

/**
 * One row of the `clickstream` table.
 *
 * The shared `ClickEvent` of `de.common.domain` carries no country, because a click on a web page does not know one.
 * The shop, however, resolves a country for every visitor (from the storefront domain they arrived on) before the event
 * reaches the data lake, so the analytics table has one. `Clickstream.rowsFrom` performs that resolution.
 */
final case class ClickRow(
    customerId: String,
    page: String,
    sku: Option[String],
    occurredAtEpochMillis: Long,
    partition: HivePartition
)

object Clickstream {

  /**
   * The storefronts the shop operates, in the same order as the country list of `de.common.gen.DataGenerator`.
   *
   * Keeping the two lists aligned means the clickstream and the order stream of the other examples talk about the same
   * five markets.
   */
  val Countries: Vector[String] = Vector("DE", "PL", "UA", "FR", "ES")

  /**
   * Resolves the storefront a customer shops on.
   *
   * The mapping is a stable hash of the customer identifier rather than a random draw, for two reasons: a customer must
   * not appear to hop between countries in the middle of a funnel, and re-running the generator must produce the same
   * partitions so that the numbers in the README stay true.
   *
   * `Math.floorMod` is used instead of `%` because `%` on a negative hash code yields a negative index.
   */
  def countryOf(customerId: String): String =
    Countries(Math.floorMod(customerId.hashCode, Countries.size))

  /** Turns shared-domain events into table rows, resolving the country and the partition of each one. */
  def rowsFrom(events: Iterable[ClickEvent]): Vector[ClickRow] =
    events.iterator.map { event =>
      val country = countryOf(event.customerId.value)
      ClickRow(
        customerId = event.customerId.value,
        page = event.page,
        sku = event.sku.map(_.value),
        occurredAtEpochMillis = event.occurredAtEpochMillis,
        partition = HivePartition.of(country, event.occurredAtEpochMillis)
      )
    }.toVector

  /**
   * Groups rows by the partition they belong to.
   *
   * This is the step that decides how many files the writer produces. Grouping first and writing once per group keeps
   * one Parquet file per partition; appending row by row would instead produce many tiny files, which is the classic
   * way to make a Hive table slow.
   */
  def groupByPartition(rows: Iterable[ClickRow]): Map[HivePartition, Vector[ClickRow]] =
    rows.groupBy(_.partition).view.mapValues(_.toVector).toMap
}
