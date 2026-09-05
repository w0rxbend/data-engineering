package de.presto.hive

import de.common.domain.{ClickEvent, CustomerId, Sku}
import de.common.gen.DataGenerator

final class ClickstreamSuite extends munit.FunSuite {

  private def click(customer: String, page: String, atMillis: Long, sku: Option[String] = None): ClickEvent =
    ClickEvent(CustomerId(customer), page, sku.map(Sku.apply), atMillis)

  test("a customer always belongs to the same market") {
    val first  = Clickstream.countryOf("cust-0042")
    val repeat = Clickstream.countryOf("cust-0042")
    assertEquals(first, repeat)
    assert(Clickstream.Countries.contains(first))
  }

  test("every generated customer maps to a known market, including negative hash codes") {
    val generator = new DataGenerator(seed = 7L)
    val countries = generator.clickStream.take(5000).map(e => Clickstream.countryOf(e.customerId.value)).toSet
    assert(countries.subsetOf(Clickstream.Countries.toSet), s"unexpected markets: $countries")
    assertEquals(countries.size, Clickstream.Countries.size, "the generated data should reach every market")
  }

  test("rows keep the event fields and gain a partition") {
    val rows = Clickstream.rowsFrom(Seq(click("cust-0001", "/product", 1699920000000L, Some("SKU-MUG"))))
    assertEquals(rows.size, 1)
    val row = rows.head
    assertEquals(row.customerId, "cust-0001")
    assertEquals(row.page, "/product")
    assertEquals(row.sku, Some("SKU-MUG"))
    assertEquals(row.occurredAtEpochMillis, 1699920000000L)
    assertEquals(row.partition.country, Clickstream.countryOf("cust-0001"))
    assertEquals(row.partition.relativePath.startsWith("country="), true)
  }

  test("grouping puts every row in exactly one partition") {
    val oneDay  = 24L * 60 * 60 * 1000
    val events  = Seq(click("cust-0001", "/home", 1699920000000L), click("cust-0002", "/cart", 1699920000000L + oneDay))
    val grouped = Clickstream.groupByPartition(Clickstream.rowsFrom(events))
    assertEquals(grouped.values.map(_.size).sum, events.size)
    grouped.foreach { case (partition, rows) => rows.foreach(row => assertEquals(row.partition, partition)) }
  }

  test("the same seed produces the same partitions twice") {
    def partitionsOf: Set[String] =
      Clickstream
        .rowsFrom(new DataGenerator(1L, Settings.DefaultStartEpochMillis).clickStream.take(2000).toVector)
        .map(_.partition.relativePath)
        .toSet
    assertEquals(partitionsOf, partitionsOf)
  }
}
