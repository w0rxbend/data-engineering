package de.parquet.arrow

import de.common.domain.*
import de.common.gen.DataGenerator

class OrderArchiveSuite extends munit.FunSuite {

  private def order(id: String, placedAt: Long, lines: (String, Int, Long)*): Order =
    Order(
      id = OrderId(id),
      customerId = CustomerId("cust-0001"),
      lines = lines.map { case (sku, quantity, cents) =>
        OrderLine(Sku(sku), quantity, Money.eur(cents))
      }.toList,
      placedAtEpochMillis = placedAt,
      country = "DE"
    )

  test("one order becomes one row per order line") {
    val rows = OrderArchive.rowsFrom(order("order-1", 1000L, ("SKU-MUG", 2, 500L), ("SKU-KETTLE", 1, 9000L)))

    assertEquals(rows.size, 2)
    assertEquals(rows.map(_.orderId).distinct, Vector("order-1"))
    assertEquals(rows.map(_.sku), Vector("SKU-MUG", "SKU-KETTLE"))
  }

  test("the line total is the unit price times the quantity") {
    val rows = OrderArchive.rowsFrom(order("order-1", 1000L, ("SKU-MUG", 3, 250L)))

    assertEquals(rows.head.unitPriceCents, 250L)
    assertEquals(rows.head.lineTotalCents, 750L)
  }

  test("an order without lines contributes no rows") {
    assertEquals(OrderArchive.rowsFrom(order("order-empty", 1000L)), Vector.empty)
  }

  test("rows are sorted by timestamp, which is what makes row-group statistics selective") {
    val unsorted = Seq(
      order("order-late", 3000L, ("SKU-MUG", 1, 100L)),
      order("order-early", 1000L, ("SKU-MUG", 1, 100L)),
      order("order-middle", 2000L, ("SKU-MUG", 1, 100L))
    )

    assertEquals(OrderArchive.rowsFrom(unsorted).map(_.placedAtEpochMillis), Vector(1000L, 2000L, 3000L))
  }

  test("revenue per stock keeping unit adds up the line totals") {
    val rows = OrderArchive.rowsFrom(
      Seq(
        order("order-1", 1000L, ("SKU-MUG", 2, 500L), ("SKU-KETTLE", 1, 9000L)),
        order("order-2", 2000L, ("SKU-MUG", 1, 500L))
      )
    )

    assertEquals(OrderArchive.revenueBySku(rows), Map("SKU-MUG" -> 1500L, "SKU-KETTLE" -> 9000L))
  }

  test("the shared generator produces an archive with every column populated") {
    val rows = OrderArchive.rowsFrom(new DataGenerator(seed = 7L).orders(50))

    assert(rows.sizeIs >= 50, s"50 orders should yield at least 50 lines, got ${rows.size}")
    assert(rows.forall(_.orderId.startsWith("order-")))
    assert(rows.forall(_.country.length == 2))
    assert(rows.forall(_.quantity >= 1))
  }
}
