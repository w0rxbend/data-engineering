package de.parquet.arrow

import scala.jdk.CollectionConverters.*

class ArchiveSchemaSuite extends munit.FunSuite {

  private val row = ArchiveRow(
    orderId = "order-0000001",
    customerId = "cust-0042",
    country = "PL",
    placedAtEpochMillis = 1700000000000L,
    sku = "SKU-COFFEE",
    quantity = 2,
    unitPriceCents = 1250L,
    lineTotalCents = 2500L
  )

  test("the Avro schema declares the archive's columns in order") {
    assertEquals(ArchiveSchema.avro.getFields.asScala.map(_.name).toVector, OrderArchive.Columns)
  }

  test("the Avro namespace differs from this package, so Avro cannot mistake a record for the case class") {
    assertNotEquals(ArchiveSchema.avro.getNamespace, "de.parquet.arrow")
  }

  test("a row converts to a record and back to the same row") {
    assertEquals(ArchiveSchema.toRow(ArchiveSchema.toRecord(row)), row)
  }

  test("a projection keeps only the named fields, in the full schema's order") {
    val projection = ArchiveSchema.projection(Set("line_total_cents", "sku"))

    assertEquals(projection.getFields.asScala.map(_.name).toVector, Vector("sku", "line_total_cents"))
  }

  test("a projection of every column is the full column list again") {
    val projection = ArchiveSchema.projection(OrderArchive.Columns.toSet)

    assertEquals(projection.getFields.asScala.map(_.name).toVector, OrderArchive.Columns)
  }

  test("an unknown column is rejected instead of silently producing a smaller projection") {
    intercept[IllegalArgumentException] {
      ArchiveSchema.projection(Set("sku", "not_a_column"))
    }
  }

  test("the predicate reads as the range it describes") {
    assertEquals(
      ArchiveReader.between("placed_at", 10L, 20L).toString,
      "and(gteq(placed_at, 10), lteq(placed_at, 20))"
    )
  }
}
