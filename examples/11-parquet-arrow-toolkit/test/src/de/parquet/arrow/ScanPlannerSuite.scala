package de.parquet.arrow

/**
 * The cost arithmetic, exercised against a layout built by hand.
 *
 * Constructing the layout literally rather than reading a real file keeps the numbers in the assertions obvious: a
 * reader can add them up on paper and see whether the expectation is right.
 */
class ScanPlannerSuite extends munit.FunSuite {

  private def chunk(
      column: String,
      compressedBytes: Long,
      bounds: Option[(Long, Long)] = None,
      encodings: Set[String] = Set("PLAIN")
  ): ColumnChunkLayout =
    ColumnChunkLayout(
      column = column,
      codec = "SNAPPY",
      compressedBytes = compressedBytes,
      uncompressedBytes = compressedBytes * 2,
      valueCount = 100L,
      encodings = encodings,
      bounds = bounds.map { case (low, high) => (low.toString, high.toString) },
      numericBounds = bounds,
      nullCount = Some(0L)
    )

  /** Three row groups of 300 bytes each, whose `placed_at` ranges are 0-99, 100-199 and 200-299. */
  private val layout = ParquetLayout(
    fileBytes = 1000L,
    createdBy = "test",
    schema = "message OrderArchiveRow { }",
    rowGroups = Vector.tabulate(3) { ordinal =>
      val low = ordinal * 100L
      RowGroupLayout(
        ordinal = ordinal,
        rowCount = 100L,
        columns = Vector(
          chunk("placed_at", 100L, bounds = Some((low, low + 99L))),
          chunk("sku", 20L, encodings = Set("RLE_DICTIONARY")),
          chunk("customer_id", 180L)
        )
      )
    }
  )

  test("a full scan reads every column of every row group") {
    val cost = ScanPlanner.fullScan(layout)

    assertEquals(cost.rowGroupsRead, 3)
    assertEquals(cost.rowGroupsSkipped, 0)
    assertEquals(cost.bytesRead, 900L)
  }

  test("a projection reads every row group but only the named columns") {
    val cost = ScanPlanner.projectedScan(layout, Set("sku"))

    assertEquals(cost.rowGroupsRead, 3)
    assertEquals(cost.bytesRead, 60L)
  }

  test("a projection of an unknown column reads nothing") {
    assertEquals(ScanPlanner.projectedScan(layout, Set("not_a_column")).bytesRead, 0L)
  }

  test("a predicate keeps only the row groups whose statistics allow a match") {
    val surviving = ScanPlanner.survivingRowGroups(layout, "placed_at", 120L, 130L)

    assertEquals(surviving.map(_.ordinal), Vector(1))
  }

  test("a predicate spanning a boundary keeps both neighbouring row groups") {
    val surviving = ScanPlanner.survivingRowGroups(layout, "placed_at", 99L, 100L)

    assertEquals(surviving.map(_.ordinal), Vector(0, 1))
  }

  test("a predicate matching nothing skips every row group") {
    val cost = ScanPlanner.filteredScan(layout, "placed_at", 500L, 600L, Set("placed_at"))

    assertEquals(cost.rowGroupsRead, 0)
    assertEquals(cost.rowGroupsSkipped, 3)
    assertEquals(cost.bytesRead, 0L)
  }

  test("a column without statistics is never skipped, because there is no basis for skipping it") {
    val surviving = ScanPlanner.survivingRowGroups(layout, "customer_id", 0L, 1L)

    assertEquals(surviving.size, 3)
  }

  test("predicate and projection compose: fewer row groups and fewer columns") {
    val cost = ScanPlanner.filteredScan(layout, "placed_at", 0L, 99L, Set("sku"))

    assertEquals(cost.rowGroupsRead, 1)
    assertEquals(cost.rowGroupsSkipped, 2)
    assertEquals(cost.bytesRead, 20L)
  }

  test("dictionary encoding is recognised under both its modern and its legacy name") {
    assert(chunk("c", 1L, encodings = Set("RLE_DICTIONARY")).usesDictionary)
    assert(chunk("c", 1L, encodings = Set("PLAIN_DICTIONARY")).usesDictionary)
    assert(!chunk("c", 1L, encodings = Set("PLAIN")).usesDictionary)
  }

  test("numeric bounds are compared as numbers, not as text") {
    val numericLayout = layout.copy(rowGroups =
      Vector(
        RowGroupLayout(0, 2L, Vector(chunk("price", 10L, bounds = Some((509L, 9489L)))))
      )
    )

    assertEquals(numericLayout.bounds("price"), Some(("509", "9489")))
  }

  test("the file-wide summary adds up its row groups") {
    assertEquals(layout.rowCount, 300L)
    assertEquals(layout.dataBytes, 900L)
    assertEquals(layout.compressedBytesOf("sku"), 60L)
    assertEquals(layout.codec, "SNAPPY")
    assert(layout.isDictionaryEncoded("sku"))
    assert(!layout.isDictionaryEncoded("customer_id"))
  }
}
