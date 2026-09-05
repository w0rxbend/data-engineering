package de.parquet.arrow

class ReportsSuite extends munit.FunSuite {

  test("byte counts are rendered in the largest unit that keeps the number readable") {
    assertEquals(Reports.bytes(0L), "0 B")
    assertEquals(Reports.bytes(512L), "512 B")
    assertEquals(Reports.bytes(1024L), "1.00 KiB")
    assertEquals(Reports.bytes(1536L), "1.50 KiB")
    assertEquals(Reports.bytes(3L * 1024 * 1024), "3.00 MiB")
  }

  test("ratios and percentages are rendered to one decimal") {
    assertEquals(Reports.factor(4.68), "4.7x")
    assertEquals(Reports.percentage(0.125), "12.5%")
    assertEquals(Reports.percentage(0.0), "0.0%")
  }

  test("a table pads every column to its widest cell and underlines the header") {
    val rendered = Reports.table(Seq("name", "size"), Seq(Seq("country", "1 B"), Seq("sku", "22 B")))

    assertEquals(
      rendered,
      """name     size
        |-------  ----
        |country  1 B
        |sku      22 B""".stripMargin
    )
  }

  test("a table with no rows is still a readable header") {
    assertEquals(Reports.table(Seq("name"), Nil), "name\n----")
  }

  test("the codec comparison measures every variant against the first one") {
    val rendered = Reports.codecComparison(
      Seq(
        CodecMeasurement("uncompressed", 1000L, dictionaryEncoded = true),
        CodecMeasurement("zstd", 250L, dictionaryEncoded = false)
      )
    )

    assert(rendered.contains("1.0x"), rendered)
    assert(rendered.contains("4.0x"), rendered)
    assert(rendered.contains("no"), rendered)
  }

  test("a codec measurement of zero bytes reports no saving rather than dividing by zero") {
    val baseline = CodecMeasurement("uncompressed", 1000L, dictionaryEncoded = true)

    assertEquals(CodecMeasurement("empty", 0L, dictionaryEncoded = true).savingAgainst(baseline), 0.0)
  }

  test("the scan comparison shows how many row groups each strategy touches") {
    val rendered = Reports.scanComparison(
      Seq(
        "full scan" -> ScanCost(rowGroupsRead = 4, rowGroupsSkipped = 0, bytesRead = 800L),
        "predicate" -> ScanCost(rowGroupsRead = 1, rowGroupsSkipped = 3, bytesRead = 200L)
      )
    )

    assert(rendered.contains("4 of 4"), rendered)
    assert(rendered.contains("1 of 4"), rendered)
    assert(rendered.contains("4.0x"), rendered)
  }

  test("a scan that reads nothing reports no factor instead of an infinite one") {
    val rendered = Reports.scanComparison(
      Seq(
        "full scan" -> ScanCost(4, 0, 800L),
        "no match"  -> ScanCost(0, 4, 0L)
      )
    )

    assert(rendered.linesIterator.exists(line => line.startsWith("no match") && line.endsWith("-")), rendered)
  }

  test("the Arrow footprint lists one line per vector") {
    val rendered = Reports.arrowFootprint(
      Seq(VectorFootprint("sku", 4096L, 1000), VectorFootprint("quantity", 512L, 1000))
    )

    assertEquals(rendered.linesIterator.size, 4)
    assert(rendered.contains("4.00 KiB"), rendered)
  }
}
