package de.presto.hive

final class ReportsSuite extends munit.FunSuite {

  test("percentages are rendered with one decimal") {
    assertEquals(Reports.percentage(0.125), "12.5%")
    assertEquals(Reports.percentage(0.0), "0.0%")
    assertEquals(Reports.percentage(1.0), "100.0%")
  }

  test("byte counts pick the largest readable unit") {
    assertEquals(Reports.bytes(512), "512 B")
    assertEquals(Reports.bytes(1024), "1.00 KiB")
    assertEquals(Reports.bytes(1536), "1.50 KiB")
    assertEquals(Reports.bytes(5L * 1024 * 1024), "5.00 MiB")
    assertEquals(Reports.bytes(3L * 1024 * 1024 * 1024), "3.00 GiB")
  }

  test("the funnel shows both the overall and the step-to-step survival rate") {
    val rendered = Reports.funnel(FunnelCounts(1000, 500, 250, 100, 50))
    val expected =
      """step       sessions  of entered  of previous
        |---------  --------  ----------  -----------
        |/home      1000      100.0%      100.0%
        |/search    500       50.0%       50.0%
        |/product   250       25.0%       50.0%
        |/cart      100       10.0%       40.0%
        |/checkout  50        5.0%        50.0%""".stripMargin
    assertNoDiff(rendered, expected)
  }

  test("an empty funnel reports zero instead of dividing by zero") {
    val rendered = Reports.funnel(FunnelCounts(0, 0, 0, 0, 0))
    assert(rendered.contains("0.0%"))
    assert(!rendered.contains("NaN"))
  }

  test("conversion rows are ordered by conversion rate, best first") {
    val rendered = Reports.conversion(
      Seq(ConversionRow("DE", 200, 10), ConversionRow("PL", 100, 20), ConversionRow("UA", 100, 1))
    )
    val countries = rendered.linesIterator.drop(2).map(_.take(2)).toList
    assertEquals(countries, List("PL", "DE", "UA"))
  }

  test("a country with no sessions converts at zero rather than NaN") {
    assertEquals(ConversionRow("ES", 0, 0).conversionRate, 0.0)
    assert(!Reports.conversion(Seq(ConversionRow("ES", 0, 0))).contains("NaN"))
  }

  test("the scan comparison reports how much less data the pruned query read") {
    val rendered =
      Reports.scanComparison(ScanComparison(bytesWithoutPredicate = 10485760, bytesWithPredicate = 1048576))
    assert(rendered.contains("10.00 MiB"))
    assert(rendered.contains("1.00 MiB"))
    assert(rendered.contains("10.0 x less data read"))
  }

  test("a pruned query that read nothing reports no reduction instead of infinity") {
    assertEquals(ScanComparison(1000, 0).reductionFactor, None)
    assert(Reports.scanComparison(ScanComparison(1000, 0)).contains("no measurable reduction"))
  }

  test("table columns are padded to the widest cell and trailing padding is trimmed") {
    val rendered = Reports.table(Seq("a", "long-header"), Seq(Seq("much-longer-cell", "x")))
    val expected =
      """a                 long-header
        |----------------  -----------
        |much-longer-cell  x""".stripMargin
    assertNoDiff(rendered, expected)
  }
}
