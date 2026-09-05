package de.kafkastreams.fraud

/** The console rendering of an interactive query, without a running cluster. */
class StoreQueriesSuite extends munit.FunSuite {

  test("an empty result says so instead of printing nothing") {
    assert(StoreQueries.render(Nil).contains("no declines"))
  }

  test("rows are listed worst first") {
    val rows = List(
      StoreQueries.Row("cust-0001", 0L, DeclineTally(2, 400L)),
      StoreQueries.Row("cust-0002", 0L, DeclineTally(7, 1400L))
    )
    val lines = StoreQueries.render(rows).linesIterator.toList
    assertEquals(lines.size, 2)
    assert(lines.head.contains("cust-0002"), lines.head)
    assert(lines.head.contains("declines=7"), lines.head)
    assert(lines(1).contains("cust-0001"), lines(1))
  }

  test("amounts are rendered in euro rather than in cents") {
    val line = StoreQueries.render(List(StoreQueries.Row("cust-0003", 0L, DeclineTally(3, 597L))))
    assert(line.contains("5.97 EUR"), line)
  }
}
