package de.hugegraph.fraud

import de.hugegraph.fraud.RingDetection.{FraudRing, Path}

/** Tests for the console rendering, so that a change to the output is a deliberate change. */
class ReportSuite extends munit.FunSuite {

  test("a section underlines its title and ends with a blank line") {
    assertEquals(Report.section("Rings", List("one")), List("Rings", "-----", "one", ""))
  }

  test("an empty section says so instead of looking broken") {
    assertEquals(Report.section("Rings", Nil), List("Rings", "-----", "(nothing found)", ""))
  }

  test("a ring line lists the members and the evidence") {
    val line = Report.rings(List(FraudRing(List("cust-1", "cust-2"), List("card-hot")))).head

    assert(line.contains("cust-1, cust-2"))
    assert(line.contains("shared: card-hot"))
    assert(line.trim.startsWith("2 accounts"))
  }

  test("a found path is rendered as the walk it is") {
    assertEquals(
      Report.path("a", "d", Some(Path(List("a", "b", "c", "d")))),
      List("3 hops", "a -> b -> c -> d")
    )
  }

  test("a missing path says which pair had none") {
    assertEquals(
      Report.path("a", "z", None),
      List("no path between a and z within the hop budget")
    )
  }

  test("an empty server path is reported as no path rather than as zero hops") {
    assertEquals(Report.serverPath(Nil), List("no path found by the server within the hop budget"))
  }

  test("label counts arrive as one JSON object and are rendered sorted by label") {
    val rows = List(ujson.read("""{"order": 12, "customer": 5}"""))

    assertEquals(Report.counts(rows).map(_.trim), List("5  customer", "12  order"))
  }

  test("shared artefact rows show the artefact, the count and the accounts") {
    val rows = List(ujson.read("""{"artefact": "card-hot", "customers": ["cust-1", "cust-2"]}"""))

    val line = Report.sharedArtefacts(rows).head
    assert(line.startsWith("card-hot"))
    assert(line.contains("used by  2 accounts: cust-1, cust-2"))
  }

  test("degree rows from the server render like the in-memory ones") {
    val rows = List(ujson.read("""{"id": "card-hot", "degree": 7}"""))

    assertEquals(Report.serverDegrees(rows), Report.degrees(List(("card-hot", 7))))
  }
}
