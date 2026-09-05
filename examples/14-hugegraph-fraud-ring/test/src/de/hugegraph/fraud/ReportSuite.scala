package de.hugegraph.fraud

import de.hugegraph.fraud.RingDetection.{FraudRing, Path}
import de.hugegraph.fraud.BackendCheck.{Failed, Passed}
import de.hugegraph.fraud.PropertyGraph.{Edge, Graph, Vertex}

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

  private val verificationGraph = Graph(
    vertices = List(Vertex("customer", "a", Map.empty), Vertex("customer", "b", Map.empty)),
    edges = List(Edge("related", "a", "customer", "b", "customer", Map.empty))
  )

  test("backend label counts are matched against the local graph") {
    val checks = BackendVerification.graphCounts(
      verificationGraph,
      vertexRows = List(ujson.Obj("customer" -> 2)),
      edgeRows = List(ujson.Obj("related" -> 1))
    )
    assert(checks.forall(_.isInstanceOf[Passed]), checks.map(_.render).mkString("\n"))
  }

  test("a partial backend load is reported as a mismatch") {
    val checks = BackendVerification.graphCounts(
      verificationGraph,
      vertexRows = List(ujson.Obj("customer" -> 1)),
      edgeRows = List(ujson.Obj("related" -> 1))
    )
    assert(checks.exists(_.isInstanceOf[Failed]), checks.map(_.render).mkString("\n"))
  }

  test("equal totals with different labels are reported as a mismatch") {
    val checks = BackendVerification.graphCounts(
      verificationGraph,
      vertexRows = List(ujson.Obj("customer" -> 1, "order" -> 1)),
      edgeRows = List(ujson.Obj("related" -> 1))
    )

    val rendered = checks.map(_.render).mkString("\n")
    assert(checks.head.isInstanceOf[Failed], rendered)
    assert(rendered.contains("customer=1, order=1"), rendered)
    assert(rendered.contains("customer=2"), rendered)
  }

  test("shortest paths compare endpoints and hop count, not one arbitrary tied route") {
    val local  = Some(Path(List("a", "local-middle", "b")))
    val remote = List("a", "server-middle", "b")
    assert(BackendVerification.shortestPath(local, remote).isInstanceOf[Passed])
  }

  test("a backend path outside the local hop count is reported") {
    val check = BackendVerification.shortestPath(Some(Path(List("a", "b"))), List("a", "x", "b"))
    assert(check.isInstanceOf[Failed], check.render)
  }
}
