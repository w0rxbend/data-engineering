package de.hugegraph.fraud

import de.hugegraph.fraud.FraudSchema.Vertices

/** Tests for the Gremlin query builder. The expected text is written out in full so a typo cannot slip through. */
class GremlinQueriesSuite extends munit.FunSuite {

  test("a literal is single-quoted") {
    assertEquals(GremlinQueries.literal("customer"), "'customer'")
  }

  test("a literal escapes the quote and backslash that would otherwise end it") {
    assertEquals(GremlinQueries.literal("o'brien"), """'o\'brien'""")
    assertEquals(GremlinQueries.literal("""back\slash"""), """'back\\slash'""")
  }

  test("several literals become a comma-separated argument list") {
    assertEquals(GremlinQueries.literals(List("card", "device")), "'card', 'device'")
  }

  test("the shared-artefact query walks back from the artefact to the customers") {
    assertEquals(
      GremlinQueries.sharedArtefacts(Vertices.Card).script,
      "g.V().hasLabel('card')" +
        ".where(__.in('paid_with').in('placed').dedup().count().is(gte(2)))" +
        ".project('artefact', 'customers')" +
        ".by(id())" +
        ".by(__.in('paid_with').in('placed').values('customer_id').dedup().order().fold())" +
        ".order().by(select('artefact'))"
    )
  }

  test("each shared artefact label uses the edge label that reaches it from an order") {
    assert(GremlinQueries.sharedArtefacts(Vertices.Device).script.contains("__.in('placed_from')"))
    assert(GremlinQueries.sharedArtefacts(Vertices.Address).script.contains("__.in('ships_to')"))
  }

  test("the related-accounts query passes the identifier as a binding, never inline") {
    val query = GremlinQueries.relatedAccounts("source", "cust-0101", depth = 4)

    assertEquals(
      query.script,
      "g.V(source).repeat(__.both().simplePath()).times(4).emit()" +
        ".hasLabel('customer').dedup()" +
        ".values('customer_id').order()"
    )
    assertEquals(query.bindings, Map("source" -> "cust-0101"))
    assert(!query.script.contains("cust-0101"), "the identifier must not be pasted into the script")
  }

  test("an identifier containing a quote stays out of the script entirely") {
    val query = GremlinQueries.relatedAccounts("source", "cust-'); g.V().drop(); //", depth = 2)
    assert(!query.script.contains("drop()"))
  }

  test("the degree-centrality query orders by the counted degree") {
    assertEquals(
      GremlinQueries.degreeCentrality(Vertices.Card, limit = 5).script,
      "g.V().hasLabel('card')" +
        ".project('id', 'degree').by(id()).by(__.bothE().count())" +
        ".order().by('degree', desc).limit(5)"
    )
  }

  test("the shortest-path query stops at the target or at the hop budget") {
    val query = GremlinQueries.shortestPath("source", "cust-1", "target", "cust-2", maxDepth = 6)

    assertEquals(
      query.script,
      "g.V(source).repeat(__.both().simplePath())" +
        ".until(__.hasId(target).or().loops().is(gte(6)))" +
        ".hasId(target).limit(1).path().by(id())"
    )
    assertEquals(query.bindings, Map("source" -> "cust-1", "target" -> "cust-2"))
  }

  test("counting queries need no bindings") {
    assertEquals(GremlinQueries.vertexCountByLabel.bindings, Map.empty[String, String])
    assertEquals(GremlinQueries.edgeCountByLabel.script, "g.E().groupCount().by(label)")
  }
}
