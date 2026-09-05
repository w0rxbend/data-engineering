package de.hugegraph.fraud

import de.hugegraph.fraud.FraudSchema.{Edges, Properties, Vertices}

/**
 * The Gremlin queries this example sends to HugeGraph.
 *
 * **Gremlin** is the query language of Apache TinkerPop, the graph API that HugeGraph implements. Where SQL describes a
 * result set, Gremlin describes a *walk*: `g.V()` starts at every vertex, `.hasLabel('customer')` keeps the customers,
 * `.out('placed')` follows every outgoing `placed` edge, and so on. Each step feeds the next, so a traversal reads left
 * to right in the order a person would trace it with a finger.
 *
 * Building the queries as strings in one place, rather than scattering them through the client, keeps them unit
 * testable: the tests below assert on the exact text, so a typo fails the build instead of a demo.
 */
object GremlinQueries {

  /**
   * A query plus the values it refers to.
   *
   * Values arrive as **bindings** - named parameters the server substitutes - rather than being pasted into the script.
   * That is the same discipline as a prepared statement in SQL: a customer identifier containing a quote cannot end the
   * string and start being code.
   */
  final case class GremlinQuery(script: String, bindings: Map[String, String] = Map.empty)

  /** Renders a compile-time-known constant as a Gremlin string literal, escaping the characters that would break it. */
  def literal(value: String): String =
    "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

  /** Renders several constants as the comma-separated argument list that steps such as `hasLabel` accept. */
  def literals(values: List[String]): String = values.map(literal).mkString(", ")

  /** Which edge label connects an order to each kind of shared artefact. */
  val artefactEdge: Map[String, String] = Map(
    Vertices.Card    -> Edges.PaidWith,
    Vertices.Device  -> Edges.PlacedFrom,
    Vertices.Address -> Edges.ShipsTo
  )

  /** How many vertices of each label the graph holds - the first thing to run after a load. */
  val vertexCountByLabel: GremlinQuery =
    GremlinQuery("g.V().groupCount().by(label)")

  /** How many edges of each label the graph holds. */
  val edgeCountByLabel: GremlinQuery =
    GremlinQuery("g.E().groupCount().by(label)")

  /**
   * Artefacts of one kind that more than one customer used, with the customers listed.
   *
   * The walk, read left to right: take every card; keep only those where walking *backwards* along `paid_with` to the
   * orders and backwards along `placed` to the customers yields more than one distinct customer; then report the card
   * identifier and that customer list side by side.
   *
   * `project(...).by(...).by(...)` is Gremlin's way of building a small record: one `by` per named field, in order.
   */
  def sharedArtefacts(artefactLabel: String, minimumCustomers: Int = 2): GremlinQuery = {
    val edge      = artefactEdge(artefactLabel)
    val customers = s"__.in(${literal(edge)}).in(${literal(Edges.Placed)})"
    GremlinQuery(
      s"g.V().hasLabel(${literal(artefactLabel)})" +
        s".where($customers.dedup().count().is(gte(${minimumCustomers})))" +
        ".project('artefact', 'customers')" +
        ".by(id())" +
        s".by($customers.values(${literal(Properties.CustomerId)}).dedup().order().fold())" +
        ".order().by(select('artefact'))"
    )
  }

  /**
   * Other customer accounts within `depth` hops of one account.
   *
   * `repeat(...).times(n).emit()` is the k-hop neighbourhood: walk `both()` (edges in either direction) up to `n` times
   * and emit everything seen on the way. `simplePath()` forbids revisiting a vertex already on the current path, which
   * is what stops the traversal bouncing between two vertices forever.
   *
   * Four hops is the interesting depth in this schema, because that is the distance between two customers sharing an
   * artefact: customer, order, artefact, order, customer.
   */
  def relatedAccounts(customerBinding: String, customerId: String, depth: Int): GremlinQuery =
    GremlinQuery(
      s"g.V($customerBinding).repeat(__.both().simplePath()).times($depth).emit()" +
        s".hasLabel(${literal(Vertices.Customer)}).dedup()" +
        s".values(${literal(Properties.CustomerId)}).order()",
      Map(customerBinding -> customerId)
    )

  /**
   * The most connected vertices of one label.
   *
   * `bothE().count()` counts the edges touching a vertex regardless of direction; ordering by it descending is degree
   * centrality. In this graph a high-degree card is a card many separate orders were charged to.
   */
  def degreeCentrality(label: String, limit: Int): GremlinQuery =
    GremlinQuery(
      s"g.V().hasLabel(${literal(label)})" +
        ".project('id', 'degree').by(id()).by(__.bothE().count())" +
        s".order().by('degree', desc).limit($limit)"
    )

  /**
   * The shortest connection between two accounts, as the identifiers it passes through.
   *
   * `until(...)` stops the repeat when the target is reached or the hop budget is spent; `path().by(id())` renders the
   * whole walk instead of only its endpoint. Because `repeat` explores breadth first, the first path found is a
   * shortest one.
   */
  def shortestPath(
      sourceBinding: String,
      sourceId: String,
      targetBinding: String,
      targetId: String,
      maxDepth: Int
  ): GremlinQuery =
    GremlinQuery(
      s"g.V($sourceBinding).repeat(__.both().simplePath())" +
        s".until(__.hasId($targetBinding).or().loops().is(gte($maxDepth)))" +
        s".hasId($targetBinding).limit(1).path().by(id())",
      Map(sourceBinding -> sourceId, targetBinding -> targetId)
    )
}
