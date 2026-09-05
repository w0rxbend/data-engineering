package de.hugegraph.fraud

import de.hugegraph.fraud.FraudSchema.Vertices
import de.hugegraph.fraud.PropertyGraph.Graph

import scala.util.control.NonFatal

/**
 * Wiring: build the graph, push it into HugeGraph, ask both sides the same questions, print the answers.
 *
 * Nothing here decides anything. The schema lives in [[FraudSchema]], the modelling in [[ShopGraph]], the analysis in
 * [[RingDetection]] and [[GremlinQueries]], and the formatting in [[Report]]. Keeping this file free of logic is what
 * lets the rest of the example be unit tested without a server.
 *
 * Configuration comes from environment variables so that the same jar runs against the Docker stack, against a server
 * on another host, or against a differently named graph:
 *
 *   - `HUGEGRAPH_URL` - default `http://localhost:11400`, the port the compose file publishes.
 *   - `HUGEGRAPH_GRAPH` - default `hugegraph`, the name of the graph inside the server.
 *   - `ORDER_COUNT` - default 400, how many background orders to generate.
 */
object Main {

  private val defaultUrl        = "http://localhost:11400"
  private val defaultGraph      = "hugegraph"
  private val defaultOrderCount = 400

  /**
   * The two ends of the merged ring, used to demonstrate the shortest-path query.
   *
   * `cust-0201` belongs to the device farm and `cust-0302` to the drop address. They share nothing at all with each
   * other; they are connected only because `cust-0203` is in both groups. Finding the path between them means walking
   * through three other accounts, which is exactly the work a graph database exists to do.
   */
  private val probeAccounts = ("cust-0201", "cust-0302")

  def main(args: Array[String]): Unit = {
    val url        = sys.env.getOrElse("HUGEGRAPH_URL", defaultUrl)
    val graphName  = sys.env.getOrElse("HUGEGRAPH_GRAPH", defaultGraph)
    val orderCount = sys.env.get("ORDER_COUNT").flatMap(_.toIntOption).getOrElse(defaultOrderCount)

    val graph = ShopGraph.sample(orderCount)
    offline(graph).foreach(println)

    val client = HugeGraphClient.open(url, graphName)
    try {
      awaitServer(client, url)
      online(client, graph).foreach(println)
    } finally client.close()
  }

  /**
   * The analysis run in memory, before the server is involved at all.
   *
   * This half of the report is the answer the tests assert on. Printing it first means the numbers below it can be read
   * as a confirmation that the database agrees.
   */
  private def offline(graph: Graph): List[String] = {
    val (from, to) = probeAccounts
    Report.section(
      "Generated graph",
      List(
        f"${graph.vertices.size}%6d vertices",
        f"${graph.edges.size}%6d edges"
      )
    ) ++
      Report.section("Fraud rings found in memory", Report.rings(RingDetection.rings(graph))) ++
      Report.section("Most reused artefacts", Report.degrees(RingDetection.busiestArtefacts(graph, 5))) ++
      Report.section(
        s"Shortest path $from -> $to, in memory",
        Report.path(from, to, RingDetection.shortestPath(graph, from, to, maxDepth = 8))
      )
  }

  /** The same questions, answered by the server. */
  private def online(client: HugeGraphClient, graph: Graph): List[String] = {
    val (from, to) = probeAccounts

    val schemaReport = client.createSchema(FraudSchema.shop)
    val loadReport   = client.load(graph)

    val vertexCounts = client.gremlin(GremlinQueries.vertexCountByLabel)
    val edgeCounts   = client.gremlin(GremlinQueries.edgeCountByLabel)
    val serverPath   = client.shortestPath(from, to, maxDepth = 8)
    val localPath    = RingDetection.shortestPath(graph, from, to, maxDepth = 8)
    val verification =
      BackendVerification.graphCounts(graph, vertexCounts, edgeCounts) :+
        BackendVerification.shortestPath(localPath, serverPath)

    val sharedArtefactLines = Vertices.sharedArtefacts.flatMap { label =>
      Report.sharedArtefacts(client.gremlin(GremlinQueries.sharedArtefacts(label)))
    }

    Report.section(
      s"HugeGraph ${client.serverVersion()}",
      List(
        s"schema: ${schemaReport.created} elements created, ${schemaReport.alreadyPresent} already present",
        s"loaded: ${loadReport.vertices} vertices, ${loadReport.edges} edges"
      )
    ) ++
      Report.section("Backend verification (HugeGraph versus in-memory reference)", verification.map(_.render)) ++
      Report.section(
        "Vertices per label (Gremlin)",
        Report.counts(vertexCounts)
      ) ++
      Report.section("Edges per label (Gremlin)", Report.counts(edgeCounts)) ++
      Report.section("Artefacts shared by several accounts (Gremlin)", sharedArtefactLines) ++
      Report.section(
        s"Accounts related to $from within 4 hops (Gremlin)",
        client.gremlin(GremlinQueries.relatedAccounts("source", from, depth = 4)).map(_.str)
      ) ++
      Report.section(
        "Highest-degree cards (Gremlin)",
        Report.serverDegrees(client.gremlin(GremlinQueries.degreeCentrality(Vertices.Card, limit = 5)))
      ) ++
      Report.section(
        s"Vertices exactly 4 hops from $from (built-in kout traverser)",
        client.kOut(from, depth = 4, limit = 20)
      ) ++
      Report.section(
        s"Shortest path $from -> $to (built-in shortestpath traverser)",
        Report.serverPath(serverPath)
      )
  }

  /**
   * Waits for the server to answer `/versions`.
   *
   * The compose file already has a health check, so this loop only matters when the program is pointed at a server
   * started by hand. It retries for a minute and then gives up with a message that says what to check.
   */
  private def awaitServer(client: HugeGraphClient, url: String): Unit = {
    val attempts                      = 30
    def attempt(remaining: Int): Unit =
      try {
        client.serverVersion()
        ()
      } catch {
        case NonFatal(_) if remaining > 0 =>
          Thread.sleep(2000L)
          attempt(remaining - 1)
        case NonFatal(cause) =>
          throw new HugeGraphException(
            s"HugeGraph at $url did not become reachable. Is the compose stack up? Cause: ${cause.getMessage}"
          )
      }
    attempt(attempts)
  }
}
