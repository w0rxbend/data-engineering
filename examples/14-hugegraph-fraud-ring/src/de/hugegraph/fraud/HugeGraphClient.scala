package de.hugegraph.fraud

import de.hugegraph.fraud.FraudSchema.Schema
import de.hugegraph.fraud.GremlinQueries.GremlinQuery
import de.hugegraph.fraud.PropertyGraph.Graph
import sttp.client4.*
import sttp.client4.httpclient.HttpClientSyncBackend
import sttp.model.Uri

/** Raised when HugeGraph answers with something other than success. */
final class HugeGraphException(message: String) extends RuntimeException(message)

/**
 * A thin, direct-style client for the HugeGraph HTTP interface.
 *
 * "Direct style" means every call blocks and returns its result as an ordinary value: `client.gremlin(query)` gives
 * back a `ujson.Value`, not a `Future` or an `IO`. This program is a single-threaded script that runs a handful of
 * queries in order, so there is no concurrency for an effect type to manage, and the code reads like the shell session
 * it replaces.
 *
 * The class deliberately stops at "send this JSON, hand back that JSON". Everything with an opinion - the schema, the
 * graph, the queries - lives in the pure files next to it.
 */
final class HugeGraphClient(baseUri: Uri, graph: String, backend: SyncBackend) extends AutoCloseable {

  /** HugeGraph refuses batches above a server-side limit; 200 keeps every request comfortably below it. */
  private val batchSize = 200

  private def graphUri(segments: String*): Uri = baseUri.addPath("graphs" +: graph +: segments)

  /**
   * Sends a request and returns the body, failing loudly on any non-2xx status.
   *
   * `asStringAlways` is used rather than sttp's default because HugeGraph puts a useful explanation in the body of an
   * error response, and throwing that explanation away would make every failure look the same.
   */
  private def send(request: Request[String], description: String): String = {
    val response = request.send(backend)
    if (response.code.isSuccess) { response.body }
    else { throw new HugeGraphException(s"$description failed with HTTP ${response.code}: ${response.body}") }
  }

  private def postJson(uri: Uri, body: ujson.Value, description: String): String =
    send(
      basicRequest.post(uri).body(ujson.write(body)).contentType("application/json").response(asStringAlways),
      description
    )

  /** The server's own version banner, used as a readiness check and printed in the report. */
  def serverVersion(): String = {
    val body = send(basicRequest.get(baseUri.addPath("versions")).response(asStringAlways), "GET /versions")
    ujson.read(body)("versions")("core").str
  }

  /**
   * Creates every schema element, in the order property keys, vertex labels, edge labels, index labels.
   *
   * Re-running the loader is a normal thing to do while experimenting, so an "already existed" answer is treated as
   * success. HugeGraph reports it as HTTP 400 with an `ExistedException` in the body, which is why the check looks at
   * the body rather than only at the status code.
   */
  def createSchema(schema: Schema): SchemaReport = {
    def create(path: String, payloads: List[ujson.Value], kind: String): (Int, Int) =
      payloads.foldLeft((0, 0)) { case ((created, existing), payload) =>
        val response = basicRequest
          .post(graphUri("schema", path))
          .body(ujson.write(payload))
          .contentType("application/json")
          .response(asStringAlways)
          .send(backend)

        if (response.code.isSuccess) { (created + 1, existing) }
        else if (response.body.contains("ExistedException")) { (created, existing + 1) }
        else { throw new HugeGraphException(s"creating $kind failed with HTTP ${response.code}: ${response.body}") }
      }

    val keys     = create("propertykeys", schema.propertyKeys.map(Payloads.propertyKey), "property key")
    val vertices = create("vertexlabels", schema.vertexLabels.map(Payloads.vertexLabel), "vertex label")
    val edges    = create("edgelabels", schema.edgeLabels.map(Payloads.edgeLabel), "edge label")
    val indexes  = create("indexlabels", schema.indexLabels.map(Payloads.indexLabel), "index label")

    SchemaReport(
      created = keys._1 + vertices._1 + edges._1 + indexes._1,
      alreadyPresent = keys._2 + vertices._2 + edges._2 + indexes._2
    )
  }

  /**
   * Loads a whole graph, vertices first.
   *
   * Vertices must come first because an edge names its endpoints, and HugeGraph rejects an edge whose vertices do not
   * exist yet. Both endpoints accept a JSON array, and both are idempotent for this schema: the vertex identifiers are
   * chosen by the loader, and the edge labels have `SINGLE` frequency, so a second run updates rather than duplicates.
   */
  def load(graphData: Graph): LoadReport = {
    val vertexCount = graphData.vertices
      .grouped(batchSize)
      .map { batch =>
        postJson(graphUri("graph", "vertices", "batch"), ujson.Arr.from(batch.map(Payloads.vertex)), "vertex batch")
        batch.size
      }
      .sum

    val edgeCount = graphData.edges
      .grouped(batchSize)
      .map { batch =>
        postJson(graphUri("graph", "edges", "batch"), ujson.Arr.from(batch.map(Payloads.edge)), "edge batch")
        batch.size
      }
      .sum

    LoadReport(vertexCount, edgeCount)
  }

  /**
   * Runs a Gremlin traversal and returns the rows it produced.
   *
   * A HugeGraph Gremlin response wraps the answer as `{"result": {"data": [...]}}`; only that inner array is
   * interesting here.
   */
  def gremlin(query: GremlinQuery): List[ujson.Value] = {
    val body = postJson(baseUri.addPath("gremlin"), Payloads.gremlinRequest(query, graph), "gremlin query")
    ujson.read(body)("result")("data").arr.toList
  }

  /**
   * The built-in k-hop traverser: every vertex exactly `depth` hops away from `source`.
   *
   * HugeGraph ships a set of ready-made traversal endpoints under `/traversers/` for the walks people ask for most
   * often. They do the same thing a Gremlin query would, but they run inside the server as tuned Java rather than as an
   * interpreted script, and they take a hop and result budget so a mistake cannot walk the whole graph.
   */
  def kOut(source: String, depth: Int, limit: Int = 100): List[String] = {
    val uri = graphUri("traversers", "kout")
      .addParam("source", jsonId(source))
      .addParam("max_depth", depth.toString)
      .addParam("limit", limit.toString)
    val body = send(basicRequest.get(uri).response(asStringAlways), "kout traversal")
    ujson.read(body)("vertices").arr.map(_.str).toList
  }

  /** The built-in shortest-path traverser. An empty list means no path within `maxDepth` hops. */
  def shortestPath(source: String, target: String, maxDepth: Int): List[String] = {
    val uri = graphUri("traversers", "shortestpath")
      .addParam("source", jsonId(source))
      .addParam("target", jsonId(target))
      .addParam("max_depth", maxDepth.toString)
    val body = send(basicRequest.get(uri).response(asStringAlways), "shortest path traversal")
    ujson.read(body)("path").arr.map(_.str).toList
  }

  /**
   * Vertex identifiers travel in a query string as JSON, so a string identifier has to arrive quoted. Without the
   * quotes HugeGraph reads `cust-0101` as a number and reports that the vertex does not exist.
   */
  private def jsonId(id: String): String = ujson.write(ujson.Str(id))

  override def close(): Unit = backend.close()
}

/** How much of the schema this run had to create, and how much was already there from a previous run. */
final case class SchemaReport(created: Int, alreadyPresent: Int)

/** How many vertices and edges were sent. */
final case class LoadReport(vertices: Int, edges: Int)

object HugeGraphClient {

  /**
   * Opens a client against a running server. The caller closes it, which also closes the underlying HTTP client.
   *
   * The backend is pinned to HTTP/1.1. Java's built-in HTTP client, which sttp uses here, otherwise offers to upgrade
   * every connection to HTTP/2, and HugeGraph 1.5.0 accepts that offer on `/gremlin` without ever answering: the
   * request sits there until the server's own query timeout fires and reports a `TimeoutException`. Every other
   * endpoint in this example is unaffected, which makes the symptom look like a slow query rather than a protocol
   * mismatch. Asking for HTTP/1.1 costs nothing here and removes the whole class of problem.
   */
  def open(baseUrl: String, graph: String): HugeGraphClient = {
    val httpClient = java.net.http.HttpClient
      .newBuilder()
      .version(java.net.http.HttpClient.Version.HTTP_1_1)
      .build()
    new HugeGraphClient(uri"$baseUrl", graph, HttpClientSyncBackend.usingClient(httpClient))
  }
}
