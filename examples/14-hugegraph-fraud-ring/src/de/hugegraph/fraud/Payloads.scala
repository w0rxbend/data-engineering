package de.hugegraph.fraud

import de.hugegraph.fraud.FraudSchema.*
import de.hugegraph.fraud.GremlinQueries.GremlinQuery
import de.hugegraph.fraud.PropertyGraph.{Edge, Value, Vertex}

/**
 * Turns the schema, the graph and a Gremlin query into the JSON bodies the HugeGraph HTTP interface expects.
 *
 * This file is the only place that knows the wire format, and it is pure: every function takes a value and returns a
 * `ujson.Value`. The tests can therefore check the exact JSON the server would receive without a server being involved.
 */
object Payloads {

  private def value(property: Value): ujson.Value = property match {
    case Value.Text(text)     => ujson.Str(text)
    case Value.Number(number) => ujson.Num(number.toDouble)
  }

  private def properties(entries: Map[String, Value]): ujson.Obj =
    ujson.Obj.from(entries.toList.sortBy(_._1).map { case (key, property) => key -> value(property) })

  /** `POST /graphs/{graph}/schema/propertykeys` */
  def propertyKey(key: PropertyKey): ujson.Value =
    ujson.Obj("name" -> key.name, "data_type" -> key.dataType.wireName, "cardinality" -> "SINGLE")

  /**
   * `POST /graphs/{graph}/schema/vertexlabels`
   *
   * `enable_label_index` makes "give me every customer" a lookup rather than a full scan. It costs write throughput,
   * which is a trade worth making for a reference example and worth measuring in production.
   */
  def vertexLabel(label: VertexLabel): ujson.Value =
    ujson.Obj(
      "name"               -> label.name,
      "id_strategy"        -> label.idStrategy,
      "properties"         -> label.properties,
      "nullable_keys"      -> ujson.Arr(),
      "enable_label_index" -> true
    )

  /** `POST /graphs/{graph}/schema/edgelabels` */
  def edgeLabel(label: EdgeLabel): ujson.Value =
    ujson.Obj(
      "name"               -> label.name,
      "source_label"       -> label.sourceLabel,
      "target_label"       -> label.targetLabel,
      "frequency"          -> label.frequency,
      "properties"         -> label.properties,
      "sort_keys"          -> ujson.Arr(),
      "nullable_keys"      -> ujson.Arr(),
      "enable_label_index" -> true
    )

  /** `POST /graphs/{graph}/schema/indexlabels` */
  def indexLabel(label: IndexLabel): ujson.Value =
    ujson.Obj(
      "name"       -> label.name,
      "base_type"  -> label.baseType,
      "base_value" -> label.baseValue,
      "index_type" -> label.indexType,
      "fields"     -> label.fields
    )

  /** One element of the JSON array `POST /graphs/{graph}/graph/vertices/batch` takes. */
  def vertex(vertex: Vertex): ujson.Value =
    ujson.Obj("id" -> vertex.id, "label" -> vertex.label, "properties" -> properties(vertex.properties))

  /**
   * One element of the JSON array `POST /graphs/{graph}/graph/edges/batch` takes.
   *
   * `outV`/`inV` are the endpoint identifiers and `outVLabel`/`inVLabel` their labels. HugeGraph asks for the labels
   * too so that it can validate the edge against the schema without first loading both vertices.
   */
  def edge(edge: Edge): ujson.Value =
    ujson.Obj(
      "label"      -> edge.label,
      "outV"       -> edge.outV,
      "outVLabel"  -> edge.outVLabel,
      "inV"        -> edge.inV,
      "inVLabel"   -> edge.inVLabel,
      "properties" -> properties(edge.properties)
    )

  /**
   * `POST /gremlin`
   *
   * `aliases` is the HugeGraph-specific part: one server can host several graphs, so the request has to say which graph
   * the names `graph` and `g` refer to. The traversal source of a graph called `hugegraph` is always `__g_hugegraph`.
   */
  def gremlinRequest(query: GremlinQuery, graph: String): ujson.Value =
    ujson.Obj(
      "gremlin"  -> query.script,
      "bindings" -> ujson.Obj.from(query.bindings.toList.sortBy(_._1).map { case (k, v) => k -> ujson.Str(v) }),
      "language" -> "gremlin-groovy",
      "aliases"  -> ujson.Obj("graph" -> graph, "g" -> s"__g_$graph")
    )
}
