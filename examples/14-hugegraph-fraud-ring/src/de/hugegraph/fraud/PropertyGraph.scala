package de.hugegraph.fraud

/**
 * A tiny, storage-independent description of a property graph.
 *
 * A *property graph* is the data model behind HugeGraph, Neo4j, JanusGraph and most other graph databases. It has only
 * two kinds of thing:
 *
 *   - a **vertex** (also called a node): one entity, such as a customer or a payment card. It carries a label saying
 *     what kind of entity it is, an identifier, and a bag of key/value properties.
 *   - an **edge** (also called a relationship): a directed connection from one vertex to another, again with a label
 *     and its own properties.
 *
 * Keeping this description separate from the HugeGraph client is what lets the same graph be loaded into the database
 * *and* be traversed in memory by [[RingDetection]] during unit tests.
 */
object PropertyGraph {

  /** A property value. Only the two types this example needs are modelled. */
  sealed trait Value extends Product with Serializable

  object Value {
    final case class Text(value: String) extends Value
    final case class Number(value: Long) extends Value
  }

  /**
   * One entity in the graph.
   *
   * @param label
   *   the kind of entity, for example `customer`. Must match a vertex label in [[FraudSchema]].
   * @param id
   *   the identifier. This example uses HugeGraph's `CUSTOMIZE_STRING` identifier strategy, which means the loader
   *   chooses the identifier instead of the database generating one. Re-loading the same identifier overwrites the
   *   vertex rather than duplicating it, so the loader is safe to run twice.
   */
  final case class Vertex(label: String, id: String, properties: Map[String, Value])

  /**
   * A directed connection between two vertices.
   *
   * @param outV
   *   identifier of the vertex the edge leaves ("out vertex", the tail of the arrow).
   * @param inV
   *   identifier of the vertex the edge enters ("in vertex", the head of the arrow).
   *
   * HugeGraph's batch endpoint wants the labels of both endpoints alongside their identifiers, which is why they are
   * stored here rather than looked up later.
   */
  final case class Edge(
      label: String,
      outV: String,
      outVLabel: String,
      inV: String,
      inVLabel: String,
      properties: Map[String, Value]
  )

  /** A whole graph: everything the loader has to send, and everything the in-memory analysis reads. */
  final case class Graph(vertices: List[Vertex], edges: List[Edge]) {

    def verticesWithLabel(label: String): List[Vertex] = vertices.filter(_.label == label)

    def edgesWithLabel(label: String): List[Edge] = edges.filter(_.label == label)

    /**
     * The graph seen as undirected: every vertex identifier mapped to the identifiers it is connected to, in either
     * direction.
     *
     * Fraud questions are undirected questions. "Did these two customers use the same card?" does not care that the
     * `paid_with` edge happens to point from the order to the card. Both Gremlin's `both()` step and the algorithms in
     * [[RingDetection]] work on this view.
     */
    def undirectedAdjacency: Map[String, Set[String]] = {
      val empty = vertices.map(vertex => vertex.id -> Set.empty[String]).toMap
      edges.foldLeft(empty) { (adjacency, edge) =>
        adjacency
          .updated(edge.outV, adjacency.getOrElse(edge.outV, Set.empty) + edge.inV)
          .updated(edge.inV, adjacency.getOrElse(edge.inV, Set.empty) + edge.outV)
      }
    }

    /** Vertex identifier to label, used to keep traversals inside the labels a question is about. */
    def labelOf: Map[String, String] = vertices.map(vertex => vertex.id -> vertex.label).toMap
  }

  object Graph {

    /**
     * Builds a graph, removing duplicate vertices and edges.
     *
     * Duplicates are the normal case here rather than a mistake: a hundred orders paid with fifteen cards produce the
     * same card vertex again and again, and de-duplicating once at the boundary keeps every later stage simple.
     */
    def of(vertices: Iterable[Vertex], edges: Iterable[Edge]): Graph =
      Graph(
        vertices.groupBy(_.id).values.map(_.head).toList.sortBy(vertex => (vertex.label, vertex.id)),
        edges.toList.distinct.sortBy(edge => (edge.label, edge.outV, edge.inV))
      )
  }
}
