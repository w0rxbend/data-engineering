package de.hugegraph.fraud

import de.hugegraph.fraud.FraudSchema.{Edges, Vertices}
import de.hugegraph.fraud.PropertyGraph.Graph

import scala.annotation.tailrec
import scala.collection.immutable.Queue

/**
 * The fraud analysis, written as ordinary Scala over the in-memory graph.
 *
 * Every function here has a counterpart in [[GremlinQueries]] that asks HugeGraph the same question. Having both is the
 * point of the example twice over:
 *
 *   - it makes the behaviour testable without Docker, because these functions need nothing but a [[Graph]];
 *   - it shows what a graph database is actually doing, since the traversals below are small enough to read.
 *
 * What a real deployment gains from the database is scale and indexing, not a different answer: a few thousand vertices
 * fit in memory, a few billion do not.
 */
object RingDetection {

  /**
   * A group of customer accounts that are connected to each other through shared artefacts.
   *
   * @param members
   *   the customer identifiers, sorted so the result is stable.
   * @param sharedArtefacts
   *   the card, device and address identifiers that more than one member of this group used. These are the evidence:
   *   they say *why* the accounts were grouped.
   */
  final case class FraudRing(members: List[String], sharedArtefacts: List[String]) {
    def size: Int = members.size
  }

  /** A path through the graph, as the sequence of vertex identifiers it visits. */
  final case class Path(vertices: List[String]) {

    /** The number of edges, which is what "distance" means in a graph. A path of three vertices is two hops long. */
    def hops: Int = math.max(0, vertices.size - 1)
  }

  /**
   * Which customers touched which artefact.
   *
   * The walk is `customer -placed-> order -paid_with|placed_from|ships_to-> artefact`, collapsed so that the order in
   * the middle disappears. Orders are not interesting to a fraud analyst on their own; they are the join.
   */
  def customersByArtefact(graph: Graph): Map[String, Set[String]] = {
    val customerOfOrder: Map[String, String] =
      graph.edgesWithLabel(Edges.Placed).map(edge => edge.inV -> edge.outV).toMap

    val artefactEdges = Edges.fromOrder.flatMap(graph.edgesWithLabel)

    artefactEdges.foldLeft(Map.empty[String, Set[String]]) { (byArtefact, edge) =>
      customerOfOrder.get(edge.outV) match {
        case None           => byArtefact
        case Some(customer) => byArtefact.updated(edge.inV, byArtefact.getOrElse(edge.inV, Set.empty) + customer)
      }
    }
  }

  /** Artefacts used by more than one customer, mapped to the customers that used them. */
  def sharedArtefacts(graph: Graph): Map[String, Set[String]] =
    customersByArtefact(graph).filter { case (_, customers) => customers.size > 1 }

  /**
   * Customer identifier to the other customers it shares at least one artefact with.
   *
   * This is the graph a fraud ring lives in. It is derived from the stored graph rather than stored itself, which is
   * the whole argument for a graph database: nobody has to maintain a "suspiciously related customers" table.
   */
  def customerLinks(graph: Graph): Map[String, Set[String]] =
    sharedArtefacts(graph).values.foldLeft(Map.empty[String, Set[String]]) { (links, customers) =>
      customers.foldLeft(links) { (updated, customer) =>
        updated.updated(customer, updated.getOrElse(customer, Set.empty) ++ (customers - customer))
      }
    }

  /**
   * Every fraud ring of at least `minimumSize` accounts.
   *
   * A ring is a *connected component* of the customer-link graph: a set of accounts reachable from one another, however
   * indirectly. Two accounts that share nothing directly still land in the same ring if a third account bridges them,
   * which is precisely the case that defeats a fixed-depth SQL self-join.
   */
  def rings(graph: Graph, minimumSize: Int = 2): List[FraudRing] = {
    val links     = customerLinks(graph)
    val artefacts = sharedArtefacts(graph)

    @tailrec
    def component(frontier: List[String], seen: Set[String]): Set[String] = frontier match {
      case Nil          => seen
      case head :: rest =>
        val fresh = links.getOrElse(head, Set.empty).diff(seen)
        component(fresh.toList ++ rest, seen ++ fresh)
    }

    val (_, found) = links.keys.toList.sorted.foldLeft((Set.empty[String], List.empty[FraudRing])) {
      case ((visited, rings), start) if visited.contains(start) => (visited, rings)
      case ((visited, rings), start)                            =>
        val members  = component(List(start), Set(start))
        val evidence = artefacts.collect {
          case (artefact, customers) if customers.exists(members.contains) => artefact
        }
        (visited ++ members, FraudRing(members.toList.sorted, evidence.toList.sorted) :: rings)
    }

    found.filter(_.size >= minimumSize).sortBy(ring => (-ring.size, ring.members.headOption.getOrElse("")))
  }

  /**
   * Every vertex within `depth` hops of `start`, not counting `start` itself.
   *
   * This is a breadth-first search, the operation graph databases call a *k-hop neighbourhood*. Depth 4 from a customer
   * reaches the other customers of a shared card, because the walk is customer, order, card, order, customer.
   */
  def kHopNeighbourhood(graph: Graph, start: String, depth: Int): Set[String] = {
    val adjacency = graph.undirectedAdjacency

    @tailrec
    def expand(frontier: Set[String], seen: Set[String], remaining: Int): Set[String] =
      if (remaining <= 0 || frontier.isEmpty) { seen }
      else {
        val next = frontier.flatMap(vertex => adjacency.getOrElse(vertex, Set.empty)).diff(seen)
        expand(next, seen ++ next, remaining - 1)
      }

    expand(Set(start), Set(start), depth) - start
  }

  /**
   * The shortest path between two vertices, or `None` if none exists within `maxDepth` hops.
   *
   * Breadth-first search visits vertices in order of distance, so the first time the target is reached the path found
   * is a shortest one. The predecessor map records how each vertex was first reached, and the path is read back from
   * the target.
   */
  def shortestPath(graph: Graph, from: String, to: String, maxDepth: Int = 6): Option[Path] = {
    val adjacency = graph.undirectedAdjacency

    @tailrec
    def search(
        queue: Queue[(String, Int)],
        seen: Set[String],
        predecessors: Map[String, String]
    ): Option[Map[String, String]] =
      queue.dequeueOption match {
        case None                             => None
        case Some(((vertex, distance), rest)) =>
          if (vertex == to) { Some(predecessors) }
          else if (distance == maxDepth) { search(rest, seen, predecessors) }
          else {
            val unseen = adjacency.getOrElse(vertex, Set.empty).diff(seen).toList.sorted
            search(
              rest ++ unseen.map(next => (next, distance + 1)),
              seen ++ unseen,
              predecessors ++ unseen.map(next => next -> vertex)
            )
          }
      }

    @tailrec
    def unwind(vertex: String, predecessors: Map[String, String], acc: List[String]): List[String] =
      predecessors.get(vertex) match {
        case None              => vertex :: acc
        case Some(predecessor) => unwind(predecessor, predecessors, vertex :: acc)
      }

    if (from == to) { Option.when(adjacency.contains(from))(Path(List(from))) }
    else if (!adjacency.contains(from) || !adjacency.contains(to)) { None }
    else { search(Queue((from, 0)), Set(from), Map.empty).map(predecessors => Path(unwind(to, predecessors, Nil))) }
  }

  /**
   * The most connected vertices of one label, highest first.
   *
   * *Degree centrality* is the simplest measure of how important a vertex is: how many edges touch it. A card with a
   * degree of forty was used by forty orders, and no honest card is. It is a blunt instrument - it ignores who the
   * neighbours are - but it is the right first question to ask of a new graph.
   */
  def degreeCentrality(graph: Graph, label: String, limit: Int): List[(String, Int)] = {
    val adjacency = graph.undirectedAdjacency
    graph
      .verticesWithLabel(label)
      .map(vertex => vertex.id -> adjacency.getOrElse(vertex.id, Set.empty).size)
      .sortBy { case (id, degree) => (-degree, id) }
      .take(limit)
  }

  /** Convenience wrapper used by the report: the most-shared cards, devices and addresses in one list. */
  def busiestArtefacts(graph: Graph, limit: Int): List[(String, Int)] =
    Vertices.sharedArtefacts
      .flatMap(label => degreeCentrality(graph, label, limit))
      .sortBy { case (id, degree) => (-degree, id) }
      .take(limit)
}
