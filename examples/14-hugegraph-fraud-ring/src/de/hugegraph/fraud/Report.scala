package de.hugegraph.fraud

import de.hugegraph.fraud.RingDetection.{FraudRing, Path}
import de.hugegraph.fraud.PropertyGraph.Graph

/**
 * Text rendering of the analysis results.
 *
 * Formatting is kept apart from both the algorithms and the HTTP client so that a test can assert on the exact report
 * text. Every function returns lines rather than printing them, and [[Main]] does the printing.
 */
object Report {

  /** A titled block of lines, with the title underlined so the console output is skimmable. */
  def section(title: String, lines: List[String]): List[String] =
    (title :: "-" * title.length :: (if (lines.isEmpty) { List("(nothing found)") }
                                     else { lines })) :+ ""

  /** One line per detected ring: how many accounts, which ones, and the artefacts that betrayed them. */
  def rings(found: List[FraudRing]): List[String] =
    found.map { ring =>
      f"${ring.size}%2d accounts  ${ring.members.mkString(", ")}%-70s shared: ${ring.sharedArtefacts.mkString(", ")}"
    }

  /** One line per vertex, widest degree first. */
  def degrees(entries: List[(String, Int)]): List[String] =
    entries.map { case (id, degree) => f"$degree%4d edges  $id" }

  /** A path rendered as the walk it is, or a clear statement that there is none. */
  def path(from: String, to: String, found: Option[Path]): List[String] = found match {
    case None        => List(s"no path between $from and $to within the hop budget")
    case Some(value) =>
      List(s"${value.hops} hops", value.vertices.mkString(" -> "))
  }

  /** The same shape as [[path]], for the answer that came back from the server. */
  def serverPath(vertices: List[String]): List[String] =
    if (vertices.isEmpty) { List("no path found by the server within the hop budget") }
    else { List(s"${vertices.size - 1} hops", vertices.mkString(" -> ")) }

  /** Counts returned by a `groupCount().by(label)` traversal, which arrive as a single JSON object. */
  def counts(rows: List[ujson.Value]): List[String] =
    rows.headOption.map(_.obj.toList.sortBy(_._1)).getOrElse(Nil).map { case (label, count) =>
      f"${count.num.toLong}%6d  $label"
    }

  /** Rows of a `project('artefact', 'customers')` traversal. */
  def sharedArtefacts(rows: List[ujson.Value]): List[String] =
    rows.map { row =>
      val artefact  = row("artefact").str
      val customers = row("customers").arr.map(_.str).toList
      f"$artefact%-24s used by ${customers.size}%2d accounts: ${customers.mkString(", ")}"
    }

  /** Rows of a `project('id', 'degree')` traversal. */
  def serverDegrees(rows: List[ujson.Value]): List[String] =
    rows.map(row => f"${row("degree").num.toLong}%4d edges  ${row("id").str}")
}

/** The outcome of comparing a real HugeGraph answer with the local reference calculation. */
enum BackendCheck {
  case Passed(detail: String)
  case Failed(detail: String)

  def render: String = this match {
    case Passed(detail) => s"PASS  $detail"
    case Failed(detail) => s"FAIL  $detail"
  }
}

/**
 * Reconciles selected server results with the in-memory model.
 *
 * The in-memory algorithms are an executable specification, not evidence that HugeGraph stored or traversed anything.
 * These comparisons run only after the HTTP calls return and name both sides of a disagreement, so the console output
 * distinguishes a local expectation from a result computed by the database process.
 */
object BackendVerification {

  def graphCounts(graph: Graph, vertexRows: List[ujson.Value], edgeRows: List[ujson.Value]): List[BackendCheck] =
    List(
      compareCounts("vertex", countsByLabel(graph.vertices.map(_.label)), readCounts(vertexRows)),
      compareCounts("edge", countsByLabel(graph.edges.map(_.label)), readCounts(edgeRows))
    )

  /**
   * Paths may have different intermediate vertices when several shortest paths tie; endpoints and hop count must agree.
   */
  def shortestPath(local: Option[Path], hugeGraph: List[String]): BackendCheck = {
    val localShape  = local.map(path => (path.vertices.headOption, path.vertices.lastOption, path.hops))
    val remoteShape = Option.when(hugeGraph.nonEmpty)(
      (hugeGraph.headOption, hugeGraph.lastOption, hugeGraph.size - 1)
    )

    if (localShape == remoteShape) {
      BackendCheck.Passed(s"HugeGraph shortest path agrees with the in-memory model: ${describePath(hugeGraph)}")
    } else {
      BackendCheck.Failed(
        s"HugeGraph shortest path ${describePath(hugeGraph)}; in-memory model expected ${describePath(local.map(_.vertices).getOrElse(Nil))}"
      )
    }
  }

  private def compareCounts(
      kind: String,
      local: Map[String, Long],
      remote: Either[String, Map[String, Long]]
  ): BackendCheck = remote match {
    case Right(value) if value == local =>
      BackendCheck.Passed(s"HugeGraph $kind counts by label match the in-memory graph: ${describeCounts(value)}")
    case Right(value) =>
      BackendCheck.Failed(
        s"HugeGraph $kind counts by label are ${describeCounts(value)}; " +
          s"in-memory graph expected ${describeCounts(local)}"
      )
    case Left(problem) =>
      BackendCheck.Failed(s"HugeGraph $kind counts could not be read: $problem")
  }

  private def readCounts(rows: List[ujson.Value]): Either[String, Map[String, Long]] = rows match {
    case (obj: ujson.Obj) :: Nil =>
      val entries = obj.value.toList
      if (entries.forall(_._2.isInstanceOf[ujson.Num])) {
        Right(entries.collect { case (label, ujson.Num(value)) => label -> value.toLong }.toMap)
      } else {
        Left("groupCount response contains a non-numeric value")
      }
    case _ => Left(s"expected one groupCount object, received ${rows.size} row(s)")
  }

  private def countsByLabel(labels: List[String]): Map[String, Long] =
    labels.groupMapReduce(identity)(_ => 1L)(_ + _)

  private def describeCounts(counts: Map[String, Long]): String =
    if (counts.isEmpty) { "{}" }
    else { counts.toList.sortBy(_._1).map { case (label, count) => s"$label=$count" }.mkString("{", ", ", "}") }

  private def describePath(vertices: List[String]): String =
    if (vertices.isEmpty) { "no path" }
    else { s"${vertices.size - 1} hops (${vertices.head} -> ${vertices.last})" }
}
