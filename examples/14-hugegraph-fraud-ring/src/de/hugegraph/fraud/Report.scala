package de.hugegraph.fraud

import de.hugegraph.fraud.RingDetection.{FraudRing, Path}

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
