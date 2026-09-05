package de.trino.lakehouse

import scala.annotation.tailrec

/**
 * One statement out of a `.sql` file, together with the comment lines that introduced it.
 *
 * Keeping the leading comments means the runner can print the explanation an author wrote above a query right before
 * showing that query's result.
 */
final case class SqlStatement(description: List[String], sql: String)

/**
 * Splits the text of a `.sql` file into the individual statements to send to Trino.
 *
 * A JDBC (Java Database Connectivity) driver executes exactly one statement per call, so a file holding several
 * statements has to be cut apart first. Doing that in a pure function - text in, statements out - means the rules can
 * be pinned down by unit tests instead of being discovered against a live cluster.
 *
 * The rules are deliberately small and explicit:
 *   - a semicolon ends a statement, unless it sits inside a single-quoted string literal;
 *   - a line starting with `--` is a comment: it is remembered as the description of the statement that follows and is
 *     not sent to the server;
 *   - blank lines separate one comment block from the next.
 */
object SqlScript {

  private val commentPrefix = "--"

  def parse(text: String): List[SqlStatement] = {
    val (statements, trailing) = text.linesIterator.foldLeft((List.empty[SqlStatement], ParseState.empty)) {
      case ((done, state), line) => consume(done, state, line)
    }
    (statements ++ trailing.finish).toList
  }

  private def consume(
      done: List[SqlStatement],
      state: ParseState,
      line: String
  ): (List[SqlStatement], ParseState) = {
    val trimmed = line.trim
    if (trimmed.startsWith(commentPrefix) && state.body.isEmpty) {
      (done, state.addComment(trimmed.drop(commentPrefix.length).trim))
    } else if (trimmed.isEmpty && state.body.isEmpty) {
      (done, ParseState.empty)
    } else {
      absorb(done, state, line)
    }
  }

  /**
   * Adds one line to the statement being collected, closing off a statement at every semicolon on that line. A single
   * line can legally hold more than one statement, hence the recursion over the remainder.
   */
  @tailrec
  private def absorb(
      done: List[SqlStatement],
      state: ParseState,
      line: String
  ): (List[SqlStatement], ParseState) =
    indexOfTerminator(line) match {
      case None           => (done, state.addLine(line))
      case Some(position) =>
        val sql       = (state.body :+ line.take(position)).mkString("\n").trim
        val completed = if (sql.isEmpty) done else done :+ SqlStatement(state.comments, sql)
        val remainder = line.drop(position + 1)
        if (remainder.trim.isEmpty) (completed, ParseState.empty)
        else absorb(completed, ParseState.empty, remainder)
    }

  /** Position of the first semicolon that is not inside a single-quoted literal, if there is one. */
  private def indexOfTerminator(line: String): Option[Int] = {
    var insideLiteral = false
    var index         = 0
    var found         = -1
    while (index < line.length && found < 0) {
      line.charAt(index) match {
        case '\''                  => insideLiteral = !insideLiteral
        case ';' if !insideLiteral => found = index
        case _                     => ()
      }
      index += 1
    }
    Option.when(found >= 0)(found)
  }

  /** Comment lines seen so far plus the statement lines collected so far. */
  private final case class ParseState(comments: List[String], body: List[String]) {

    def addComment(text: String): ParseState = copy(comments = comments :+ text)

    def addLine(text: String): ParseState = copy(body = body :+ text)

    /** A file may end without a final semicolon; whatever is left then still counts as a statement. */
    def finish: Option[SqlStatement] = {
      val sql = body.mkString("\n").trim
      Option.when(sql.nonEmpty)(SqlStatement(comments, sql))
    }
  }

  private object ParseState {
    val empty: ParseState = ParseState(Nil, Nil)
  }
}
