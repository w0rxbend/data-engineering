package de.trino.lakehouse

/**
 * The outcome of a query, already turned into text.
 *
 * Trino returns many different column types (numbers, dates, arrays). Rendering them is a presentation concern, so this
 * type stores every cell as a `String` and keeps no reference to JDBC (Java Database Connectivity, the standard Java
 * API for talking to a database). That makes the whole rendering path a pure function that a unit test can exercise
 * without a running cluster.
 */
final case class ResultTable(columns: List[String], rows: List[List[String]]) {

  def rowCount: Int = rows.size
}

object ResultTable {

  val empty: ResultTable = ResultTable(Nil, Nil)

  /**
   * Reads a cursor to exhaustion.
   *
   * The cursor is an interface rather than a `java.sql.ResultSet` so that tests can hand in a list of rows. This is the
   * only place that knows how to walk a result, and it knows nothing about where the rows came from.
   */
  def from(cursor: ResultCursor): ResultTable = {
    val columns = cursor.columnNames
    val rows    = List.newBuilder[List[String]]
    while (cursor.next())
      rows += columns.indices.map(cursor.valueAt).toList
    ResultTable(columns, rows.result())
  }
}

/**
 * The smallest view of a query result this example needs: the column names, a way to step to the next row, and a way to
 * read one cell of the current row as text.
 *
 * Depending on an interface this narrow (instead of on JDBC directly) is what keeps `ResultTable.from` testable.
 */
trait ResultCursor {

  def columnNames: List[String]

  /** Advances to the next row; returns false once the result is exhausted. */
  def next(): Boolean

  /** The cell in the given zero-based column of the current row, `NULL` when the database returned no value. */
  def valueAt(columnIndex: Int): String
}
