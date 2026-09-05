package de.trino.lakehouse

/**
 * Draws a [[ResultTable]] as a fixed-width text table, the way a command line client would.
 *
 * Everything here is a pure function from data to a `String`, which means the formatting can be asserted on in a unit
 * test character by character.
 */
object TableRenderer {

  private val columnSeparator = " | "

  private val nullPlaceholder = "NULL"

  /** Longest cell text this renderer prints before it truncates with an ellipsis. */
  private val maxCellWidth = 60

  def render(table: ResultTable): String =
    if (table.columns.isEmpty) "(no columns)"
    else {
      val cells  = table.columns.map(shorten) :: table.rows.map(_.map(shorten))
      val widths = columnWidths(cells, table.columns.size)
      val header = renderRow(cells.head, widths)
      val rule   = widths.map("-" * _).mkString("-+-")
      val body   = cells.tail.map(renderRow(_, widths))
      (header :: rule :: body).mkString(System.lineSeparator)
    }

  /** A one-line summary such as `3 rows`, printed underneath a rendered table. */
  def summary(table: ResultTable): String =
    if (table.rowCount == 1) "1 row" else s"${table.rowCount} rows"

  private def columnWidths(cells: List[List[String]], columnCount: Int): List[Int] =
    (0 until columnCount).map { index =>
      cells.map(row => row.lift(index).fold(0)(_.length)).max
    }.toList

  private def renderRow(cells: List[String], widths: List[Int]): String =
    cells.zip(widths).map { case (cell, width) => cell.padTo(width, ' ') }.mkString(columnSeparator).stripTrailing

  /**
   * Keeps one cell printable: a missing value becomes `NULL`, line breaks inside a value (Trino's `EXPLAIN` returns a
   * whole query plan in a single cell) become spaces, and very long values are cut short.
   */
  private def shorten(raw: String): String = {
    val flattened = Option(raw).getOrElse(nullPlaceholder).replaceAll("\\s+", " ").trim
    if (flattened.length <= maxCellWidth) flattened else flattened.take(maxCellWidth - 1) + "…"
  }
}
