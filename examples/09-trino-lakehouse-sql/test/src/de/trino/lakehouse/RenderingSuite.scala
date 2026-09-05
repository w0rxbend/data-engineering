package de.trino.lakehouse

/** A [[ResultCursor]] over rows held in memory, standing in for a real `java.sql.ResultSet`. */
final class FakeCursor(val columnNames: List[String], rows: List[List[String]]) extends ResultCursor {

  private var remaining = rows

  private var current: List[String] = Nil

  def next(): Boolean = remaining match {
    case head :: tail => current = head; remaining = tail; true
    case Nil          => false
  }

  def valueAt(columnIndex: Int): String = current(columnIndex)
}

final class RenderingSuite extends munit.FunSuite {

  private val newline = System.lineSeparator

  test("reading a cursor collects every row in order") {
    val table = ResultTable.from(new FakeCursor(List("a", "b"), List(List("1", "2"), List("3", "4"))))
    assertEquals(table.columns, List("a", "b"))
    assertEquals(table.rows, List(List("1", "2"), List("3", "4")))
  }

  test("reading an empty cursor yields no rows") {
    assertEquals(ResultTable.from(new FakeCursor(List("a"), Nil)).rowCount, 0)
  }

  test("columns are padded to the width of their widest cell") {
    val table    = ResultTable(List("tier", "orders"), List(List("gold", "12"), List("standard", "7")))
    val expected = List(
      "tier     | orders",
      "---------+-------",
      "gold     | 12",
      "standard | 7"
    ).mkString(newline)
    assertEquals(TableRenderer.render(table), expected)
  }

  test("a very long cell is truncated so the table stays readable") {
    val rendered = TableRenderer.render(ResultTable(List("plan"), List(List("x" * 200))))
    assert(rendered.linesIterator.forall(_.length <= 60), rendered)
  }

  test("line breaks inside a cell are collapsed into spaces") {
    assert(TableRenderer.render(ResultTable(List("c"), List(List("a\nb")))).endsWith("a b"))
  }

  test("the row summary uses the singular for exactly one row") {
    assertEquals(TableRenderer.summary(ResultTable(List("c"), List(List("1")))), "1 row")
    assertEquals(TableRenderer.summary(ResultTable(List("c"), Nil)), "0 rows")
  }

  test("a result with no columns renders as a placeholder") {
    assertEquals(TableRenderer.render(ResultTable.empty), "(no columns)")
  }
}
