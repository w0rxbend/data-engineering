package de.trino.lakehouse

/** The statement splitter is pure text handling, so every rule it implements can be pinned down here. */
final class SqlScriptSuite extends munit.FunSuite {

  test("splits a file into one statement per semicolon") {
    val parsed = SqlScript.parse("SELECT 1;\nSELECT 2;\n")
    assertEquals(parsed.map(_.sql), List("SELECT 1", "SELECT 2"))
  }

  test("keeps the leading comment lines as the description of the statement below them") {
    val parsed = SqlScript.parse("-- counts the orders\n-- of every country\nSELECT country FROM orders;\n")
    assertEquals(parsed.map(_.description), List(List("counts the orders", "of every country")))
    assertEquals(parsed.map(_.sql), List("SELECT country FROM orders"))
  }

  test("a blank line starts a new comment block") {
    val parsed = SqlScript.parse("-- stale\n\n-- current\nSELECT 1;\n")
    assertEquals(parsed.map(_.description), List(List("current")))
  }

  test("a semicolon inside a string literal does not end the statement") {
    val parsed = SqlScript.parse("SELECT 'a;b' AS text;\n")
    assertEquals(parsed.map(_.sql), List("SELECT 'a;b' AS text"))
  }

  test("keeps the line structure of a multi line statement") {
    val parsed = SqlScript.parse("SELECT a,\n       b\nFROM t;\n")
    assertEquals(parsed.map(_.sql), List("SELECT a,\n       b\nFROM t"))
  }

  test("two statements on one line are both recognised") {
    val parsed = SqlScript.parse("SELECT 1; SELECT 2;")
    assertEquals(parsed.map(_.sql), List("SELECT 1", "SELECT 2"))
  }

  test("a trailing statement without a closing semicolon is still returned") {
    val parsed = SqlScript.parse("SELECT 1")
    assertEquals(parsed.map(_.sql), List("SELECT 1"))
  }

  test("an empty file yields no statements") {
    assertEquals(SqlScript.parse("\n-- nothing here\n\n"), Nil)
  }

  test("every shipped SQL file parses into at least one statement") {
    SqlLibrary.fileNames.foreach { fileName =>
      assert(SqlLibrary.load(fileName).nonEmpty, s"$fileName produced no statements")
    }
  }
}
