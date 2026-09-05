package de.zeppelin.notebooks

final class NotebookCheckSuite extends munit.FunSuite {

  private val config = InterpreterConfig(
    List(
      InterpreterSetting("spark", "spark", Map.empty, Set("spark", "sql")),
      InterpreterSetting("md", "md", Map.empty, Set("md"))
    )
  )

  private val note = Notebook("2DEMO0001", "Demo", "spark", List(Paragraph(None, "%md\nhello")))

  test("a sound notebook reports no problems") {
    assertEquals(NotebookCheck.problems("Demo_2DEMO0001.zpln", note, config), Nil)
  }

  test("a magic naming an interpreter the stack does not configure is reported") {
    val broken  = note.copy(paragraphs = List(Paragraph(None, "%flink\nenv")))
    val message = NotebookCheck.interpreterProblems(broken, config).mkString
    assert(message.startsWith("interpreter 'flink' is used but not configured"), message)
  }

  test("a file name that disagrees with the note inside it is reported") {
    val message = NotebookCheck.fileNameProblems("Something Else_2DEMO0001.zpln", note).mkString
    assert(message.contains("expected 'Demo_2DEMO0001.zpln'"), message)
  }

  test("a dotted magic must name an interpreter that exists inside its setting") {
    val broken  = note.copy(paragraphs = List(Paragraph(None, "%spark.typo\nSELECT 1")))
    val message = NotebookCheck.interpreterProblems(broken, config).mkString
    assert(message.contains("%spark.typo"), message)
    assert(message.contains("spark, sql"), message)
  }

  test("malformed leading magic syntax is reported instead of falling back to the default") {
    List("%spark.sql.extra", "%spark.", "%spark..", "%.sql").foreach { magic =>
      val broken = note.copy(paragraphs = List(Paragraph(None, s"$magic\nSELECT 1")))
      assertEquals(
        NotebookCheck.magicSyntaxProblems(broken),
        List("paragraph 1 starts with an invalid interpreter magic"),
        magic
      )
    }
  }
}
