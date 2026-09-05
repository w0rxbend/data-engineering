package de.zeppelin.notebooks

final class NotebookCheckSuite extends munit.FunSuite {

  private val config = InterpreterConfig(
    List(
      InterpreterSetting("spark", "spark", Map.empty),
      InterpreterSetting("md", "md", Map.empty)
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
}
