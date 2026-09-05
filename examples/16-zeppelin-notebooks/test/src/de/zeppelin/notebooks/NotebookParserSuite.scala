package de.zeppelin.notebooks

final class NotebookParserSuite extends munit.FunSuite {

  private val minimalNote =
    """{
      |  "id": "2EXAMPLE01",
      |  "name": "Demo",
      |  "defaultInterpreterGroup": "spark",
      |  "paragraphs": [ { "title": "First", "text": "%md\nhello" } ]
      |}""".stripMargin

  test("a well-formed note yields its identity and its paragraphs") {
    val note = Notebook.parse(minimalNote).fold(failure => fail(failure), identity)
    assertEquals(note.id, "2EXAMPLE01")
    assertEquals(note.name, "Demo")
    assertEquals(note.paragraphs.map(_.title), List(Some("First")))
  }

  test("the required interpreters include the note default and every magic, without duplicates") {
    val note = Notebook("id", "name", "spark", List(Paragraph(None, "%md\na"), Paragraph(None, "%spark.sql\nb")))
    assertEquals(note.requiredInterpreterGroups, List("spark", "md"))
  }

  test("broken JSON is reported rather than thrown") {
    assert(Notebook.parse("{ not json").isLeft)
  }

  test("a note without paragraphs is rejected") {
    assertEquals(
      Notebook.parse("""{"id":"a","name":"b","defaultInterpreterGroup":"md","paragraphs":[]}"""),
      Left("the note has no paragraphs")
    )
  }

  test("a missing field is named in the failure") {
    assertEquals(Notebook.parse("""{"name":"b"}"""), Left("field 'id' is missing"))
  }
}
