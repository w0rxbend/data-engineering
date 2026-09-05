package de.zeppelin.notebooks

final class InterpreterConfigSuite extends munit.FunSuite {

  private val json =
    """{
      |  "interpreterSettings": {
      |    "md": {
      |      "name": "md",
      |      "group": "md",
      |      "properties": { "markdown.parser.type": { "name": "markdown.parser.type", "value": "flexmark" } },
      |      "interpreterGroup": [
      |        { "name": "md", "class": "org.apache.zeppelin.markdown.Markdown" }
      |      ]
      |    }
      |  }
      |}""".stripMargin

  test("a setting exposes its name, its group and the value of every property") {
    val config = InterpreterConfig.parse(json).fold(failure => fail(failure), identity)
    assertEquals(config.names, Set("md"))
    assertEquals(config.setting("md").flatMap(_.properties.get("markdown.parser.type")), Some("flexmark"))
    assertEquals(config.setting("md").map(_.interpreters), Some(Set("md")))
  }

  test("a configuration without interpreter settings is rejected") {
    assertEquals(InterpreterConfig.parse("{}"), Left("field 'interpreterSettings' is missing"))
  }

  test("broken JSON is reported rather than thrown") {
    assert(InterpreterConfig.parse("nonsense").isLeft)
  }
}
