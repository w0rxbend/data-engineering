package de.zeppelin.notebooks

final class MagicSuite extends munit.FunSuite {

  test("a bare magic names an interpreter setting and no interpreter inside it") {
    assertEquals(Magic.parse("%md\nhello"), Some(Magic("md", None)))
  }

  test("a dotted magic names the interpreter inside the setting") {
    assertEquals(Magic.parse("%spark.sql\nSELECT 1"), Some(Magic("spark", Some("sql"))))
  }

  test("a connection prefix in parentheses belongs to the setting, not to the name") {
    assertEquals(Magic.parse("%jdbc(warehouse)\nSELECT 1"), Some(Magic("jdbc", None)))
  }

  test("leading blank lines do not hide the magic") {
    assertEquals(Magic.parse("\n\n%spark\nval x = 1"), Some(Magic("spark", None)))
  }

  test("a paragraph without a magic runs on the note's default interpreter") {
    assertEquals(Magic.parse("SELECT 1"), None)
  }

  test("a magic renders back to the text a reader would type") {
    assertEquals(Magic("spark", Some("sql")).render, "%spark.sql")
    assertEquals(Magic("md", None).render, "%md")
  }

  test("empty magic components are rejected") {
    List("%spark.", "%spark..", "%.sql").foreach { text =>
      assertEquals(Magic.parse(text), None, text)
    }
  }
}
