package de.zeppelin.notebooks

final class DynamicFormSuite extends munit.FunSuite {

  test("a declaration with choices becomes a dropdown") {
    val form = DynamicForm.parseAll("WHERE country = '${country=DE,DE|PL|UA}'").head
    assertEquals(form.name, "country")
    assertEquals(form.defaultValue, Some("DE"))
    assertEquals(form.options, List("DE", "PL", "UA"))
    assert(form.isDropdown)
  }

  test("a declaration without choices becomes a free-text field") {
    val form = DynamicForm.parseAll("LIMIT ${rows=10}").head
    assertEquals(form.options, Nil)
    assert(!form.isDropdown)
  }

  test("a declaration without a default is still a form") {
    assertEquals(DynamicForm.parseAll("${sku}"), List(DynamicForm("sku", None, Nil)))
  }

  test("several declarations are reported in the order they appear") {
    assertEquals(DynamicForm.parseAll("${a=1} then ${b=2}").map(_.name), List("a", "b"))
  }

  test("text with no declarations has no forms") {
    assertEquals(DynamicForm.parseAll("SELECT 1"), Nil)
  }
}
