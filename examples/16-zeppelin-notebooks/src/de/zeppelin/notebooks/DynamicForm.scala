package de.zeppelin.notebooks

/**
 * An input widget Apache Zeppelin renders above a paragraph.
 *
 * Zeppelin calls these "dynamic forms". They are declared inside the paragraph text rather than in a separate place:
 * writing `${country=DE,DE|PL|UA}` in a query turns into a dropdown whose choices are `DE`, `PL` and `UA`, defaulting
 * to `DE`, and the chosen value is substituted into the text before the interpreter sees it. Readers of the notebook
 * therefore change the analysis without editing any code.
 *
 * @param name
 *   the form's identifier, also used as its label
 * @param defaultValue
 *   the value used until the reader picks another one
 * @param options
 *   the choices of a dropdown; empty for a free-text field
 */
final case class DynamicForm(name: String, defaultValue: Option[String], options: List[String]) {

  /** A dropdown offers a fixed list of choices; a text field does not. */
  def isDropdown: Boolean = options.nonEmpty
}

object DynamicForm {

  private val opening = "${"
  private val closing = "}"

  /** Finds every form declaration in a paragraph body, in the order they appear. */
  def parseAll(paragraphText: String): List[DynamicForm] =
    declarations(paragraphText, from = 0, found = Nil).reverse.flatMap(parseOne)

  /** Collects the text between every `${` and its matching `}`. Recursion keeps the scan free of mutable state. */
  private def declarations(text: String, from: Int, found: List[String]): List[String] = {
    val start = text.indexOf(opening, from)
    if (start < 0) found
    else {
      val end = text.indexOf(closing, start)
      if (end < 0) found
      else declarations(text, end + 1, text.substring(start + opening.length, end) :: found)
    }
  }

  /**
   * Reads one declaration body such as `country=DE,DE|PL|UA`.
   *
   * The grammar is: a name, then an optional `=default`, then an optional `,choice|choice|choice`. A declaration whose
   * name is empty is not a form and is dropped.
   */
  private def parseOne(declaration: String): Option[DynamicForm] = {
    val (namePart, rest) = declaration.span(_ != '=')
    val name             = namePart.trim
    if (name.isEmpty) None
    else {
      val (defaultPart, optionsPart) = rest.drop(1).span(_ != ',')
      val defaultValue               = Option(defaultPart.trim).filter(_.nonEmpty)
      val options                    = optionsPart.drop(1).split('|').map(_.trim).filter(_.nonEmpty).toList
      Some(DynamicForm(name, defaultValue, options))
    }
  }
}
