package de.zeppelin.notebooks

/**
 * The interpreter selector at the top of an Apache Zeppelin paragraph, for example `%spark.sql`.
 *
 * Zeppelin calls this line a "magic". It is how one notebook can mix languages: the first line decides which
 * interpreter process receives the rest of the paragraph.
 *
 * A magic has two parts. `group` names an interpreter *setting* - an entry in `interpreter.json`, such as `spark`, `md`
 * or `jdbc`. `name` optionally selects one interpreter inside that setting, such as the `sql` interpreter of the
 * `spark` setting. `%spark` alone means "the default interpreter of the spark setting", which is the Scala one.
 *
 * @param group
 *   the interpreter setting, the part before the first dot
 * @param name
 *   the interpreter inside that setting, absent when the paragraph relies on the setting's default
 */
final case class Magic(group: String, name: Option[String]) {

  /** The magic as it is written in a paragraph, for example `%spark.sql`. */
  def render: String = "%" + group + name.fold("")("." + _)
}

object Magic {

  /**
   * The characters Zeppelin allows in a magic. Anything else - a space, a newline, an opening parenthesis - ends it.
   *
   * The parenthesis matters: `%jdbc(trino)` selects the `trino` connection prefix of the `jdbc` setting, and the
   * interpreter that has to exist is still plain `jdbc`.
   */
  private def isMagicChar(character: Char): Boolean =
    character.isLetterOrDigit || character == '.' || character == '_' || character == '-'

  /**
   * Reads the magic from a paragraph body.
   *
   * Returns `None` when the paragraph does not open with one, which is legal: such a paragraph runs on the note's
   * `defaultInterpreterGroup`.
   */
  def parse(paragraphText: String): Option[Magic] = {
    val trimmed = paragraphText.stripLeading()
    if (!trimmed.startsWith("%")) None
    else {
      val token = trimmed.drop(1).takeWhile(isMagicChar)
      token.split("\\.", -1).toList match {
        case group :: Nil if group.nonEmpty                          => Some(Magic(group, None))
        case group :: name :: Nil if group.nonEmpty && name.nonEmpty => Some(Magic(group, Some(name)))
        case _                                                       => None
      }
    }
  }
}
