package de.common.json

/**
 * A very small JSON writer.
 *
 * JSON (JavaScript Object Notation) is the wire format used by most examples.
 * A full JSON library (circe, jsoniter, upickle, ...) cannot be used here
 * because this module is compiled for three different Scala versions and no
 * single library version covers all of them. Encoding is all this module
 * needs; every example that also has to *parse* JSON pulls in a real library.
 */
object Json {

  /** Escapes a string and wraps it in double quotes, as JSON requires. */
  def string(raw: String): String = {
    val out = new StringBuilder(raw.length + 2)
    out.append('"')
    var i = 0
    while (i < raw.length) {
      raw.charAt(i) match {
        case '"'                     => out.append("\\\"")
        case '\\'                    => out.append("\\\\")
        case '\n'                    => out.append("\\n")
        case '\r'                    => out.append("\\r")
        case '\t'                    => out.append("\\t")
        case c if c.toInt < 0x20     => out.append("\\u%04x".format(c.toInt))
        case c                       => out.append(c)
      }
      i += 1
    }
    out.append('"')
    out.toString
  }

  /** Builds `{"a":1,"b":"x"}` from already-rendered values, skipping `None`. */
  def obj(fields: (String, Option[String])*): String =
    fields.collect { case (key, Some(value)) => string(key) + ":" + value }
      .mkString("{", ",", "}")

  /** Builds `[1,2,3]` from already-rendered values. */
  def arr(values: Seq[String]): String = values.mkString("[", ",", "]")

  def num(value: Long): String = value.toString

  def num(value: Int): String = value.toString
}
