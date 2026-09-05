package de.trino.lakehouse

import scala.io.Source
import scala.util.Using

/**
 * Finds the `.sql` files that ship with this example.
 *
 * They live in `resources/sql/` and therefore end up on the classpath of the compiled module, which means they are
 * found the same way whether the example runs from Mill or from an assembled jar. The order below is the order in which
 * the runner executes them, so it doubles as the table of contents of the demonstration.
 */
object SqlLibrary {

  val fileNames: List[String] = List(
    "01-explore-catalogs.sql",
    "02-cross-catalog-join.sql",
    "03-explain-the-join.sql",
    "04-session-properties.sql"
  )

  /** Reads one file from the classpath and splits it into statements. */
  def load(fileName: String): List[SqlStatement] = SqlScript.parse(read(fileName))

  private def read(fileName: String): String = {
    val path   = s"/sql/$fileName"
    val stream = Option(getClass.getResourceAsStream(path))
      .getOrElse(throw new IllegalArgumentException(s"no SQL file on the classpath at $path"))
    Using.resource(Source.fromInputStream(stream, "UTF-8"))(_.mkString)
  }
}
