package de.zeppelin.notebooks

import scala.util.Try

/**
 * One paragraph of an Apache Zeppelin note: a block of text plus the result of running it.
 *
 * Only the input side is modelled here. The stored results are irrelevant to this example, which ships notebooks that a
 * reader runs for themselves.
 */
final case class Paragraph(title: Option[String], text: String) {

  /** Which interpreter this paragraph asks for, if it says so explicitly. */
  def magic: Option[Magic] = Magic.parse(text)

  /** The input widgets declared inside the paragraph body. */
  def forms: List[DynamicForm] = DynamicForm.parseAll(text)
}

/**
 * A Zeppelin note: the document a reader opens in the browser.
 *
 * On disk a note is a single JSON file named `<note name>_<note id>.zpln`. Zeppelin scans its notebook folder at
 * startup, so a note file placed in that folder is present the moment the server finishes booting - which is what lets
 * this example ship ready-made analyses rather than pasted instructions.
 *
 * @param id
 *   the note identifier, which also appears in the file name and in the note's browser address
 * @param name
 *   the note's display name, with `/` acting as a folder separator in the note list
 * @param defaultInterpreterGroup
 *   the interpreter used by paragraphs that carry no magic of their own
 */
final case class Notebook(
    id: String,
    name: String,
    defaultInterpreterGroup: String,
    paragraphs: List[Paragraph]
) {

  /** Every interpreter setting this note needs, without duplicates, in the order first used. */
  def requiredInterpreterGroups: List[String] =
    (defaultInterpreterGroup :: paragraphs.flatMap(_.magic).map(_.group)).distinct
}

object Notebook {

  /**
   * Reads a `.zpln` file.
   *
   * The parse is deliberately narrow: it looks only at the handful of fields this example reasons about and ignores the
   * rest, so a Zeppelin release that adds fields does not break it.
   */
  def parse(json: String): Either[String, Notebook] =
    Try(ujson.read(json)).toEither.left.map(failure => s"not valid JSON: ${failure.getMessage}").flatMap { root =>
      for {
        id         <- requiredString(root, "id")
        name       <- requiredString(root, "name")
        group      <- requiredString(root, "defaultInterpreterGroup")
        paragraphs <- readParagraphs(root)
      } yield Notebook(id, name, group, paragraphs)
    }

  private def requiredString(node: ujson.Value, field: String): Either[String, String] =
    node.objOpt.flatMap(_.get(field)).flatMap(_.strOpt).filter(_.nonEmpty).toRight(s"field '$field' is missing")

  private def readParagraphs(root: ujson.Value): Either[String, List[Paragraph]] =
    root.objOpt.flatMap(_.get("paragraphs")).flatMap(_.arrOpt) match {
      case None        => Left("field 'paragraphs' is missing")
      case Some(nodes) =>
        val paragraphs = nodes.toList.map { node =>
          Paragraph(
            title = node.objOpt.flatMap(_.get("title")).flatMap(_.strOpt).filter(_.nonEmpty),
            text = node.objOpt.flatMap(_.get("text")).flatMap(_.strOpt).getOrElse("")
          )
        }
        if (paragraphs.isEmpty) Left("the note has no paragraphs") else Right(paragraphs)
    }
}
