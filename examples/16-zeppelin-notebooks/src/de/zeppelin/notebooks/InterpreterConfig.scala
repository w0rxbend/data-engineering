package de.zeppelin.notebooks

import scala.util.Try

/**
 * One entry of Apache Zeppelin's `interpreter.json`.
 *
 * Zeppelin calls it an *interpreter setting*: a named, configured instance of an interpreter group. The distinction
 * matters. `jdbc` is the group - the code that talks to any database over Java Database Connectivity. A setting called
 * `jdbc` with a Trino address in its properties is one usable instance of that group, and a second setting could point
 * the same code at PostgreSQL. The name of the setting is what a paragraph writes after its `%`.
 *
 * @param name
 *   the setting's name, and therefore the magic that selects it
 * @param group
 *   the interpreter group the setting instantiates
 * @param properties
 *   the configured values, for example `SPARK_HOME` or `default.url`
 */
final case class InterpreterSetting(name: String, group: String, properties: Map[String, String])

/** The interpreter configuration the Docker Compose stack installs in the Zeppelin container. */
final case class InterpreterConfig(settings: List[InterpreterSetting]) {

  def names: Set[String] = settings.map(_.name).toSet

  def setting(name: String): Option[InterpreterSetting] = settings.find(_.name == name)
}

object InterpreterConfig {

  /**
   * Reads `interpreter.json`.
   *
   * Zeppelin stores every property as an object (`{"name": ..., "value": ..., "type": ...}`) rather than as a bare
   * value, because the browser needs the type to render the right editor. Only the value is of interest here.
   */
  def parse(json: String): Either[String, InterpreterConfig] =
    Try(ujson.read(json)).toEither.left.map(failure => s"not valid JSON: ${failure.getMessage}").flatMap { root =>
      root.objOpt.flatMap(_.get("interpreterSettings")).flatMap(_.objOpt) match {
        case None           => Left("field 'interpreterSettings' is missing")
        case Some(settings) =>
          val parsed = settings.toList.map { case (key, node) => readSetting(key, node) }
          if (parsed.isEmpty) Left("no interpreter settings are configured") else Right(InterpreterConfig(parsed))
      }
    }

  private def readSetting(key: String, node: ujson.Value): InterpreterSetting = {
    val fields = node.objOpt.getOrElse(scala.collection.mutable.LinkedHashMap.empty[String, ujson.Value])
    InterpreterSetting(
      name = fields.get("name").flatMap(_.strOpt).getOrElse(key),
      group = fields.get("group").flatMap(_.strOpt).getOrElse(key),
      properties = readProperties(fields.get("properties"))
    )
  }

  private def readProperties(node: Option[ujson.Value]): Map[String, String] =
    node.flatMap(_.objOpt).fold(Map.empty[String, String]) { properties =>
      properties.toList.flatMap { case (key, value) => readPropertyValue(value).map(key -> _) }.toMap
    }

  private def readPropertyValue(node: ujson.Value): Option[String] =
    node.objOpt.flatMap(_.get("value")).map(stringify).orElse(Some(stringify(node)))

  /** Zeppelin stores booleans and numbers unquoted; every property is compared as text here. */
  private def stringify(node: ujson.Value): String = node.strOpt.getOrElse(node.render())
}
