package de.zeppelin.notebooks

/**
 * The notebooks and the interpreter configuration this example ships.
 *
 * Both live under `resources/`, which puts them on the classpath for the tests. The `zeppelin-init` container of
 * `docker/docker-compose.yml` copies the very same files into the Apache Zeppelin container for the reader.
 */
object NotebookLibrary {

  /** Folder on the classpath holding the `.zpln` note files; copied into Zeppelin's notebook directory. */
  val notebookFolder = "/notebooks"

  /**
   * The shipped note files, listed explicitly.
   *
   * Listing them rather than scanning a folder is intentional: a classpath folder cannot be listed reliably once the
   * code is packaged into a jar, and an explicit list makes a forgotten notebook fail the build rather than disappear.
   */
  val fileNames: List[String] = List(
    "01 Lakehouse Tour_2ZEPSHOP01.zpln",
    "02 Trino Federation_2ZEPSHOP02.zpln"
  )

  /** The interpreter configuration installed as `/opt/zeppelin/conf/interpreter.json`. */
  val interpreterConfigPath = "/interpreter/interpreter.json"

  def rawNotebook(fileName: String): String = Resources.text(s"$notebookFolder/$fileName")

  def notebook(fileName: String): Either[String, Notebook] = Notebook.parse(rawNotebook(fileName))

  def interpreterConfig: Either[String, InterpreterConfig] =
    InterpreterConfig.parse(Resources.text(interpreterConfigPath))
}
