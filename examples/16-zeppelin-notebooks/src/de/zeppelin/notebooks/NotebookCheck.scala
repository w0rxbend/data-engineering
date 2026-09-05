package de.zeppelin.notebooks

/**
 * Checks a shipped notebook against the interpreter configuration it will run on.
 *
 * The problem this solves is real. A notebook is data, not code: nothing compiles it, and a paragraph that begins
 * `%trino` instead of `%jdbc` fails only when a reader clicks run, minutes after the containers have started. These
 * checks turn that late failure into a unit test that needs no Docker at all.
 */
object NotebookCheck {

  /** Suffix of every Zeppelin note file. */
  val fileExtension = ".zpln"

  /**
   * Every problem found, as sentences a reader can act on. An empty list means the notebook is sound.
   *
   * @param fileName
   *   the note's file name, which Zeppelin also treats as data
   * @param notebook
   *   the parsed note
   * @param config
   *   the interpreter settings the Docker Compose stack installs
   */
  def problems(fileName: String, notebook: Notebook, config: InterpreterConfig): List[String] =
    fileNameProblems(fileName, notebook) ++ interpreterProblems(notebook, config)

  /**
   * Zeppelin derives a note's identity from its file name: `<display name>_<note id>.zpln`. A file whose name disagrees
   * with the JSON inside it still loads, but the note list and the JSON then tell different stories.
   */
  def fileNameProblems(fileName: String, notebook: Notebook): List[String] = {
    val expected = s"${notebook.name}_${notebook.id}$fileExtension"
    if (fileName == expected) Nil
    else List(s"file name '$fileName' does not match the note inside it; expected '$expected'")
  }

  /** Reports every magic that names an interpreter setting the compose stack does not configure. */
  def interpreterProblems(notebook: Notebook, config: InterpreterConfig): List[String] =
    notebook.requiredInterpreterGroups
      .filterNot(config.names.contains)
      .map(group =>
        s"interpreter '$group' is used but not configured; " +
          s"configured settings are ${config.names.toList.sorted.mkString(", ")}"
      )
}
