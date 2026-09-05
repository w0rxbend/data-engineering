package de.trino.lakehouse

/**
 * Runs the statements of a parsed `.sql` file against a [[TrinoSession]] and turns the whole run into printable text.
 *
 * The runner takes the session as a parameter instead of opening one, so a unit test can pass a fake that returns
 * canned results. That is what makes the report format testable without a Trino cluster.
 */
object ScriptRunner {

  private val separator = "=" * 100

  /** A single column whose rows contain line breaks is a query plan; those are printed verbatim, not as a table. */
  private def isQueryPlan(table: ResultTable): Boolean =
    table.columns.size == 1 && table.rows.exists(_.exists(_.contains("\n")))

  def run(session: TrinoSession, script: List[SqlStatement]): String =
    script.map(statement => report(session, statement)).mkString(System.lineSeparator + System.lineSeparator)

  private def report(session: TrinoSession, statement: SqlStatement): String = {
    val heading = (separator :: statement.description.map("-- " + _)) :+ statement.sql
    (heading :+ present(session.run(statement.sql))).mkString(System.lineSeparator)
  }

  private def present(table: ResultTable): String =
    if (table.columns.isEmpty) "(statement executed, no result set)"
    else if (isQueryPlan(table)) table.rows.flatten.mkString(System.lineSeparator)
    else TableRenderer.render(table) + System.lineSeparator + TableRenderer.summary(table)
}
