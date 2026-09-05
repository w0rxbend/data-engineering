package de.trino.lakehouse

/**
 * A [[TrinoSession]] that never touches a network.
 *
 * It records the statements it was asked to run and answers each one from a lookup table, which is enough to test both
 * the runner's output and the seeding script without a cluster.
 */
final class FakeSession(answers: Map[String, ResultTable] = Map.empty) extends TrinoSession {

  private val executedStatements = List.newBuilder[String]

  def run(sql: String): ResultTable = {
    executedStatements += sql
    answers.getOrElse(sql, ResultTable.empty)
  }

  def executed: List[String] = executedStatements.result()
}

final class ScriptRunnerSuite extends munit.FunSuite {

  test("every statement of the script is executed, in file order") {
    val session = new FakeSession()
    ScriptRunner.run(session, SqlScript.parse("SELECT 1;\nSELECT 2;"))
    assertEquals(session.executed, List("SELECT 1", "SELECT 2"))
  }

  test("the report shows the comment, the statement and the rendered result") {
    val answer  = ResultTable(List("tier"), List(List("gold")))
    val session = new FakeSession(Map("SELECT tier FROM c" -> answer))
    val report  = ScriptRunner.run(session, SqlScript.parse("-- the tiers\nSELECT tier FROM c;"))
    assert(report.contains("-- the tiers"), report)
    assert(report.contains("SELECT tier FROM c"), report)
    assert(report.contains("gold"), report)
    assert(report.contains("1 row"), report)
  }

  test("a statement without a result set says so instead of printing an empty table") {
    val report = ScriptRunner.run(new FakeSession(), SqlScript.parse("SET SESSION x = 'y';"))
    assert(report.contains("(statement executed, no result set)"), report)
  }

  test("a query plan is printed verbatim rather than squeezed into a table") {
    val plan    = ResultTable(List("Query Plan"), List(List("Output\n  Aggregate\n    TableScan")))
    val session = new FakeSession(Map("EXPLAIN SELECT 1" -> plan))
    val report  = ScriptRunner.run(session, SqlScript.parse("EXPLAIN SELECT 1;"))
    assert(report.contains("    TableScan"), report)
  }
}
