package de.ksqldb

import de.ksqldb.KsqlProtocol.QueryLine

class KsqlProtocolSuite extends munit.FunSuite {

  test("a request body carries the statement and its streams properties") {
    val body = KsqlProtocol.requestBody("SHOW STREAMS;", Map("ksql.streams.auto.offset.reset" -> "earliest"))

    assertEquals(
      ujson.read(body),
      ujson.Obj(
        "ksql"              -> "SHOW STREAMS;",
        "streamsProperties" -> ujson.Obj("ksql.streams.auto.offset.reset" -> "earliest")
      )
    )
  }

  test("a statement report is read out of the response array") {
    val body =
      """[{"@type":"currentStatus","statementText":"CREATE STREAM orders_raw (id VARCHAR KEY);",
        |"commandStatus":{"status":"SUCCESS","message":"Stream created"}}]""".stripMargin

    val outcomes = KsqlProtocol.parseStatementOutcomes(body)

    assertEquals(outcomes.map(_.status), List("SUCCESS"))
    assertEquals(outcomes.map(_.message), List("Stream created"))
    assert(outcomes.forall(_.succeeded))
  }

  test("a statement without a command status counts as successful") {
    val outcomes = KsqlProtocol.parseStatementOutcomes("""[{"@type":"insert","statementText":"INSERT INTO a ..."}]""")

    assertEquals(outcomes.map(_.succeeded), List(true))
  }

  test("a rejected statement is reported with its message") {
    val body =
      """[{"statementText":"CREATE STREAM orders_raw (id VARCHAR KEY);",
        |"commandStatus":{"status":"ERROR","message":"Stream already exists"}}]""".stripMargin

    assertEquals(
      KsqlProtocol.parseStatementOutcomes(body).map(o => (o.succeeded, o.message)),
      List(false -> "Stream already exists")
    )
  }

  test("an error status carrying a statement error is read as a failed statement") {
    val body =
      """{"@type":"statement_error","error_code":40001,
        |"message":"Cannot add stream 'ORDERS_RAW': A stream with the same name already exists",
        |"statementText":"CREATE STREAM ORDERS_RAW (ID STRING KEY);","entities":[]}""".stripMargin

    val outcome = KsqlProtocol.parseStatementError(body)

    assertEquals(outcome.map(_.statementText), Some("CREATE STREAM ORDERS_RAW (ID STRING KEY);"))
    assert(outcome.exists(_.message.contains("already exists")))
    assert(outcome.forall(!_.succeeded))
  }

  test("a body that is not a statement error is left alone") {
    assertEquals(KsqlProtocol.parseStatementError("not json at all"), None)
    assertEquals(KsqlProtocol.parseStatementError("""[{"statementText":"SELECT 1;"}]"""), None)
  }

  test("the opening line of a query response yields the column names") {
    val line = """[{"header":{"queryId":"q-1","schema":"`COUNTRY` STRING, `REVENUECENTS` BIGINT"}}"""

    assertEquals(
      KsqlProtocol.parseQueryLine(line),
      Some(QueryLine.Header("q-1", List("COUNTRY", "REVENUECENTS")))
    )
  }

  test("a row line yields its columns rendered as display text") {
    val line = """,{"row":{"columns":["DE",4200,null,true]}}"""

    assertEquals(
      KsqlProtocol.parseQueryLine(line),
      Some(QueryLine.Row(List("DE", "4200", "null", "true")))
    )
  }

  test("the closing message of a limited push query is recognised") {
    assertEquals(
      KsqlProtocol.parseQueryLine(""",{"finalMessage":"Limit Reached"}]"""),
      Some(QueryLine.Finished("Limit Reached"))
    )
  }

  test("an error object is reduced to its message") {
    assertEquals(
      KsqlProtocol.parseQueryLine("""{"errorMessage":{"message":"Table not found","statementText":"SELECT 1;"}}"""),
      Some(QueryLine.Failed("Table not found"))
    )
  }

  test("a line holding only array punctuation is ignored") {
    assertEquals(KsqlProtocol.parseQueryLine("["), None)
    assertEquals(KsqlProtocol.parseQueryLine("]"), None)
    assertEquals(KsqlProtocol.parseQueryLine("   "), None)
  }

  test("commas inside a nested type do not split the schema into extra columns") {
    val schema = "`ID` STRING, `LINES` ARRAY<STRUCT<`SKU` STRING, `QUANTITY` INTEGER>>, `PLACEDAT` BIGINT"

    assertEquals(KsqlProtocol.columnNamesOf(schema), List("ID", "LINES", "PLACEDAT"))
  }

  test("a whole number is rendered without a decimal point") {
    assertEquals(KsqlProtocol.renderValue(ujson.Num(1700000000000.0)), "1700000000000")
    assertEquals(KsqlProtocol.renderValue(ujson.Num(12.5)), "12.5")
  }
}
