package de.ksqldb

import scala.util.Try

/**
 * The wire format of the ksqlDB REST (Representational State Transfer) API.
 *
 * ksqlDB exposes two endpoints that matter here:
 *
 *   - `POST /ksql` runs a statement that changes the server (creating a stream, inserting a row) and answers with one
 *     JSON array once it is done.
 *   - `POST /query` runs a SELECT and answers with a *chunked* response: the connection stays open and the server
 *     writes one JSON object per line as results become available.
 *
 * Building those request bodies and interpreting the answers are pure string transformations, so they live here, apart
 * from the HTTP wiring in [[KsqlDbClient]], and are covered by unit tests that need no server.
 */
object KsqlProtocol {

  /**
   * Body for `POST /ksql` and `POST /query`.
   *
   * `streamsProperties` carries Kafka Streams settings for this one request. The one used throughout this example is
   * `ksql.streams.auto.offset.reset = earliest`, which means "start from the oldest record in the topic" instead of
   * "only show me what arrives from now on" - without it a query over already-inserted orders would print nothing.
   */
  def requestBody(sql: String, streamsProperties: Map[String, String]): String = {
    val properties = ujson.Obj.from(streamsProperties.map { case (key, value) => key -> ujson.Str(value) })
    ujson.write(ujson.Obj("ksql" -> ujson.Str(sql), "streamsProperties" -> properties))
  }

  /** What the server reported back about one statement sent to `POST /ksql`. */
  final case class StatementOutcome(statementText: String, status: String, message: String) {
    def succeeded: Boolean = status == "SUCCESS"
  }

  /**
   * Reads the JSON array returned by `POST /ksql`.
   *
   * A statement that changed the server carries a `commandStatus` object; `INSERT INTO ... VALUES` answers with an
   * entry that has none, which is reported here as a success with an empty message.
   */
  def parseStatementOutcomes(body: String): List[StatementOutcome] =
    ujson.read(body).arr.toList.map { entry =>
      val commandStatus = entry.obj.get("commandStatus").map(_.obj)
      StatementOutcome(
        statementText = entry.obj.get("statementText").map(_.str).getOrElse("").trim,
        status = commandStatus.flatMap(_.get("status")).map(_.str).getOrElse("SUCCESS"),
        message = commandStatus.flatMap(_.get("message")).map(_.str).getOrElse("")
      )
    }

  /**
   * Reads the JSON object ksqlDB returns with a 4xx answer.
   *
   * A rejected statement - asking for a stream that is already there, for instance - does not come back inside the
   * normal result array; the server answers with an error status and a single object naming the statement and what was
   * wrong with it.
   */
  def parseStatementError(body: String): Option[StatementOutcome] =
    Try(ujson.read(body).obj).toOption.flatMap { error =>
      error.get("message").map { message =>
        StatementOutcome(
          statementText = error.get("statementText").map(_.str).getOrElse("").trim,
          status = "ERROR",
          message = message.str
        )
      }
    }

  /** One line of the chunked response of `POST /query`. */
  enum QueryLine {

    /** The first line: the identifier of the query and its column names. */
    case Header(queryId: String, columnNames: List[String])

    /** One result row, with every column already rendered as display text. */
    case Row(values: List[String])

    /** The server closed the query on its own, for example because of a LIMIT. */
    case Finished(message: String)

    /** The server rejected the query or gave up on it. */
    case Failed(message: String)
  }

  /**
   * Interprets one line of the chunked `POST /query` response.
   *
   * The whole response is a single JSON array streamed piece by piece, so an individual line still carries the array's
   * punctuation: a leading `[` on the first line, a leading or trailing comma between elements, a `]` at the end. Those
   * characters are stripped before parsing. Lines that hold nothing but punctuation yield `None`.
   */
  def parseQueryLine(raw: String): Option[QueryLine] = {
    val payload = raw.trim.stripPrefix("[").stripPrefix(",").stripSuffix("]").stripSuffix(",").trim
    if (payload.isEmpty) { None }
    else {
      val json = ujson.read(payload).obj
      json.get("header").map(header => headerOf(header.obj)) orElse
        json.get("row").map(row => QueryLine.Row(row.obj("columns").arr.toList.map(renderValue))) orElse
        json.get("finalMessage").map(message => QueryLine.Finished(message.str)) orElse
        json.get("errorMessage").map(error => QueryLine.Failed(messageOf(error)))
    }
  }

  /** An error is reported either as bare text or as an object with a `message` field. */
  private def messageOf(error: ujson.Value): String =
    error.objOpt.flatMap(_.get("message")).map(renderValue).getOrElse(renderValue(error))

  private def headerOf(header: ujson.Obj): QueryLine.Header =
    QueryLine.Header(
      queryId = header.value.get("queryId").filterNot(_.isNull).map(_.str).getOrElse(""),
      columnNames = columnNamesOf(header.value.get("schema").map(_.str).getOrElse(""))
    )

  /**
   * Pulls the column names out of a ksqlDB schema string.
   *
   * A schema looks like ``` `COUNTRY` STRING, `REVENUECENTS` BIGINT ```. It
   * cannot be split on every comma, because a nested type such as
   * ``` `LINES` ARRAY<STRUCT<`SKU` STRING, `QUANTITY` INTEGER>> ``` contains
   * commas of its own, so only commas outside any angle brackets separate
   * columns.
   */
  def columnNamesOf(schema: String): List[String] =
    splitTopLevel(schema).map(nameOf).filter(_.nonEmpty)

  private def nameOf(column: String): String = {
    val trimmed = column.trim
    val closing = trimmed.indexOf('`', 1)
    if (trimmed.startsWith("`") && closing > 0) { trimmed.substring(1, closing) }
    else { trimmed.takeWhile(!_.isWhitespace) }
  }

  private def splitTopLevel(schema: String): List[String] = {
    val parts  = List.newBuilder[String]
    val part   = new StringBuilder
    var depth  = 0
    var quoted = false
    schema.foreach {
      case '`'                          => quoted = !quoted; part.append('`')
      case '<' if !quoted               => depth += 1; part.append('<')
      case '>' if !quoted               => depth -= 1; part.append('>')
      case ',' if !quoted && depth == 0 => parts += part.toString; part.clear()
      case other                        => part.append(other)
    }
    parts += part.toString
    parts.result().map(_.trim).filter(_.nonEmpty)
  }

  /**
   * Renders one JSON column value as the text a console reader wants to see: a string without its quotes, a whole
   * number without a trailing `.0`.
   */
  def renderValue(value: ujson.Value): String = value match {
    case ujson.Str(text)                     => text
    case ujson.Num(number) if number.isWhole => number.toLong.toString
    case ujson.Num(number)                   => number.toString
    case ujson.Null                          => "null"
    case other                               => ujson.write(other)
  }
}
