package de.ksqldb

import de.ksqldb.KsqlProtocol.{QueryLine, StatementOutcome}
import sttp.client4.*
import sttp.model.{MediaType, Uri}

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

/** Why a call to ksqlDB did not produce a usable answer. */
enum KsqlFailure {
  case Transport(cause: String)
  case HttpError(statusCode: Int, body: String)
  case StatementRejected(statement: String, message: String)

  def describe: String = this match {
    case Transport(cause)                     => s"could not reach ksqlDB: $cause"
    case HttpError(statusCode, body)          => s"ksqlDB answered HTTP $statusCode: $body"
    case StatementRejected(statement, reason) => s"ksqlDB rejected [$statement]: $reason"
  }
}

/**
 * A thin, blocking client for the ksqlDB REST API.
 *
 * "Direct style" means every call looks like an ordinary method call that returns when the server has answered - there
 * is no `Future`, no `IO`, and no callback to compose. sttp's synchronous backend gives exactly that, and the only
 * concession to streaming is [[streamQuery]], which hands each arriving line to a function instead of collecting them
 * into a list.
 *
 * This class owns no state beyond the backend it was given; all parsing is delegated to [[KsqlProtocol]].
 */
final class KsqlDbClient(baseUri: Uri, backend: SyncBackend) {

  /**
   * The media type of the ksqlDB API, sent both as `Content-Type` and as `Accept`.
   *
   * Asking for it explicitly matters: offered a free choice, a recent server answers `POST /query` in a newer, leaner
   * format that this client does not parse. Naming version 1 pins the shape of every response.
   */
  private val ksqlMediaType = MediaType.unsafeParse("application/vnd.ksql.v1+json")

  /**
   * Runs one statement through `POST /ksql` and waits for it to be applied.
   *
   * @return
   *   the server's report, or the first reason the call did not work out
   */
  def execute(statement: String, streamsProperties: Map[String, String]): Either[KsqlFailure, StatementOutcome] =
    executeBatch(List(statement), streamsProperties).flatMap { outcomes =>
      outcomes.headOption.toRight(KsqlFailure.StatementRejected(statement, "the server returned no result at all"))
    }

  /**
   * Runs several statements in a single request.
   *
   * ksqlDB applies them in the order given and answers with one report per statement. Sending a few hundred
   * `INSERT INTO ... VALUES` statements this way costs a handful of round trips instead of a few hundred.
   */
  def executeBatch(
      statements: List[String],
      streamsProperties: Map[String, String]
  ): Either[KsqlFailure, List[StatementOutcome]] = {
    val script  = statements.mkString("\n")
    val request = basicRequest
      .post(baseUri.addPath("ksql"))
      .contentType(ksqlMediaType)
      .header("Accept", ksqlMediaType.toString)
      .body(KsqlProtocol.requestBody(script, streamsProperties))
      .response(asStringAlways("utf-8"))

    send(request).flatMap { response =>
      if (!response.code.isSuccess) { Left(rejectionOrHttpError(response.code.code, response.body)) }
      else {
        val outcomes = KsqlProtocol.parseStatementOutcomes(response.body)
        outcomes.find(!_.succeeded) match {
          case Some(rejected) => Left(KsqlFailure.StatementRejected(rejected.statementText, rejected.message))
          case None           => Right(outcomes)
        }
      }
    }
  }

  /**
   * Runs a SELECT through `POST /query` and feeds every line to `onLine` as it arrives.
   *
   * The response body is read as a stream rather than as a string, so a push query - whose body only ends when the
   * query does - prints its first row immediately instead of after the connection closes. The same endpoint serves pull
   * queries, which simply end by themselves.
   */
  def streamQuery(
      sql: String,
      streamsProperties: Map[String, String]
  )(onLine: QueryLine => Unit): Either[KsqlFailure, Unit] = {
    val request = basicRequest
      .post(baseUri.addPath("query"))
      .contentType(ksqlMediaType)
      .header("Accept", ksqlMediaType.toString)
      .body(KsqlProtocol.requestBody(sql, streamsProperties))
      .response(asInputStreamAlways(body => consume(body, onLine)))

    send(request).flatMap { response =>
      if (response.code.isSuccess) { Right(()) }
      else { Left(rejectionOrHttpError(response.code.code, response.body)) }
    }
  }

  /**
   * A rejected statement arrives as an error status carrying a JSON explanation; anything else is reported as the bare
   * HTTP failure it is.
   */
  private def rejectionOrHttpError(statusCode: Int, body: String): KsqlFailure =
    KsqlProtocol
      .parseStatementError(body)
      .map(outcome => KsqlFailure.StatementRejected(outcome.statementText, outcome.message))
      .getOrElse(KsqlFailure.HttpError(statusCode, body))

  /**
   * Reads the response line by line, handing every recognised line to `onLine`.
   *
   * Lines that carry no result - the punctuation of the surrounding JSON array, or the explanation the server sends
   * instead of results when it rejects the query - are collected and returned, so that a failed call can report what
   * the server actually said.
   */
  private def consume(body: InputStream, onLine: QueryLine => Unit): String = {
    val reader     = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))
    val unexpected = new StringBuilder
    var line       = reader.readLine()
    while (line != null) {
      KsqlProtocol.parseQueryLine(line) match {
        case Some(parsed) => onLine(parsed)
        case None         => unexpected.append(line)
      }
      line = reader.readLine()
    }
    unexpected.toString
  }

  /**
   * Turns the exception an unreachable server throws into a value, so that callers handle a missing Docker stack the
   * same way they handle a rejected statement.
   */
  private def send[T](request: Request[T]): Either[KsqlFailure, Response[T]] =
    try Right(request.send(backend))
    catch { case error: Exception => Left(KsqlFailure.Transport(Option(error.getMessage).getOrElse(error.toString))) }
}

object KsqlDbClient {

  /** Kafka Streams settings applied to every statement and query in this example. */
  val readFromStart: Map[String, String] = Map("ksql.streams.auto.offset.reset" -> "earliest")
}
