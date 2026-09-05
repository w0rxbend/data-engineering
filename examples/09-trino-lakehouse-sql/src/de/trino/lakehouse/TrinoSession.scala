package de.trino.lakehouse

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

/** Everything needed to open one connection to a Trino coordinator. */
final case class TrinoEndpoint(host: String, port: Int, user: String, catalog: String, schema: String) {

  /**
   * The JDBC (Java Database Connectivity) address of the coordinator.
   *
   * `catalog` and `schema` in the address are only defaults: a query may always name a different catalog, and the
   * cross-catalog join in this example does exactly that.
   */
  def jdbcUrl: String = s"jdbc:trino://$host:$port/$catalog/$schema"
}

object TrinoEndpoint {

  /** The coordinator published by this example's Docker Compose stack. */
  val local: TrinoEndpoint =
    TrinoEndpoint(host = "localhost", port = 10980, user = "lakehouse", catalog = "delta", schema = "shop")
}

/**
 * The one operation the rest of this example needs from a database: run a statement and get a [[ResultTable]] back.
 *
 * Depending on this trait rather than on `java.sql.Connection` keeps the query logic and the printing free of JDBC, and
 * lets the tests substitute a fake session. This is the "port" of a ports-and-adapters design; [[JdbcTrinoSession]] is
 * the adapter that speaks to a real coordinator.
 */
trait TrinoSession {

  /** Runs one statement. Statements that return no rows (`CREATE`, `INSERT`, `SET SESSION`) yield an empty table. */
  def run(sql: String): ResultTable
}

/**
 * A [[TrinoSession]] backed by the official Trino JDBC driver.
 *
 * The class is deliberately not public API: it is created by [[TrinoSession.connected]], which owns the connection's
 * lifetime, so no caller can forget to close it.
 */
private final class JdbcTrinoSession(connection: Connection) extends TrinoSession {

  def run(sql: String): ResultTable = {
    val statement = connection.createStatement()
    try
      if (statement.execute(sql)) readAll(statement.getResultSet) else ResultTable.empty
    finally statement.close()
  }

  private def readAll(resultSet: ResultSet): ResultTable =
    try ResultTable.from(new JdbcCursor(resultSet))
    finally resultSet.close()
}

/** Reads a `java.sql.ResultSet` through the narrow [[ResultCursor]] interface the pure code expects. */
private final class JdbcCursor(resultSet: ResultSet) extends ResultCursor {

  private val metadata = resultSet.getMetaData

  // JDBC counts columns from one, the rest of this example counts from zero.
  val columnNames: List[String] = (1 to metadata.getColumnCount).map(metadata.getColumnLabel).toList

  def next(): Boolean = resultSet.next()

  def valueAt(columnIndex: Int): String = Option(resultSet.getObject(columnIndex + 1)).fold("NULL")(_.toString)
}

object TrinoSession {

  /**
   * Opens a connection, hands it to `body` as a [[TrinoSession]], and closes it again - even when `body` throws.
   *
   * This is the direct-style equivalent of a bracket or a `Resource`: the caller writes ordinary sequential code and
   * still cannot leak a connection.
   */
  def connected[A](endpoint: TrinoEndpoint)(body: TrinoSession => A): A = {
    val properties = new Properties()
    properties.setProperty("user", endpoint.user)
    val connection = DriverManager.getConnection(endpoint.jdbcUrl, properties)
    try body(new JdbcTrinoSession(connection))
    finally connection.close()
  }

  /**
   * Waits until the coordinator answers a trivial query.
   *
   * A freshly started Trino coordinator accepts connections before it has registered a worker, and a query submitted in
   * that window fails with `No nodes available`. Retrying a `SELECT 1` is the cheapest reliable readiness check.
   */
  def awaitReady(endpoint: TrinoEndpoint, attempts: Int, pauseMillis: Long): Unit = {
    def attempt(remaining: Int): Unit =
      try connected(endpoint)(_.run("SELECT 1"))
      catch {
        case failure: Exception if remaining > 1 =>
          println(s"Trino is not ready yet (${failure.getMessage}); retrying in ${pauseMillis}ms")
          Thread.sleep(pauseMillis)
          attempt(remaining - 1)
      }

    attempt(attempts)
  }
}
