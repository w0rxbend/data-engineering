package de.zeppelin.notebooks

import java.sql.{Connection, DriverManager}
import java.util.Properties

/** Where the Trino coordinator listens and who the seeder claims to be. */
final case class TrinoEndpoint(host: String, port: Int, user: String) {

  /** The JDBC (Java Database Connectivity) address of the coordinator. */
  def jdbcUrl: String = s"jdbc:trino://$host:$port"
}

object TrinoEndpoint {

  /** The coordinator published by this example's Docker Compose stack, on this example's own port range. */
  val local: TrinoEndpoint = TrinoEndpoint(host = "localhost", port = 11680, user = "seeder")
}

/**
 * The single operation the seeder needs from a database: run one statement.
 *
 * Depending on this small trait rather than on `java.sql.Connection` keeps [[LakehouseSeed]] free of JDBC and lets the
 * tests hand the seeding loop a recording double. This is the "port" of a ports-and-adapters design.
 */
trait SqlSession {

  def run(sql: String): Unit
}

object TrinoSession {

  /**
   * Opens a connection, hands it to `body`, and closes it again even when `body` throws.
   *
   * This is the direct-style equivalent of a bracket: the caller writes ordinary sequential code and still cannot leak
   * a connection.
   */
  def connected[A](endpoint: TrinoEndpoint)(body: SqlSession => A): A = {
    val properties = new Properties()
    properties.setProperty("user", endpoint.user)
    val connection = DriverManager.getConnection(endpoint.jdbcUrl, properties)
    try body(sessionOn(connection))
    finally connection.close()
  }

  /**
   * Waits until the coordinator answers a trivial query.
   *
   * A freshly started Trino coordinator accepts network connections before it has registered a worker, and a query
   * submitted in that window fails with `No nodes available`. Retrying a `SELECT 1` is the cheapest reliable check.
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

  private def sessionOn(connection: Connection): SqlSession = (sql: String) => {
    val statement = connection.createStatement()
    try {
      statement.execute(sql)
      ()
    } finally statement.close()
  }
}
