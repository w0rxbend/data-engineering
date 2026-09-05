package de.presto.hive

import com.facebook.presto.jdbc.PrestoStatement

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.util.Using

/** The result of a query together with how much work PrestoDB did to answer it. */
final case class QueryOutcome[A](value: A, processedBytes: Long, processedRows: Long)

/**
 * A thin wrapper over the PrestoDB JDBC driver.
 *
 * It exists for one reason beyond convenience: the driver can report per-query statistics through a progress monitor,
 * and `processedBytes` from those statistics is the number that turns "partition pruning helps" into a measurement.
 * Plain JDBC has nowhere to expose that, so a small wrapper is needed.
 *
 * @param jdbcUrl
 *   for example `jdbc:presto://localhost:11080/hive/shop`.
 * @param user
 *   PrestoDB requires a user name but, in its default configuration, does not authenticate it.
 */
final class PrestoClient(jdbcUrl: String, user: String) extends AutoCloseable {

  private val connection: Connection = {
    val properties = new Properties()
    properties.setProperty("user", user)
    DriverManager.getConnection(jdbcUrl, properties)
  }

  /** Runs a statement that returns no rows, such as `CREATE TABLE`, `ANALYZE` or `CALL`. */
  def execute(sql: String): Unit =
    Using.resource(connection.createStatement())(statement => statement.execute(sql): Unit)

  /**
   * Runs a query and maps every row with `readRow`, also capturing the query statistics.
   *
   * The progress monitor is called repeatedly while the query runs, so the last value it reports is the total. It is
   * registered before execution because statistics that arrive after the result set is closed are lost.
   */
  def query[A](sql: String)(readRow: ResultSet => A): QueryOutcome[Seq[A]] =
    Using.resource(connection.createStatement()) { statement =>
      var bytes = 0L
      var rows  = 0L
      statement.unwrap(classOf[PrestoStatement]).setProgressMonitor { stats =>
        bytes = stats.getProcessedBytes
        rows = stats.getProcessedRows
      }
      val collected = Using.resource(statement.executeQuery(sql)) { resultSet =>
        Iterator.continually(resultSet).takeWhile(_.next()).map(readRow).toVector
      }
      QueryOutcome(collected, bytes, rows)
    }

  /** Runs a query whose result is a single row, failing loudly if the query returned anything else. */
  def queryOne[A](sql: String)(readRow: ResultSet => A): QueryOutcome[A] = {
    val outcome = query(sql)(readRow)
    outcome.value match {
      case Seq(only) => QueryOutcome(only, outcome.processedBytes, outcome.processedRows)
      case other     => throw new IllegalStateException(s"expected exactly one row, got ${other.size}:\n$sql")
    }
  }

  /** Returns the textual query plan of `sql`, as PrestoDB's `EXPLAIN` prints it. */
  def explain(sql: String, dialect: HiveSql): String =
    query(dialect.explain(sql))(_.getString(1)).value.mkString("\n")

  override def close(): Unit = connection.close()
}
