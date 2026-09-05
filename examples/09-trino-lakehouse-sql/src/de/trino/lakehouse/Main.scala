package de.trino.lakehouse

/**
 * Entry point of example 09.
 *
 * The program does two things and nothing else:
 *   - `seed` writes the Delta Lake table in the object store, through Trino itself;
 *   - `query` executes the `.sql` files in `resources/sql/` and prints the results.
 *
 * Running it without arguments does both, which is the shortest path from a freshly started Docker Compose stack to a
 * federated query result.
 */
object Main {

  private val orderCount = 500

  private val readinessAttempts = 30

  private val readinessPauseMillis = 2000L

  def main(args: Array[String]): Unit = {
    val endpoint = TrinoEndpoint.local
    val mode     = args.headOption.getOrElse("all")

    println(s"Connecting to ${endpoint.jdbcUrl} as ${endpoint.user}")
    TrinoSession.awaitReady(endpoint, readinessAttempts, readinessPauseMillis)

    TrinoSession.connected(endpoint) { session =>
      mode match {
        case "seed"  => seed(session)
        case "query" => query(session)
        case "all"   => seed(session); query(session)
        case other   => throw new IllegalArgumentException(s"unknown mode '$other'; expected seed, query or all")
      }
    }
  }

  /** Creates the lakehouse schema and table and loads the generated orders into it. */
  private def seed(session: TrinoSession): Unit = {
    val orders     = DeltaSeed.sampleOrders(orderCount)
    val statements = DeltaSeed.script(orders)
    println(s"Seeding ${DeltaSeed.tableName} with ${orders.size} orders (${statements.size} statements)")
    statements.foreach(session.run)
    println(s"Seeded ${DeltaSeed.tableName}")
  }

  /** Runs every shipped `.sql` file and prints the report. */
  private def query(session: TrinoSession): Unit =
    SqlLibrary.fileNames.foreach { fileName =>
      println()
      println(s"### $fileName")
      println(ScriptRunner.run(session, SqlLibrary.load(fileName)))
    }
}
