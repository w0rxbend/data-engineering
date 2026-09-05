package de.zeppelin.notebooks

/**
 * Entry point of example 16.
 *
 * It has two jobs, both of them supporting acts for the notebooks:
 *   - `seed` creates the Delta Lake tables in the object store, through Trino, from the shared data generator;
 *   - `check` reports whether every shipped notebook is well-formed and only uses configured interpreters.
 *
 * Running it without arguments does both, which is the shortest path from a started Docker Compose stack to a notebook
 * that returns rows.
 */
object Main {

  private val historyDays = 14

  private val ordersPerDay = 150

  private val readinessAttempts = 60

  private val readinessPauseMillis = 2000L

  def main(args: Array[String]): Unit =
    args.headOption.getOrElse("all") match {
      case "seed"  => seed()
      case "check" => check()
      case "all"   =>
        check()
        seed()
      case other => throw new IllegalArgumentException(s"unknown mode '$other'; expected seed, check or all")
    }

  /** Creates the lakehouse schema and tables and loads the generated orders into them. */
  private def seed(): Unit = {
    val endpoint = TrinoEndpoint.local
    println(s"Connecting to ${endpoint.jdbcUrl} as ${endpoint.user}")
    TrinoSession.awaitReady(endpoint, readinessAttempts, readinessPauseMillis)

    val orders     = LakehouseSeed.sampleOrders(historyDays, ordersPerDay)
    val statements = LakehouseSeed.script(orders)
    println(
      s"Seeding ${LakehouseSeed.schemaName} with ${orders.size} orders over $historyDays days " +
        s"(${statements.size} statements)"
    )
    TrinoSession.connected(endpoint)(session => statements.foreach(session.run))
    println(s"Seeded ${LakehouseSeed.ordersTable} and ${LakehouseSeed.orderLinesTable}")
  }

  /** Prints the same verdict the unit tests assert on, so it is available without running the test suite. */
  private def check(): Unit = {
    val config = NotebookLibrary.interpreterConfig.fold(
      failure => throw new IllegalStateException(s"the interpreter configuration is unusable: $failure"),
      identity
    )
    println(s"Configured interpreter settings: ${config.names.toList.sorted.mkString(", ")}")

    val checked = NotebookLibrary.fileNames.map { fileName =>
      val note = NotebookLibrary
        .notebook(fileName)
        .fold(failure => throw new IllegalStateException(s"notebook '$fileName' is unusable: $failure"), identity)
      val problems = NotebookCheck.problems(fileName, note, config)
      val verdict  = if (problems.isEmpty) "ok" else problems.mkString("\n  - ", "\n  - ", "")
      println(
        s"${note.name}: ${note.paragraphs.size} paragraphs, " +
          s"interpreters ${note.requiredInterpreterGroups.mkString(", ")} -> $verdict"
      )
      fileName -> problems
    }

    val failures = checked.flatMap { case (fileName, problems) => problems.map(problem => s"$fileName: $problem") }
    if (failures.nonEmpty) {
      throw new IllegalStateException(failures.mkString("notebook checks failed:\n  - ", "\n  - ", ""))
    }
  }
}
