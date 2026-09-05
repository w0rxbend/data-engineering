package de.ksqldb

import de.common.domain.{Order, Payment}
import de.common.gen.DataGenerator
import de.ksqldb.KsqlProtocol.QueryLine
import sttp.client4.{DefaultSyncBackend, SyncBackend}
import sttp.model.Uri

import scala.io.Source
import scala.util.Using

/**
 * Walks through the whole example, from an empty ksqlDB server to a dashboard answer, printing what happens at each
 * step.
 *
 * The steps mirror the four files in `sql/`, plus the seeding step in between: declare the topics, declare the
 * dashboard, put orders and payments in, subscribe to changes, then ask a single question and get a single answer.
 */
object Main {

  /** How many orders to generate; each one also gets exactly one payment. */
  private val OrdersToSeed = 120

  /** Statements per request while seeding; a compromise between round trips and request size. */
  private val SeedBatchSize = 20

  def main(args: Array[String]): Unit = {
    val serverUri =
      Uri.unsafeParse(args.headOption.getOrElse(sys.env.getOrElse("KSQLDB_URL", "http://localhost:10388")))
    Using.resource(DefaultSyncBackend()) { backend =>
      run(new KsqlDbClient(serverUri, backend), serverUri) match {
        case Right(())     => println("\nDone.")
        case Left(failure) =>
          Console.err.println(s"\nThe example stopped: ${failure.describe}")
          sys.exit(1)
      }
    }
  }

  private def run(client: KsqlDbClient, serverUri: Uri): Either[KsqlFailure, Unit] = {
    println(s"Talking to ksqlDB at $serverUri")
    for {
      _ <- applyScript(client, "01-create-streams.sql", "declaring the raw streams")
      _ <- applyScript(client, "02-create-analytics.sql", "declaring the dashboard")
      _ <- seed(client)
      _ <- runPushQuery(client)
      _ <- runPullQuery(client)
    } yield ()
  }

  /**
   * Sends every statement of a script one at a time, so that a rejection can be reported against the statement that
   * caused it.
   *
   * Re-running the example must not fail, so a complaint that a stream or table is already there is treated as success:
   * the collection the script asks for exists, which is all the next step needs.
   */
  private def applyScript(client: KsqlDbClient, resourceName: String, what: String): Either[KsqlFailure, Unit] = {
    println(s"\n== $what ($resourceName)")
    KsqlStatements
      .statementsOf(readResource(resourceName))
      .foldLeft[Either[KsqlFailure, Unit]](Right(())) { (soFar, statement) =>
        soFar.flatMap(_ => applyStatement(client, statement))
      }
  }

  private def applyStatement(client: KsqlDbClient, statement: String): Either[KsqlFailure, Unit] =
    client.execute(statement, KsqlDbClient.readFromStart) match {
      case Right(outcome) =>
        println(s"   ${summarise(statement)} -> ${outcome.status}")
        Right(())
      case Left(failure) if describesAnExistingCollection(failure) =>
        println(s"   ${summarise(statement)} -> already there, keeping it")
        Right(())
      case Left(failure) => Left(failure)
    }

  private def describesAnExistingCollection(failure: KsqlFailure): Boolean = failure match {
    case KsqlFailure.StatementRejected(_, message) => message.toLowerCase.contains("already exists")
    case _                                         => false
  }

  /**
   * Fills the two topics with a reproducible batch of orders and payments.
   *
   * The events come from the shared [[DataGenerator]], which every example in this repository uses, so the numbers a
   * reader sees here line up with the numbers the Kafka and Flink examples produce. Roughly one payment in ten is
   * declined, which is what gives the second dashboard tile something to show.
   */
  private def seed(client: KsqlDbClient): Either[KsqlFailure, Unit] = {
    println(s"\n== inserting $OrdersToSeed orders and their payments")
    val generator = new DataGenerator()
    val orders    = generator.orders(OrdersToSeed)
    val inserts   = orders.flatMap(order => insertsFor(order, generator.paymentFor(order)))
    inserts
      .grouped(SeedBatchSize)
      .foldLeft[Either[KsqlFailure, Unit]](Right(())) { (soFar, batch) =>
        soFar.flatMap(_ => client.executeBatch(batch.toList, KsqlDbClient.readFromStart).map(_ => ()))
      }
      .map(_ => println(s"   ${inserts.size} rows inserted"))
  }

  private def insertsFor(order: Order, payment: Payment): List[String] =
    List(KsqlStatements.insertOrder(order), KsqlStatements.insertPayment(payment))

  /**
   * Subscribes to a table and prints rows as the server pushes them.
   *
   * Each line is printed the moment it is read off the chunked response, so watching the console shows the query
   * reacting rather than replaying a finished list.
   */
  private def runPushQuery(client: KsqlDbClient): Either[KsqlFailure, Unit] = {
    println("\n== push query: customers with declined payments, as the totals change (03-push-query.sql)")
    client.streamQuery(readSingleQuery("03-push-query.sql"), KsqlDbClient.readFromStart)(printLine)
  }

  /** Asks the windowed revenue table one question and prints the answer. */
  private def runPullQuery(client: KsqlDbClient): Either[KsqlFailure, Unit] = {
    println("\n== pull query: revenue per minute for Germany (04-pull-query.sql)")
    client.streamQuery(readSingleQuery("04-pull-query.sql"), KsqlDbClient.readFromStart)(printLine)
  }

  private def printLine(line: QueryLine): Unit = line match {
    case QueryLine.Header(_, columnNames) => println(s"   ${columnNames.mkString(" | ")}")
    case QueryLine.Row(values)            => println(s"   ${values.mkString(" | ")}")
    case QueryLine.Finished(message)      => println(s"   (server closed the query: $message)")
    case QueryLine.Failed(message)        => Console.err.println(s"   (query failed: $message)")
  }

  /** A script that holds exactly one query, such as the two SELECT files. */
  private def readSingleQuery(resourceName: String): String =
    KsqlStatements.statementsOf(readResource(resourceName)).mkString("\n")

  /** Shortens a statement to one readable line for the progress output. */
  private def summarise(statement: String): String = {
    val flattened = statement.replaceAll("\\s+", " ").trim
    if (flattened.length <= 70) { flattened }
    else { flattened.take(67) + "..." }
  }

  /**
   * Loads a `.sql` file from the classpath.
   *
   * The build registers `sql/` as this module's resource folder, so the files a reader opens in the repository are byte
   * for byte the ones sent to the server - there is no second, drifting copy embedded in Scala code.
   */
  private def readResource(name: String): String =
    Using.resource(Option(getClass.getResourceAsStream("/" + name)).getOrElse {
      throw new IllegalStateException(s"$name is missing from the classpath")
    })(stream => Source.fromInputStream(stream, "UTF-8").mkString)
}
