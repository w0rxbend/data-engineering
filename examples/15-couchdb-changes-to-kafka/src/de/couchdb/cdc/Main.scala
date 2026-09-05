package de.couchdb.cdc

import de.common.domain.Sku
import ox.{ExitCode, Ox, OxApp}

/** The things this example can be asked to do. */
enum Command {

  /** Create the database, install the design document and write the starting catalogue. */
  case Seed

  /** Follow the `_changes` feed and publish each applicable change row to Apache Kafka, until Ctrl+C. */
  case Follow

  /** Change one product's availability, producing one new revision. */
  case SetAvailability(sku: Sku, availability: Availability)

  /** Delete one product, producing a CouchDB tombstone. */
  case Remove(sku: Sku)

  /** Read the catalogue back with a Mango query and with the design document's view. */
  case Report(category: String)
}

object Command {

  val usage: String =
    """usage: seed | follow | in-stock <sku> | out-of-stock <sku> | remove <sku> | report [category]
      |
      |  seed                 create the database, the design document and the starting catalogue
      |  follow               publish applicable change rows to Kafka until Ctrl+C
      |  in-stock <sku>       mark a product as available again
      |  out-of-stock <sku>   mark a product as unavailable
      |  remove <sku>         delete a product, which becomes a Kafka tombstone
      |  report [category]    read the catalogue back (default category: hardware)""".stripMargin

  def parse(args: Vector[String]): Either[String, Command] = args.toList match {
    case Nil                          => Left("no command given")
    case "seed" :: Nil                => Right(Command.Seed)
    case "follow" :: Nil              => Right(Command.Follow)
    case "in-stock" :: sku :: Nil     => Right(Command.SetAvailability(Sku(sku), Availability.InStock))
    case "out-of-stock" :: sku :: Nil => Right(Command.SetAvailability(Sku(sku), Availability.OutOfStock))
    case "remove" :: sku :: Nil       => Right(Command.Remove(sku = Sku(sku)))
    case "report" :: Nil              => Right(Command.Report(defaultCategory))
    case "report" :: category :: Nil  => Right(Command.Report(category))
    case other                        => Left(s"unrecognised command: ${other.mkString(" ")}")
  }

  private val defaultCategory = "hardware"
}

/**
 * Entry point: the thin wiring layer.
 *
 * `OxApp` gives the program a root structured-concurrency scope and turns Ctrl+C into an interruption of the fork
 * running `run`, rather than an abrupt exit. That is what lets the connector write a last checkpoint and close its HTTP
 * backend and Kafka producer on the way out.
 */
object Main extends OxApp {

  override def run(args: Vector[String])(using Ox): ExitCode =
    Command.parse(args) match {
      case Left(problem) =>
        println(s"$problem\n\n${Command.usage}")
        ExitCode.Failure(2)
      case Right(command) =>
        execute(command, Settings.fromEnvironment(sys.env))
        ExitCode.Success
    }

  private def execute(command: Command, settings: Settings)(using Ox): Unit = command match {
    case Command.Follow                             => reportFinalProgress(ConnectorService.run(settings, ConsoleLog))
    case Command.Seed                               => withClient(settings)(CatalogueAdmin.seed(_, ConsoleLog))
    case Command.SetAvailability(sku, availability) =>
      withClient(settings)(CatalogueAdmin.setAvailability(_, sku, availability, ConsoleLog))
    case Command.Remove(sku)      => withClient(settings)(CatalogueAdmin.remove(_, sku, ConsoleLog))
    case Command.Report(category) => withClient(settings)(CatalogueAdmin.report(_, category, ConsoleLog))
  }

  private def withClient(settings: Settings)(use: CouchDbClient => Unit)(using Ox): Unit =
    use(CouchDbClient.open(settings))

  private def reportFinalProgress(progress: Progress): Unit =
    println(s"stopped with ${progress.summary}")
}
