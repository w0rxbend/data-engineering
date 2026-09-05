package de.kafka.eos

import ox.{useCloseableInScope, ExitCode, Ox, OxApp}

/** The three things this example can do, parsed from the command line. */
enum Command {

  /** Publish `count` generated orders to the `orders` topic. */
  case Seed(count: Int)

  /** Run the exactly-once settlement service until Ctrl+C. */
  case Settle

  /** Write one payment in a transaction, abort it, and show who can see it. */
  case ShowAbort
}

object Command {

  val usage: String =
    """usage: seed [count] | settle | abort-demo
      |
      |  seed [count]  publish generated orders to the `orders` topic (default 20)
      |  settle        consume orders and transactionally produce payments
      |  abort-demo    show that an aborted transaction is invisible to read_committed""".stripMargin

  def parse(args: Vector[String]): Either[String, Command] = args.toList match {
    case Nil                  => Left("no command given")
    case "settle" :: Nil      => Right(Command.Settle)
    case "abort-demo" :: Nil  => Right(Command.ShowAbort)
    case "seed" :: Nil        => Right(Command.Seed(defaultSeedCount))
    case "seed" :: raw :: Nil =>
      raw.toIntOption
        .filter(_ > 0)
        .toRight(s"'$raw' is not a positive whole number of orders")
        .map(Command.Seed.apply)
    case other => Left(s"unrecognised command: ${other.mkString(" ")}")
  }

  private val defaultSeedCount = 20
}

/**
 * Entry point: the thin wiring layer.
 *
 * `OxApp` gives the program a root structured-concurrency scope. When you press Ctrl+C, Ox interrupts the fork running
 * `run`, which unblocks the Kafka poll and releases the clients registered with `useCloseableInScope` in reverse order -
 * so the consumer leaves its group cleanly instead of making its peers wait for a session timeout.
 */
object Main extends OxApp {

  /** The generator seed; fixed so a re-run produces the same orders. */
  private val orderSeed = 42L

  override def run(args: Vector[String])(using Ox): ExitCode =
    Command.parse(args) match {
      case Left(problem) =>
        println(s"$problem\n\n${Command.usage}")
        ExitCode.Failure(2)
      case Right(command) =>
        execute(command, bootstrapServers)
        ExitCode.Success
    }

  /**
   * The broker address, overridable with the `KAFKA_BOOTSTRAP_SERVERS` environment variable so the example also runs
   * against a broker elsewhere.
   */
  private def bootstrapServers: BootstrapServers =
    sys.env.get("KAFKA_BOOTSTRAP_SERVERS").map(BootstrapServers.apply).getOrElse(Settings.defaultBootstrapServers)

  private def execute(command: Command, brokers: BootstrapServers)(using Ox): Unit = command match {
    case Command.Seed(count) => seed(count, brokers)
    case Command.Settle      => settle(brokers)
    case Command.ShowAbort   => showAbort(brokers)
  }

  private def seed(count: Int, brokers: BootstrapServers)(using Ox): Unit = {
    val published = OrderSeeder.seed(brokers, Settings.ordersTopic, count, orderSeed)
    println(s"published ${published.size} orders to ${Settings.ordersTopic.value} at ${brokers.value}")
    println(s"first order id: ${published.headOption.getOrElse("none")}")
  }

  /**
   * Wires the real Kafka clients into the transactional loop.
   *
   * `initTransactions` happens once, here, before the loop starts: it claims the transactional id and rolls back
   * anything a previous run of this service left half-finished.
   */
  private def settle(brokers: BootstrapServers)(using Ox): Unit = {
    val producer = useCloseableInScope(
      KafkaClients.transactionalProducer(brokers, Settings.settlementTransactionalId)
    )
    val consumer = useCloseableInScope(
      KafkaClients.consumer(brokers, Settings.settlementGroup, IsolationLevel.ReadCommitted)
    )
    producer.initTransactions()
    SettlementService.subscribe(consumer, Settings.ordersTopic)

    println(s"settling orders from ${Settings.ordersTopic.value} into ${Settings.paymentsTopic.value}; Ctrl+C to stop")
    SettlementService.runUntilCancelled(
      consumer = consumer,
      transaction = new KafkaSettlementTransaction(producer, consumer, Settings.paymentsTopic),
      now = () => System.currentTimeMillis(),
      report = outcome => SettlementService.summarise(outcome).foreach(println)
    )
    println("settlement service stopped")
  }

  private def showAbort(brokers: BootstrapServers): Unit = {
    val visibility = AbortDemo.run(brokers, Settings.paymentsTopic)
    println("one payment was written inside a transaction and then aborted")
    println(visibility.describe)
  }
}
