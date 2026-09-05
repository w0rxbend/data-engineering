package de.kafka.eos

/**
 * Names and addresses this example talks to, wrapped in opaque types.
 *
 * An "opaque type" is a Scala 3 feature: at run time the value really is a plain `String`, but the compiler refuses to
 * let you pass a `TopicName` where a `TransactionalId` is expected. Kafka configuration is a long list of strings, and
 * mixing two of them up is a mistake that only shows up at run time, so it is worth spending a few lines to make it
 * impossible.
 *
 * Each `value` accessor lives inside the companion object of its own type. That placement is not decoration: it is what
 * lets the compiler find the accessor without an import, and it keeps four accessors that all erase to
 * `(String) => String` from colliding with one another.
 */

/** Address of one or more Kafka brokers, for example `localhost:10192`. */
opaque type BootstrapServers = String

object BootstrapServers {
  def apply(raw: String): BootstrapServers = raw

  extension (servers: BootstrapServers) def value: String = servers
}

/** The name of a Kafka topic: a named, partitioned, append-only log of records. */
opaque type TopicName = String

object TopicName {
  def apply(raw: String): TopicName = raw

  extension (topic: TopicName) def value: String = topic
}

/**
 * The identity a set of consumers uses to share work with, and resume from, each other. Kafka remembers the last
 * committed offset per consumer group.
 */
opaque type ConsumerGroupId = String

object ConsumerGroupId {
  def apply(raw: String): ConsumerGroupId = raw

  extension (group: ConsumerGroupId) def value: String = group
}

/**
 * The stable identity of a transactional producer.
 *
 * Kafka uses it to fence out a previous, possibly hung, instance of the same service: when a producer with
 * transactional id `X` calls `initTransactions`, every older producer that claimed `X` is refused any further writes.
 * That fencing is what turns "at least once" into "exactly once" after a crash.
 */
opaque type TransactionalId = String

object TransactionalId {
  def apply(raw: String): TransactionalId = raw

  extension (id: TransactionalId) def value: String = id
}

/** The concrete names and addresses this example's commands use. */
object Settings {

  /** Default broker address published by this example's docker compose stack. */
  val defaultBootstrapServers: BootstrapServers = BootstrapServers("localhost:10192")

  /** Topic the seeding producer writes and the settlement service reads. */
  val ordersTopic: TopicName = TopicName("orders")

  /** Topic the settlement service writes its payment records to. */
  val paymentsTopic: TopicName = TopicName("payments")

  val settlementGroup: ConsumerGroupId = ConsumerGroupId("payment-settlement")

  val settlementTransactionalId: TransactionalId = TransactionalId("payment-settlement-1")

  /** A separate identity, so the abort demonstration never fences the service. */
  val abortDemoTransactionalId: TransactionalId = TransactionalId("payment-settlement-abort-demo")
}
