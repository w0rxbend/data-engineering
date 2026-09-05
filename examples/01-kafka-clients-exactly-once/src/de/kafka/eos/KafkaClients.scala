package de.kafka.eos

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.util.Properties

/** How much of an in-flight transaction a consumer is allowed to see. */
enum IsolationLevel(val configValue: String) {

  /** Only records from committed transactions. This is what correctness needs. */
  case ReadCommitted extends IsolationLevel("read_committed")

  /** Everything on the log, including records a transaction later abandoned. */
  case ReadUncommitted extends IsolationLevel("read_uncommitted")
}

/**
 * Builds the two plain Apache Kafka Java clients this example uses.
 *
 * Every setting that matters for exactly-once processing is set explicitly and commented, because the defaults are what
 * most "it charged twice" incidents come down to.
 */
object KafkaClients {

  /**
   * A producer that can run transactions.
   *
   * `transactional.id` gives the producer a stable identity across restarts. Setting it implies
   * `enable.idempotence=true`, which makes the broker de-duplicate retries of the same record: without it, a network
   * hiccup between "broker wrote the record" and "producer saw the acknowledgement" would make the producer resend, and
   * the record would appear twice.
   */
  def transactionalProducer(
      bootstrapServers: BootstrapServers,
      transactionalId: TransactionalId
  ): KafkaProducer[String, String] = {
    val settings = new Properties()
    settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.value)
    settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId.value)
    settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    // "all" means the leader waits for every in-sync replica before
    // acknowledging. Idempotence requires it, and it is what makes a committed
    // record survive the loss of the broker that accepted it.
    settings.put(ProducerConfig.ACKS_CONFIG, "all")
    settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    new KafkaProducer[String, String](settings)
  }

  /** A plain, non-transactional producer for seeding the input topic. */
  def idempotentProducer(bootstrapServers: BootstrapServers): KafkaProducer[String, String] = {
    val settings = new Properties()
    settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.value)
    settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    settings.put(ProducerConfig.ACKS_CONFIG, "all")
    settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    new KafkaProducer[String, String](settings)
  }

  /**
   * A consumer for the input side of a transactional pipeline.
   *
   * Two settings do the work. `isolation.level=read_committed` hides records belonging to transactions that were
   * aborted or are still open. `enable.auto.commit=false` stops the client from committing offsets on a timer behind
   * your back: in this example offsets travel inside the transaction instead, which is the only way to keep them in
   * step with the output records.
   */
  def consumer(
      bootstrapServers: BootstrapServers,
      group: ConsumerGroupId,
      isolationLevel: IsolationLevel
  ): KafkaConsumer[String, String] = {
    val settings = new Properties()
    settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.value)
    settings.put(ConsumerConfig.GROUP_ID_CONFIG, group.value)
    settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel.configValue)
    settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    // A small batch keeps the printed output readable and makes the effect of
    // an aborted transaction easy to see.
    settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10")
    settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    new KafkaConsumer[String, String](settings)
  }
}
