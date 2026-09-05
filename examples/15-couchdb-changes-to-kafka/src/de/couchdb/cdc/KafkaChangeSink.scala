package de.couchdb.cdc

import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.StringSerializer
import ox.{useCloseableInScope, Ox}

import java.util.Properties
import java.util.concurrent.ExecutionException
import scala.jdk.CollectionConverters.*

/**
 * Publishes mapped changes to Apache Kafka.
 *
 * Two settings do the work here. `enable.idempotence=true` makes the producer de-duplicate its own retries, so a
 * network hiccup cannot turn one send into two records within this producer session. `acks=all` makes it wait until
 * every in-sync replica has the record. Waiting for the send before moving the checkpoint gives at-least-once delivery:
 * a crash can replay a record in a new producer session. Stable keys make a compacted latest-value view converge, but
 * raw streaming consumers can observe both copies and may de-duplicate by the sequence in the payload.
 */
final class KafkaChangeSink private (producer: KafkaProducer[String, String], topic: TopicName) extends ChangeSink {

  /**
   * Sends one record and blocks until the broker has acknowledged it.
   *
   * A `null` value is not a mistake: that is exactly how a tombstone is expressed on the wire.
   */
  def publish(record: CatalogueRecord): Unit = {
    val message = new ProducerRecord[String, String](topic.value, record.key, record.value.orNull)
    producer.send(message).get()
  }
}

object KafkaChangeSink {

  /** Opens a producer that is closed when the enclosing Ox scope ends. */
  def open(settings: Settings)(using Ox): KafkaChangeSink = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers.value)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    new KafkaChangeSink(useCloseableInScope(new KafkaProducer[String, String](properties)), settings.topic)
  }

  /**
   * Creates the destination topic with log compaction enabled, if it is not there yet.
   *
   * A compacted topic retains a latest record for each live key, which turns the topic into a replayable copy of the
   * catalogue rather than a window of recent changes. Compaction is also what gives tombstones their meaning: after a
   * tombstone's retention window, both it and the deleted key may disappear from the compacted log.
   */
  def createTopicIfAbsent(settings: Settings, partitions: Int, replication: Short)(using Ox): Unit = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers.value)
    val admin = useCloseableInScope(Admin.create(properties))
    val topic = new NewTopic(settings.topic.value, partitions, replication)
      .configs(Map(TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT).asJava)
    try admin.createTopics(List(topic).asJava).all().get()
    catch {
      case failure: ExecutionException if failure.getCause.isInstanceOf[TopicExistsException] => ()
    }
  }
}
