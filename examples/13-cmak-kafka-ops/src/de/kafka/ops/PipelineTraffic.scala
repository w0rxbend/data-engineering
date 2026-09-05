package de.kafka.ops

import de.common.gen.DataGenerator
import de.common.json.Codecs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.*

/**
 * Puts some real traffic on the cluster, so that the operational views have something to show.
 *
 * A console full of empty topics teaches nothing: consumer lag is only interesting once a group has committed an offset
 * and then fallen behind. This object writes orders from the shared data generator and reads part of them back with a
 * consumer group that deliberately stops early.
 */
object PipelineTraffic {

  /**
   * Writes `count` orders from the shared online-shop generator to the given topic.
   *
   * The order identifier is used as the record key, which is what makes Kafka route every event about one order to the
   * same partition and therefore keep those events in order relative to each other.
   *
   * `acks=all` means a write is only acknowledged once every in-sync replica has it. Together with the
   * `min.insync.replicas=2` in the topic plan, that is the setting that makes a broker failure cost availability rather
   * than data.
   */
  def produceOrders(bootstrapServers: String, topic: String, count: Int, seed: Long = 42L): Int = {
    require(count >= 0, "record count cannot be negative")
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    val generator = new DataGenerator(seed)
    val producer  = new KafkaProducer[String, String](properties)
    try {
      val acknowledgements = (1 to count).map { _ =>
        val order = generator.nextOrder()
        producer.send(new ProducerRecord(topic, order.id.value, Codecs.order(order)))
      }
      // flush() only makes each Future complete; it does not surface an asynchronous
      // failure stored in that Future. Waiting on every result is what makes the
      // returned count mean "acknowledged by the cluster" rather than "queued locally".
      acknowledgements.foreach(_.get())
      count
    } finally producer.close()
  }

  /**
   * Reads at most `recordLimit` records with a consumer group and commits what it read, then leaves.
   *
   * Leaving early on purpose is the point: the group now has committed offsets somewhere in the middle of the topic, so
   * the lag report and the CMAK consumer view both show a real, non-zero number instead of a tidy zero.
   *
   * @return
   *   how many records were actually read.
   */
  def consumePartially(
      bootstrapServers: String,
      topic: String,
      group: String,
      recordLimit: Int,
      pollTimeout: Duration = Duration.ofSeconds(5)
  ): Int = {
    require(recordLimit > 0, "record limit must be positive")
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    // Offsets are committed explicitly below, so that "read" and "committed" cannot
    // drift apart while the example is explaining what lag means.
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, recordLimit.toString)

    val consumer = new KafkaConsumer[String, String](properties)
    try {
      consumer.subscribe(List(topic).asJava)
      var read        = 0
      var nextOffsets = Map.empty[TopicPartition, OffsetAndMetadata]
      // Two polls: the first usually returns nothing because it is spent joining the
      // group and getting a partition assignment.
      var attempts = 0
      while (read < recordLimit && attempts < 5) {
        val remaining = recordLimit - read
        val polled    = consumer.poll(pollTimeout).iterator().asScala.map { record =>
          PolledOffset(PartitionRef(record.topic(), record.partition()), record.offset())
        }
        val selection = PollSelection.upTo(polled, remaining)
        selection.nextOffsets.foreach { case (ref, offset) =>
          val partition = new TopicPartition(ref.topic, ref.partition)
          nextOffsets = nextOffsets.updated(partition, new OffsetAndMetadata(offset))
        }
        read += selection.recordsAccepted
        attempts += 1
      }
      // poll() is allowed to move the consumer position past records we did not
      // accept from its final batch. Commit explicit offsets for accepted records
      // only, otherwise "at most recordLimit" would be false at the group boundary.
      if (nextOffsets.nonEmpty) { consumer.commitSync(nextOffsets.asJava) }
      read
    } finally consumer.close()
  }
}
