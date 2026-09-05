package de.kafka.eos

import de.common.gen.DataGenerator
import de.common.json.Codecs

import org.apache.kafka.clients.producer.ProducerRecord
import ox.{useCloseableInScope, ResourceScope}

/**
 * Fills the `orders` topic with plausible online-shop orders so the settlement service has something to charge.
 *
 * The orders come from the repository-wide `DataGenerator`, which is seeded: running the seeder twice with the same
 * seed produces byte-identical orders, which makes it easy to compare what different examples do with them.
 */
object OrderSeeder {

  /**
   * @param count
   *   how many orders to publish
   * @param seed
   *   the generator seed; the same seed always yields the same orders
   * @return
   *   the ids of the published orders, in publication order
   */
  def seed(
      bootstrapServers: BootstrapServers,
      topic: TopicName,
      count: Int,
      seed: Long
  )(using ResourceScope): List[String] = {
    val producer  = useCloseableInScope(KafkaClients.idempotentProducer(bootstrapServers))
    val generator = new DataGenerator(seed)

    val publishedIds = generator.orders(count).map { order =>
      producer.send(new ProducerRecord(topic.value, order.id.value, Codecs.order(order)))
      order.id.value
    }

    // `flush` waits for every queued record to be acknowledged, so the ids
    // returned here really are on the topic by the time this method returns.
    producer.flush()
    publishedIds
  }
}
