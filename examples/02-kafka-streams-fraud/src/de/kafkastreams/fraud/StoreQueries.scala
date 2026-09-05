package de.kafkastreams.fraud

import java.time.{Duration, Instant}

import scala.jdk.CollectionConverters.*

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyWindowStore}

/**
 * "Interactive queries": reading a running application's state store directly.
 *
 * Kafka Streams keeps its aggregation state in a local store (RocksDB on disk, backed up to a Kafka topic). That store
 * is not a black box — the running instance can be asked for its current contents, which is how a service can answer
 * "how many declines does customer X have right now?" without going through a database.
 */
object StoreQueries {

  /** One row of the decline store: which customer, which window, what tally. */
  final case class Row(customerId: String, windowStartEpochMillis: Long, tally: DeclineTally)

  /**
   * Reads every window that ends inside the last `lookBack` from the store.
   *
   * The call throws while the instance is still starting up or is rebalancing, which is normal rather than exceptional:
   * callers are expected to catch the failure and try again later, the way `Main` does with `Try`.
   */
  def recentDeclines(streams: KafkaStreams, lookBack: Duration, now: Instant): List[Row] = {
    val parameters = StoreQueryParameters.fromNameAndType(
      FraudTopology.DeclineStore,
      QueryableStoreTypes.windowStore[String, DeclineTally]()
    )
    val store: ReadOnlyWindowStore[String, DeclineTally] = streams.store(parameters)
    val iterator                                         = store.fetchAll(now.minus(lookBack), now)
    try
      iterator.asScala.map { entry =>
        Row(entry.key.key, entry.key.window.start, entry.value)
      }.toList
    finally
      iterator.close()
  }

  /**
   * Formats the worst `limit` rows for the console; pure, so it can be unit tested.
   *
   * A busy shop has thousands of customers with a single decline each, and none of them are interesting. Only the top
   * of the list is printed, because that is where a card-testing burst shows up.
   */
  def render(rows: List[Row], limit: Int = 10): String =
    if (rows.isEmpty) {
      "  (no declines in the queried window yet)"
    } else {
      rows
        .sortBy(row => (-row.tally.count, row.customerId, row.windowStartEpochMillis))
        .map { row =>
          val amount = f"${row.tally.totalCents / 100.0}%.2f EUR"
          f"  ${row.customerId}%-16s window from ${Instant.ofEpochMilli(row.windowStartEpochMillis)}" +
            f"  declines=${row.tally.count}%d  total=$amount"
        }
        .take(limit)
        .mkString("\n")
    }
}
