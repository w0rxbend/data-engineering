package de.kafkastreams.fraud

import java.time.Duration

import scala.jdk.CollectionConverters.*

import de.common.domain.{CustomerId, Order, Payment}
import org.apache.kafka.common.serialization.Serdes as KafkaSerdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.streams.KeyValue

/**
 * The Kafka Streams topology that turns raw orders and payments into fraud alerts.
 *
 * A *topology* is the description of the processing graph: which topics are read, what happens to each record, and
 * where the results go. It is built once, described declaratively, and then handed to a `KafkaStreams` instance (in
 * production) or to a `TopologyTestDriver` (in the tests) to be executed.
 */
object FraudTopology {

  /** Topic carrying one message per placed order, keyed by order id. */
  val OrdersTopic = "orders"

  /** Topic carrying the outcome of each card charge, also keyed by order id. */
  val PaymentsTopic = "payments"

  /** Compacted topic holding the current risk tier of a customer. */
  val CustomerRiskTopic = "customer-risk"

  /** Topic this example writes its findings to. */
  val AlertsTopic = "fraud-alerts"

  /**
   * Name of the windowed state store holding the per-customer decline tallies.
   *
   * Naming the store matters: an unnamed store gets a generated name, and the name is what an "interactive query" needs
   * in order to look inside the store of a running application (see `StoreQueries`).
   */
  val DeclineStore = "declines-per-customer"

  /**
   * The knobs a reader is likely to want to turn.
   *
   * @param joinWindow
   *   how far apart an order and its payment may be and still be joined. Payment providers answer within seconds, but a
   *   generous window absorbs retries and clock skew.
   * @param windowSize
   *   length of the window the declines are counted in
   * @param advanceBy
   *   how far each window starts after the previous one. A *hopping* window advances by less than its size, so windows
   *   overlap and a burst is caught no matter where it falls; setting `advanceBy` equal to `windowSize` turns it into a
   *   non-overlapping tumbling window.
   * @param grace
   *   how long after a window ends late records are still accepted into it
   * @param threshold
   *   number of declines inside one window that triggers an alert
   */
  final case class Config(
      joinWindow: Duration,
      windowSize: Duration,
      advanceBy: Duration,
      grace: Duration,
      threshold: Int
  )

  val defaultConfig: Config = Config(
    joinWindow = Duration.ofMinutes(1),
    windowSize = Duration.ofMinutes(2),
    advanceBy = Duration.ofMinutes(1),
    grace = Duration.ofSeconds(10),
    threshold = 3
  )

  def build(config: Config = defaultConfig): Topology = {
    val builder = new StreamsBuilder

    // A KStream is an unbounded, append-only log: every record is an
    // independent fact ("this order was placed", "that card was declined").
    val orders: KStream[String, Order] =
      builder.stream(OrdersTopic, Consumed.`with`(KafkaSerdes.String(), JsonSerdes.order))

    val payments: KStream[String, Payment] =
      builder.stream(PaymentsTopic, Consumed.`with`(KafkaSerdes.String(), JsonSerdes.payment))

    // A KTable is the opposite reading of the same kind of log: each record
    // *replaces* the previous one for its key, so the table always holds the
    // current risk tier of every customer and nothing else.
    val riskTable: KTable[String, CustomerRisk] =
      builder.table(CustomerRiskTopic, Consumed.`with`(KafkaSerdes.String(), JsonSerdes.customerRisk))

    // Stream-stream join: pair each order with the payment that carries the
    // same order id and whose timestamp lies within `joinWindow` of it. Both
    // sides are buffered in a state store for the length of the window, which
    // is why a join window always has to be bounded.
    val paidOrders: KStream[String, PaidOrder] =
      orders.join(
        payments,
        (order: Order, payment: Payment) =>
          PaidOrder(
            orderId = order.id,
            customerId = order.customerId,
            country = order.country,
            amount = payment.amount,
            status = payment.status,
            occurredAtEpochMillis = payment.occurredAtEpochMillis
          ),
        JoinWindows.ofTimeDifferenceAndGrace(config.joinWindow, config.grace),
        StreamJoined.`with`(KafkaSerdes.String(), JsonSerdes.order, JsonSerdes.payment)
      )

    // Card testing shows up as declines, so everything else is dropped here.
    // Re-keying by customer is what makes counting *per customer* possible:
    // Kafka Streams partitions by key, so all declines of one customer end up
    // in the same partition and therefore in the same state store.
    val declinesPerCustomer: KTable[Windowed[String], DeclineTally] =
      paidOrders
        .filter((_: String, paid: PaidOrder) => paid.isDeclined)
        .groupBy(
          (_: String, paid: PaidOrder) => paid.customerId.value,
          Grouped.`with`(KafkaSerdes.String(), JsonSerdes.paidOrder)
        )
        .windowedBy(
          TimeWindows
            .ofSizeAndGrace(config.windowSize, config.grace)
            .advanceBy(config.advanceBy)
        )
        .aggregate(
          () => DeclineTally.empty,
          (_: String, paid: PaidOrder, tally: DeclineTally) => tally.add(paid),
          Materialized
            .as[String, DeclineTally, WindowStore[Bytes, Array[Byte]]](DeclineStore)
            .withKeySerde(KafkaSerdes.String())
            .withValueSerde(JsonSerdes.declineTally)
        )

    // Without suppression a downstream consumer sees every intermediate count
    // (1, then 2, then 3, ...) and would alert three times on one burst.
    // `untilWindowCloses` holds the result back until the window plus its grace
    // period has passed, so exactly one final value per window is emitted.
    val alerts: KStream[String, FraudAlert] =
      declinesPerCustomer
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream
        .flatMap((key: Windowed[String], tally: DeclineTally) => alertRecords(key, tally, config.threshold))

    // Stream-table join: look up the customer's standing risk tier, keeping the
    // alert even when the table has no entry for that customer (leftJoin).
    alerts
      .leftJoin(
        riskTable,
        (alert: FraudAlert, risk: CustomerRisk) =>
          alert.withRiskTier(if (risk == null) {
            CustomerRisk.unknownTier
          } else { risk.tier }),
        Joined.`with`(KafkaSerdes.String(), JsonSerdes.fraudAlert, JsonSerdes.customerRisk)
      )
      .to(AlertsTopic, Produced.`with`(KafkaSerdes.String(), JsonSerdes.fraudAlert))

    builder.build()
  }

  /**
   * Bridges the pure rule in `FraudRules` to the Kafka Streams API: zero or one output record, keyed by customer id so
   * the downstream table join lines up.
   */
  private def alertRecords(
      key: Windowed[String],
      tally: DeclineTally,
      threshold: Int
  ): java.lang.Iterable[KeyValue[String, FraudAlert]] = {
    val alert = FraudRules.alertFor(
      customerId = CustomerId(key.key),
      tally = tally,
      windowStartEpochMillis = key.window.start,
      windowEndEpochMillis = key.window.end,
      threshold = threshold
    )
    alert.map(a => new KeyValue(key.key, a)).toList.asJava
  }
}
