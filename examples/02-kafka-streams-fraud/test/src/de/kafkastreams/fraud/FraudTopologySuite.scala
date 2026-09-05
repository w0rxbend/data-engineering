package de.kafkastreams.fraud

import java.time.{Clock, Duration, Instant, ZoneOffset}
import java.util.Properties

import scala.jdk.CollectionConverters.*
import scala.util.Using

import de.common.domain.{CustomerId, Money, Order, OrderId, OrderLine, Payment, PaymentStatus, Sku}
import org.apache.kafka.common.serialization.{Deserializer, Serdes as KafkaSerdes, Serializer}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Runs the complete topology inside `TopologyTestDriver`.
 *
 * `TopologyTestDriver` executes the real processing graph in the current process: no broker, no network, no Docker, and
 * full control over time. Records are pushed in with an explicit timestamp, which is what makes window-based behaviour
 * testable at all — a test cannot wait ten minutes.
 */
class FraudTopologySuite extends munit.FunSuite {

  private val base    = Instant.parse("2024-05-01T10:00:00Z")
  private val suspect = CustomerId("cust-probe-01")

  test("one-shot source records stay ordered in one co-partitioned join task") {
    val scenario = new OneShotScenarioFactory(Clock.fixed(base, ZoneOffset.UTC), FraudTopology.defaultConfig)
      .create(suspect, attempts = 5)
    Using.resource(new MockProducer[String, String](true, null, new StringSerializer, new StringSerializer)) {
      producer =>
        SeedProducer.publishOneShotScenario(producer, scenario)
        val records = producer.history().asScala.toList
        assertEquals(records.size, 12)
        assertEquals(records.map(_.partition().intValue()).distinct, List(0))
        for (topic <- List(FraudTopology.OrdersTopic, FraudTopology.PaymentsTopic)) {
          val source = records.filter(_.topic() == topic)
          assertEquals(source.map(_.key()), scenario.records.map(_.sourceKey))
          assertEquals(source.map(_.timestamp().longValue()), scenario.records.map(_.recordTimestampEpochMillis))
        }
    }
  }

  /** Tumbling windows (advance equals size) keep the expectations obvious. */
  private val tumbling = FraudTopology.Config(
    joinWindow = Duration.ofMinutes(1),
    windowSize = Duration.ofMinutes(10),
    advanceBy = Duration.ofMinutes(10),
    grace = Duration.ZERO,
    threshold = 3
  )

  /** Hopping windows: the same size, but a new window starts every 5 minutes. */
  private val hopping = tumbling.copy(advanceBy = Duration.ofMinutes(5))

  // ------------------------------------------------------------- test rig

  private val stringSerializer: Serializer[String]     = KafkaSerdes.String().serializer()
  private val stringDeserializer: Deserializer[String] = KafkaSerdes.String().deserializer()

  private def driverProperties: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-topology-test")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, java.lang.Long.valueOf(0L))
    properties
  }

  /** A small facade so each test reads as "push events in, read alerts out". */
  private final class Rig(config: FraudTopology.Config) extends AutoCloseable {
    private val driver = new TopologyTestDriver(FraudTopology.build(config), driverProperties)

    private val ordersIn =
      driver.createInputTopic(FraudTopology.OrdersTopic, stringSerializer, JsonSerdes.order.serializer())
    private val paymentsIn =
      driver.createInputTopic(FraudTopology.PaymentsTopic, stringSerializer, JsonSerdes.payment.serializer())
    private val riskIn =
      driver.createInputTopic(FraudTopology.CustomerRiskTopic, stringSerializer, JsonSerdes.customerRisk.serializer())
    private val alertsOut =
      driver.createOutputTopic(FraudTopology.AlertsTopic, stringDeserializer, JsonSerdes.fraudAlert.deserializer())

    def risk(entry: CustomerRisk): Unit = riskIn.pipeInput(entry.customerId.value, entry)

    /** A payment with no order behind it, to prove the join is what gates counting. */
    def paymentOnly(orderId: String, cents: Long, status: PaymentStatus, at: Instant): Unit =
      paymentsIn.pipeInput(orderId, Payment(OrderId(orderId), Money.eur(cents), status, at.toEpochMilli), at)

    /** Pushes an order and its payment, both stamped with the same instant. */
    def attempt(customer: CustomerId, id: String, cents: Long, status: PaymentStatus, at: Instant): Unit = {
      val order = Order(
        id = OrderId(id),
        customerId = customer,
        lines = List(OrderLine(Sku("SKU-MUG"), 1, Money.eur(cents))),
        placedAtEpochMillis = at.toEpochMilli,
        country = "DE"
      )
      val payment = Payment(order.id, order.total, status, at.toEpochMilli)
      ordersIn.pipeInput(order.id.value, order, at)
      paymentsIn.pipeInput(order.id.value, payment, at)
    }

    /** Pushes one of the exact records built by the production one-shot scenario. */
    def attempt(record: TimestampedAttempt): Unit = {
      val at = Instant.ofEpochMilli(record.recordTimestampEpochMillis)
      ordersIn.pipeInput(record.sourceKey, record.attempt.order, at)
      paymentsIn.pipeInput(record.sourceKey, record.attempt.payment, at)
    }

    /**
     * Suppression only releases a window once stream time has moved past its end, and stream time only moves when
     * records arrive at the suppression node itself. The record therefore has to be a decline, so that it survives the
     * filter and reaches the aggregation; a single decline from an unrelated customer stays far below the threshold and
     * so cannot produce an alert of its own. TopologyTestDriver has one task; the production one-shot scenario uses the
     * suspect's customer key because a real deployment has three tasks.
     */
    def advanceStreamTime(to: Instant): Unit =
      attempt(CustomerId("cust-clock"), s"order-clock-${to.toEpochMilli}", 100L, PaymentStatus.Declined, to)

    def alerts: List[FraudAlert] = alertsOut.readValuesToList().asScala.toList

    override def close(): Unit = driver.close()
  }

  private def withRig(config: FraudTopology.Config)(body: Rig => Unit): Unit = {
    val rig = new Rig(config)
    try body(rig)
    finally rig.close()
  }

  // --------------------------------------------------------------- tests

  test("three declines inside one window raise exactly one alert") {
    withRig(tumbling) { rig =>
      (0 until 3).foreach { i =>
        rig.attempt(suspect, s"order-$i", 199L, PaymentStatus.Declined, base.plusSeconds(i * 30L))
      }
      rig.advanceStreamTime(base.plus(Duration.ofMinutes(30)))

      val alerts = rig.alerts
      assertEquals(alerts.size, 1)
      val alert = alerts.head
      assertEquals(alert.customerId, suspect)
      assertEquals(alert.declinedCount, 3)
      assertEquals(alert.totalDeclinedCents, 597L)
      assertEquals(alert.riskTier, CustomerRisk.unknownTier)
    }
  }

  test("two declines stay below the threshold and raise nothing") {
    withRig(tumbling) { rig =>
      (0 until 2).foreach { i =>
        rig.attempt(suspect, s"order-$i", 199L, PaymentStatus.Declined, base.plusSeconds(i * 30L))
      }
      rig.advanceStreamTime(base.plus(Duration.ofMinutes(30)))
      assertEquals(rig.alerts, Nil)
    }
  }

  test("captured payments are ignored no matter how many there are") {
    withRig(tumbling) { rig =>
      (0 until 10).foreach { i =>
        rig.attempt(suspect, s"order-$i", 199L, PaymentStatus.Captured, base.plusSeconds(i * 10L))
      }
      rig.advanceStreamTime(base.plus(Duration.ofMinutes(30)))
      assertEquals(rig.alerts, Nil)
    }
  }

  test("a payment without a matching order never reaches the counter") {
    withRig(tumbling) { rig =>
      // Declines are only counted once the payment has been joined to the
      // order that says which customer it belongs to. Five orphan declines
      // therefore produce nothing at all.
      (0 until 5).foreach { i =>
        rig.paymentOnly(s"order-orphan-$i", 199L, PaymentStatus.Declined, base.plusSeconds(i * 10L))
      }
      rig.advanceStreamTime(base.plus(Duration.ofMinutes(30)))
      assertEquals(rig.alerts, Nil)
    }
  }

  test("the risk table decorates the alert with the customer's standing tier") {
    withRig(tumbling) { rig =>
      rig.risk(CustomerRisk(suspect, "watchlist"))
      (0 until 4).foreach { i =>
        rig.attempt(suspect, s"order-$i", 250L, PaymentStatus.Declined, base.plusSeconds(i * 30L))
      }
      rig.advanceStreamTime(base.plus(Duration.ofMinutes(30)))

      val alert = rig.alerts.head
      assertEquals(alert.riskTier, "watchlist")
      assertEquals(alert.declinedCount, 4)
    }
  }

  test("hopping windows overlap, so one burst is reported by every window covering it") {
    withRig(hopping) { rig =>
      (0 until 3).foreach { i =>
        rig.attempt(suspect, s"order-$i", 199L, PaymentStatus.Declined, base.plusSeconds(i * 30L))
      }
      rig.advanceStreamTime(base.plus(Duration.ofMinutes(30)))

      val alerts = rig.alerts
      // A 10-minute window that advances every 5 minutes means each instant is
      // covered by two windows, and both of them contain the whole burst.
      assertEquals(alerts.size, 2)
      assert(alerts.forall(_.declinedCount == 3))
      assertEquals(alerts.map(_.windowStartEpochMillis).distinct.size, 2)
    }
  }

  test("declines spread across two windows never add up to an alert") {
    withRig(tumbling) { rig =>
      rig.attempt(suspect, "order-a", 199L, PaymentStatus.Declined, base)
      rig.attempt(suspect, "order-b", 199L, PaymentStatus.Declined, base.plusSeconds(60))
      rig.attempt(suspect, "order-c", 199L, PaymentStatus.Declined, base.plus(Duration.ofMinutes(12)))
      rig.attempt(suspect, "order-d", 199L, PaymentStatus.Declined, base.plus(Duration.ofMinutes(13)))
      rig.advanceStreamTime(base.plus(Duration.ofMinutes(60)))
      assertEquals(rig.alerts, Nil)
    }
  }

  test("the demo scenario really does trip the detector") {
    withRig(tumbling) { rig =>
      DemoScenario.cardTestingBurst(suspect, attempts = 5, startEpochMillis = base.toEpochMilli).foreach { a =>
        rig.attempt(
          a.order.customerId,
          a.order.id.value,
          a.order.total.cents,
          a.payment.status,
          Instant.ofEpochMilli(a.order.placedAtEpochMillis)
        )
      }
      rig.advanceStreamTime(base.plus(Duration.ofMinutes(30)))
      assertEquals(rig.alerts.map(_.declinedCount), List(5))
    }
  }

  test("the one-shot scenario derives unique source keys and exact timestamps from its clock") {
    val scenario = new OneShotScenarioFactory(Clock.fixed(base, ZoneOffset.UTC), FraudTopology.defaultConfig)
      .create(suspect, attempts = 5)
    val closeAt = base
      .plus(FraudTopology.defaultConfig.windowSize)
      .plus(FraudTopology.defaultConfig.grace)
      .plusMillis(1L)

    assertEquals(scenario.attack.map(_.recordTimestampEpochMillis), List.fill(5)(base.toEpochMilli))
    assertEquals(scenario.attack.map(_.attempt.order.placedAtEpochMillis), List.fill(5)(base.toEpochMilli))
    assertEquals(scenario.streamTimeAdvance.recordTimestampEpochMillis, closeAt.toEpochMilli)
    assertEquals(scenario.streamTimeAdvance.attempt.order.placedAtEpochMillis, closeAt.toEpochMilli)
    assertEquals(scenario.records.map(_.sourceKey).distinct.size, 6)
    assertEquals(scenario.records.map(_.customerKey).distinct, List(suspect.value))
  }

  test("a clean repeat starts one window after the preceding stream-time advance") {
    val first = new OneShotScenarioFactory(Clock.fixed(base, ZoneOffset.UTC), FraudTopology.defaultConfig)
      .create(suspect, attempts = 5)
    val previousStreamTime = first.streamTimeAdvance.recordTimestampEpochMillis

    val tooSoon =
      new OneShotScenarioFactory(Clock.fixed(base.plusSeconds(10L), ZoneOffset.UTC), FraudTopology.defaultConfig)
        .create(suspect, attempts = 5)
    val cleanRepeatAt = previousStreamTime + FraudTopology.defaultConfig.windowSize.toMillis + 1L
    val eligible      = new OneShotScenarioFactory(
      Clock.fixed(Instant.ofEpochMilli(cleanRepeatAt), ZoneOffset.UTC),
      FraudTopology.defaultConfig
    ).create(suspect, attempts = 5)

    assert(tooSoon.attack.head.recordTimestampEpochMillis < previousStreamTime)
    assert(eligible.attack.head.recordTimestampEpochMillis - previousStreamTime > 120000L)
  }

  test("the production one-shot scenario closes both five-decline hopping windows") {
    val scenario = new OneShotScenarioFactory(Clock.fixed(base, ZoneOffset.UTC), FraudTopology.defaultConfig)
      .create(suspect, attempts = 5)

    withRig(FraudTopology.defaultConfig) { rig =>
      rig.risk(CustomerRisk(suspect, "watchlist"))
      scenario.records.foreach(rig.attempt)

      val alerts = rig.alerts.sortBy(_.windowStartEpochMillis)
      assertEquals(alerts.size, 2)
      assert(alerts.forall(_.customerId == suspect))
      assert(alerts.forall(_.declinedCount == 5))
      assert(alerts.forall(_.totalDeclinedCents == 995L))
      assert(alerts.forall(_.riskTier == "watchlist"))
      assertEquals(alerts.map(_.windowStartEpochMillis).sliding(2).map(pair => pair(1) - pair(0)).toList, List(60000L))
      assert(alerts.forall(alert => alert.windowEndEpochMillis - alert.windowStartEpochMillis == 120000L))

      val repeatAt = scenario.streamTimeAdvance.recordTimestampEpochMillis +
        FraudTopology.defaultConfig.windowSize.toMillis + 1L
      val repeated = new OneShotScenarioFactory(
        Clock.fixed(Instant.ofEpochMilli(repeatAt), ZoneOffset.UTC),
        FraudTopology.defaultConfig
      ).create(suspect, attempts = 5)
      repeated.records.foreach(rig.attempt)

      val repeatedAlerts = rig.alerts
      assertEquals(repeatedAlerts.size, 2)
      assert(repeatedAlerts.forall(_.declinedCount == 5))
      assert(repeatedAlerts.forall(_.totalDeclinedCents == 995L))
    }
  }
}
