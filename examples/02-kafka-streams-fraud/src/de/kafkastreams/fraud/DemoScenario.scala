package de.kafkastreams.fraud

import de.common.domain.*
import de.common.gen.DataGenerator

/**
 * Builds the sample traffic the demo runs on, without touching Kafka.
 *
 * Keeping the scenario pure has two benefits: the tests can feed exactly the same events into `TopologyTestDriver`, and
 * the producer that talks to a real broker stays a handful of lines of wiring.
 */
object DemoScenario {

  /** An order together with the payment attempt that belongs to it. */
  final case class Attempt(order: Order, payment: Payment)

  /**
   * Ordinary shop traffic: seeded random orders, each with the payment the shared `DataGenerator` produces for it
   * (roughly one in ten is declined, spread over many different customers).
   */
  def backgroundTraffic(count: Int, seed: Long = 42L, startEpochMillis: Long = 1700000000000L): List[Attempt] = {
    val generator = new DataGenerator(seed, startEpochMillis)
    List.fill(count) {
      val order = generator.nextOrder()
      Attempt(order, generator.paymentFor(order))
    }
  }

  /**
   * A card-testing burst: one customer, many small orders in quick succession, every single one of them declined. This
   * is the pattern the topology is meant to catch.
   *
   * @param spacingMillis
   *   gap between two consecutive attempts
   */
  def cardTestingBurst(
      customerId: CustomerId,
      attempts: Int,
      startEpochMillis: Long,
      spacingMillis: Long = 20000L
  ): List[Attempt] =
    (0 until attempts).toList.map { index =>
      val at    = startEpochMillis + index * spacingMillis
      val order = Order(
        // The order id has to be unique across bursts. Kafka Streams joins on
        // the record key, so two bursts reusing the same ids would let every
        // order of the second burst also pair with the buffered payments of the
        // first one, and the decline counts would balloon.
        id = OrderId(f"order-probe-${customerId.value}-$startEpochMillis-$index%03d"),
        customerId = customerId,
        lines = List(OrderLine(Sku("SKU-MUG"), 1, Money.eur(199L))),
        placedAtEpochMillis = at,
        country = "DE"
      )
      Attempt(order, Payment(order.id, order.total, PaymentStatus.Declined, at + 500L))
    }

  /** Risk tiers seeded into the compacted `customer-risk` topic. */
  def riskProfiles(suspect: CustomerId): List[CustomerRisk] =
    List(CustomerRisk(suspect, "watchlist"), CustomerRisk(CustomerId("cust-0001"), "trusted"))
}
