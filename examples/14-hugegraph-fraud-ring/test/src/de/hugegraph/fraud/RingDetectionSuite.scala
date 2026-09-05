package de.hugegraph.fraud

import de.common.domain.{CustomerId, Money, Order, OrderId, OrderLine, Sku}
import de.hugegraph.fraud.Accounts.RingPlan
import de.hugegraph.fraud.PropertyGraph.Graph

/**
 * Tests for the analysis, run entirely in memory.
 *
 * Every fixture is built from explicitly written orders and ring plans, so each expectation can be checked by reading
 * the fixture rather than by trusting the generator.
 */
class RingDetectionSuite extends munit.FunSuite {

  /** One order with a single one-euro line, which is all these tests need. */
  private def order(id: String, customer: String, country: String = "DE"): Order =
    Order(
      id = OrderId(id),
      customerId = CustomerId(customer),
      lines = List(OrderLine(Sku("SKU-COFFEE"), 1, Money.eur(100))),
      placedAtEpochMillis = 1700000000000L,
      country = country
    )

  private def graphOf(orders: List[Order], plans: List[RingPlan]): Graph =
    ShopGraph.build(orders, Accounts.assign(orders.map(_.customerId), plans))

  test("customers with private artefacts form no ring") {
    val orders = List(order("o-1", "cust-1"), order("o-2", "cust-2"))
    assertEquals(RingDetection.rings(graphOf(orders, Nil)), Nil)
  }

  test("two customers paying with one card are one ring, with the card as evidence") {
    val plan   = RingPlan("card", List("cust-1", "cust-2"), sharedCardId = Some("card-hot"))
    val orders = List(order("o-1", "cust-1"), order("o-2", "cust-2"))

    val found = RingDetection.rings(graphOf(orders, List(plan)))

    assertEquals(found.map(_.members), List(List("cust-1", "cust-2")))
    assertEquals(found.head.sharedArtefacts, List("card-hot"))
  }

  test("rings that overlap in one account merge into a single component") {
    // cust-2 is in both plans, so all four accounts belong to one ring even though
    // cust-1 and cust-4 share nothing directly. This is the case a fixed-depth SQL
    // self-join gets wrong.
    val plans = List(
      RingPlan("device", List("cust-1", "cust-2"), sharedDeviceId = Some("device-farm")),
      RingPlan("address", List("cust-2", "cust-3", "cust-4"), sharedAddressId = Some("address-drop"))
    )
    val orders = (1 to 4).map(n => order(s"o-$n", s"cust-$n")).toList

    val found = RingDetection.rings(graphOf(orders, plans))

    assertEquals(found.size, 1)
    assertEquals(found.head.members, List("cust-1", "cust-2", "cust-3", "cust-4"))
    assertEquals(found.head.sharedArtefacts, List("address-drop", "device-farm"))
  }

  test("a customer's own repeated orders are not a ring") {
    val orders = List(order("o-1", "cust-1"), order("o-2", "cust-1"), order("o-3", "cust-1"))
    assertEquals(RingDetection.rings(graphOf(orders, Nil)), Nil)
  }

  test("minimumSize filters out the smallest groups") {
    val plans = List(
      RingPlan("pair", List("cust-1", "cust-2"), sharedCardId = Some("card-a")),
      RingPlan("triple", List("cust-3", "cust-4", "cust-5"), sharedCardId = Some("card-b"))
    )
    val orders = (1 to 5).map(n => order(s"o-$n", s"cust-$n")).toList

    val found = RingDetection.rings(graphOf(orders, plans), minimumSize = 3)

    assertEquals(found.map(_.members), List(List("cust-3", "cust-4", "cust-5")))
  }

  test("two accounts sharing a card are four hops apart") {
    val plan   = RingPlan("card", List("cust-1", "cust-2"), sharedCardId = Some("card-hot"))
    val orders = List(order("o-1", "cust-1"), order("o-2", "cust-2"))
    val graph  = graphOf(orders, List(plan))

    val path = RingDetection.shortestPath(graph, "cust-1", "cust-2")

    assertEquals(path.map(_.vertices), Some(List("cust-1", "o-1", "card-hot", "o-2", "cust-2")))
    assertEquals(path.map(_.hops), Some(4))
  }

  test("unrelated accounts have no path") {
    val orders = List(order("o-1", "cust-1"), order("o-2", "cust-2"))
    assertEquals(RingDetection.shortestPath(graphOf(orders, Nil), "cust-1", "cust-2"), None)
  }

  test("a hop budget shorter than the path finds nothing") {
    val plan   = RingPlan("card", List("cust-1", "cust-2"), sharedCardId = Some("card-hot"))
    val orders = List(order("o-1", "cust-1"), order("o-2", "cust-2"))
    val graph  = graphOf(orders, List(plan))

    assertEquals(RingDetection.shortestPath(graph, "cust-1", "cust-2", maxDepth = 3), None)
    assert(RingDetection.shortestPath(graph, "cust-1", "cust-2", maxDepth = 4).isDefined)
  }

  test("an unknown vertex has no path") {
    val orders = List(order("o-1", "cust-1"))
    assertEquals(RingDetection.shortestPath(graphOf(orders, Nil), "cust-1", "cust-nobody"), None)
  }

  test("the four-hop neighbourhood of an account reaches the accounts it shares a card with") {
    val plan   = RingPlan("card", List("cust-1", "cust-2"), sharedCardId = Some("card-hot"))
    val orders = List(order("o-1", "cust-1"), order("o-2", "cust-2"))
    val graph  = graphOf(orders, List(plan))

    val within2 = RingDetection.kHopNeighbourhood(graph, "cust-1", 2)
    val within4 = RingDetection.kHopNeighbourhood(graph, "cust-1", 4)

    assert(!within2.contains("cust-2"), "two hops only reach the shared card")
    assert(within4.contains("cust-2"), "four hops reach the other account")
  }

  test("degree centrality ranks the shared card above private ones") {
    val plan   = RingPlan("card", List("cust-1", "cust-2", "cust-3"), sharedCardId = Some("card-hot"))
    val orders = (1 to 4).map(n => order(s"o-$n", s"cust-$n")).toList

    val ranked = RingDetection.degreeCentrality(graphOf(orders, List(plan)), FraudSchema.Vertices.Card, limit = 2)

    assertEquals(ranked.head, ("card-hot", 3))
    assertEquals(ranked(1)._2, 1)
  }

  test("the generated sample graph contains exactly the planted rings") {
    val graph = ShopGraph.sample(orderCount = 200)

    val found = RingDetection.rings(graph)

    // The device farm and the drop address overlap in cust-0203, so they merge into
    // one component of five accounts; the stolen card stays a separate ring of four.
    assertEquals(found.map(_.size), List(5, 4))
    assert(found.exists(_.members == List("cust-0101", "cust-0102", "cust-0103", "cust-0104")))
    assert(found.exists(_.sharedArtefacts.contains("address-drop-1")))
  }
}
