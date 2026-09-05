package de.hugegraph.fraud

import de.common.domain.{CustomerId, Money, Order, OrderId, OrderLine, Sku}
import de.hugegraph.fraud.Accounts.RingPlan
import de.hugegraph.fraud.FraudSchema.{Edges, Vertices}

/** Tests for the mapping from shop orders to the property graph, and for the artefact assignment behind it. */
class ShopGraphSuite extends munit.FunSuite {

  private def order(id: String, customer: String): Order =
    Order(
      id = OrderId(id),
      customerId = CustomerId(customer),
      lines = List(OrderLine(Sku("SKU-MUG"), 2, Money.eur(150))),
      placedAtEpochMillis = 1700000000000L,
      country = "PL"
    )

  test("each customer gets private artefacts by default") {
    val accounts = Accounts.assign(List(CustomerId("cust-0042")), Nil)

    assertEquals(accounts("cust-0042").cardId, "card-0042")
    assertEquals(accounts("cust-0042").deviceId, "device-0042")
    assertEquals(accounts("cust-0042").addressId, "address-0042")
  }

  test("a ring plan overrides only the artefact it names") {
    val plan     = RingPlan("card", List("cust-0042"), sharedCardId = Some("card-hot"))
    val accounts = Accounts.assign(List(CustomerId("cust-0042")), List(plan))

    assertEquals(accounts("cust-0042").cardId, "card-hot")
    assertEquals(accounts("cust-0042").deviceId, "device-0042")
  }

  test("a ring plan naming a customer that is absent from the batch is ignored") {
    val plan = RingPlan("card", List("cust-9999"), sharedCardId = Some("card-hot"))

    assertEquals(Accounts.assign(List(CustomerId("cust-0042")), List(plan)).keySet, Set("cust-0042"))
  }

  test("artefact attributes depend only on the artefact identifier") {
    assertEquals(Accounts.issuerOf("card-0042"), Accounts.issuerOf("card-0042"))
    assertEquals(Accounts.cityOf("address-0042"), Accounts.cityOf("address-0042"))
  }

  test("one order becomes five vertices and four edges") {
    val orders = List(order("o-1", "cust-1"))
    val graph  = ShopGraph.build(orders, Accounts.assign(orders.map(_.customerId), Nil))

    assertEquals(graph.vertices.map(_.label).sorted, List("address", "card", "customer", "device", "order"))
    assertEquals(graph.edges.map(_.label).sorted, Edges.all.sorted)
  }

  test("a repeated customer or artefact appears only once as a vertex") {
    val orders = List(order("o-1", "cust-1"), order("o-2", "cust-1"))
    val graph  = ShopGraph.build(orders, Accounts.assign(orders.map(_.customerId), Nil))

    assertEquals(graph.verticesWithLabel(Vertices.Customer).size, 1)
    assertEquals(graph.verticesWithLabel(Vertices.Order).size, 2)
    assertEquals(graph.verticesWithLabel(Vertices.Card).size, 1)
  }

  test("an order vertex carries the order total in cents") {
    val orders = List(order("o-1", "cust-1"))
    val graph  = ShopGraph.build(orders, Accounts.assign(orders.map(_.customerId), Nil))

    val total = graph.verticesWithLabel(Vertices.Order).head.properties(FraudSchema.Properties.TotalCents)
    assertEquals(total, PropertyGraph.Value.Number(300L))
  }

  test("every edge endpoint refers to a vertex that is in the graph") {
    val graph = ShopGraph.sample(orderCount = 50)
    val ids   = graph.vertices.map(_.id).toSet

    val dangling = graph.edges.filterNot(edge => ids.contains(edge.outV) && ids.contains(edge.inV))
    assertEquals(dangling, Nil)
  }

  test("every vertex and edge label used matches the declared schema") {
    val graph        = ShopGraph.sample(orderCount = 50)
    val vertexLabels = FraudSchema.shop.vertexLabels.map(_.name).toSet
    val edgeLabels   = FraudSchema.shop.edgeLabels.map(_.name).toSet

    assertEquals(graph.vertices.map(_.label).toSet.diff(vertexLabels), Set.empty[String])
    assertEquals(graph.edges.map(_.label).toSet.diff(edgeLabels), Set.empty[String])
  }

  test("the sample graph is reproducible from its seed") {
    assertEquals(ShopGraph.sample(orderCount = 30), ShopGraph.sample(orderCount = 30))
  }

  test("every planted ring member is present regardless of what the generator happened to produce") {
    val graph   = ShopGraph.sample(orderCount = 5)
    val present = graph.verticesWithLabel(Vertices.Customer).map(_.id).toSet

    assert(ShopGraph.defaultRingPlans.flatMap(_.members).forall(present.contains))
  }
}
