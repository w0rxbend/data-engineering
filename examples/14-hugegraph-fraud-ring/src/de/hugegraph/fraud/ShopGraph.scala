package de.hugegraph.fraud

import de.common.domain.Order
import de.common.gen.DataGenerator
import de.hugegraph.fraud.Accounts.{Account, RingPlan}
import de.hugegraph.fraud.FraudSchema.{Edges, Properties, Vertices}
import de.hugegraph.fraud.PropertyGraph.{Edge, Graph, Value, Vertex}

/**
 * Turns a batch of shop orders into the property graph described by [[FraudSchema]].
 *
 * This is the whole "modelling" step of the example, and it is a pure function: orders in, graph out. Nothing here
 * knows that HugeGraph exists, which is why the same output can be asserted on in a unit test and posted to a server by
 * [[Main]].
 */
object ShopGraph {

  private def text(value: String): Value = Value.Text(value)
  private def number(value: Long): Value = Value.Number(value)

  /**
   * The rings this example plants.
   *
   * Three shapes, each a different real-world story:
   *
   *   - **card ring** - four accounts charging one stolen card. The classic carding pattern.
   *   - **device ring** - three accounts created from one phone. A bonus-abuse or account-farm pattern.
   *   - **address ring** - three accounts shipping to one drop address, and one of them also uses the device above,
   *     which is what makes the device ring and the address ring merge into a single six-account component. Rings that
   *     overlap are exactly the case a graph handles and a pairwise SQL query does not.
   */
  val defaultRingPlans: List[RingPlan] = List(
    RingPlan(
      name = "stolen card ring",
      members = List("cust-0101", "cust-0102", "cust-0103", "cust-0104"),
      sharedCardId = Some("card-stolen-1")
    ),
    RingPlan(
      name = "single device farm",
      members = List("cust-0201", "cust-0202", "cust-0203"),
      sharedDeviceId = Some("device-farm-1")
    ),
    RingPlan(
      name = "drop address",
      members = List("cust-0203", "cust-0301", "cust-0302"),
      sharedAddressId = Some("address-drop-1")
    )
  )

  /**
   * Generates `orderCount` orders from the shared seeded generator and forces the ring members to appear.
   *
   * The generator picks customer identifiers at random out of a thousand, so the planted ring members would show up
   * only by luck. Appending one order per ring member guarantees every planted account is in the graph while leaving
   * the random background traffic untouched.
   */
  def generateOrders(orderCount: Int, plans: List[RingPlan], seed: Long = 42L): List[Order] = {
    val generator   = new DataGenerator(seed)
    val background  = generator.orders(orderCount)
    val ringMembers = plans.flatMap(_.members).distinct
    val ringOrders  = ringMembers.map { member =>
      generator.nextOrder().copy(customerId = de.common.domain.CustomerId(member))
    }
    background ++ ringOrders
  }

  /** Builds the graph for a batch of orders, using the artefact assignment produced by [[Accounts.assign]]. */
  def build(orders: List[Order], accounts: Map[String, Account]): Graph = {
    val vertices = List.newBuilder[Vertex]
    val edges    = List.newBuilder[Edge]

    orders.foreach { order =>
      val customerId = order.customerId.value
      val account    = accounts.getOrElse(customerId, Accounts.privateAccount(customerId))
      val orderId    = order.id.value

      vertices += Vertex(
        Vertices.Customer,
        customerId,
        Map(Properties.CustomerId -> text(customerId), Properties.Country -> text(order.country))
      )
      vertices += Vertex(
        Vertices.Order,
        orderId,
        Map(
          Properties.OrderId    -> text(orderId),
          Properties.TotalCents -> number(order.total.cents),
          Properties.PlacedAt   -> number(order.placedAtEpochMillis),
          Properties.Country    -> text(order.country)
        )
      )
      vertices += Vertex(
        Vertices.Card,
        account.cardId,
        Map(Properties.CardId -> text(account.cardId), Properties.Issuer -> text(Accounts.issuerOf(account.cardId)))
      )
      vertices += Vertex(
        Vertices.Device,
        account.deviceId,
        Map(
          Properties.DeviceId -> text(account.deviceId),
          Properties.Platform -> text(Accounts.platformOf(account.deviceId))
        )
      )
      vertices += Vertex(
        Vertices.Address,
        account.addressId,
        Map(
          Properties.AddressId -> text(account.addressId),
          Properties.City      -> text(Accounts.cityOf(account.addressId)),
          Properties.Country   -> text(order.country)
        )
      )

      edges += Edge(
        Edges.Placed,
        customerId,
        Vertices.Customer,
        orderId,
        Vertices.Order,
        Map(Properties.PlacedAt -> number(order.placedAtEpochMillis))
      )
      edges += Edge(
        Edges.PaidWith,
        orderId,
        Vertices.Order,
        account.cardId,
        Vertices.Card,
        Map(Properties.AmountCents -> number(order.total.cents))
      )
      edges += Edge(Edges.PlacedFrom, orderId, Vertices.Order, account.deviceId, Vertices.Device, Map.empty)
      edges += Edge(Edges.ShipsTo, orderId, Vertices.Order, account.addressId, Vertices.Address, Map.empty)
    }

    Graph.of(vertices.result(), edges.result())
  }

  /** The one call [[Main]] needs: orders, artefacts and rings in a single reproducible graph. */
  def sample(orderCount: Int, plans: List[RingPlan] = defaultRingPlans, seed: Long = 42L): Graph = {
    val orders   = generateOrders(orderCount, plans, seed)
    val accounts = Accounts.assign(orders.map(_.customerId), plans)
    build(orders, accounts)
  }
}
