package de.flink.cep.sla.core

import de.common.domain.{OrderId, Shipment, ShipmentStatus}
import de.common.gen.DataGenerator

final class ShipmentTimelineSuite extends munit.FunSuite {

  private val faults = ShipmentTimeline.Faults.default

  private val allMilestones = List(
    Shipment(OrderId("order-1"), ShipmentStatus.Created, 1L),
    Shipment(OrderId("order-1"), ShipmentStatus.Dispatched, 2L),
    Shipment(OrderId("order-1"), ShipmentStatus.Delivered, 3L)
  )

  private def statuses(index: Int): List[ShipmentStatus] =
    ShipmentTimeline.milestonesFor(index, allMilestones, faults).map(_.status)

  test("most orders report the whole lifecycle") {
    assertEquals(statuses(1), List(ShipmentStatus.Created, ShipmentStatus.Dispatched, ShipmentStatus.Delivered))
  }

  test("every fourth order stops after dispatch") {
    assertEquals(statuses(4), List(ShipmentStatus.Created, ShipmentStatus.Dispatched))
  }

  test("every seventh order stops after creation, and that rule wins when both apply") {
    assertEquals(statuses(7), List(ShipmentStatus.Created))
    assertEquals(statuses(28), List(ShipmentStatus.Created))
  }

  test("stretching moves later orders further away and leaves the first one where it is") {
    val generator = new DataGenerator(seed = 1L)
    val origin    = generator.nextOrder().placedAtEpochMillis
    val order     = generator.nextOrder()
    val stretched = ShipmentTimeline.stretch(order, origin, 1000L)

    assertEquals(stretched.placedAtEpochMillis - origin, (order.placedAtEpochMillis - origin) * 1000L)
    assertEquals(
      ShipmentTimeline.stretch(order.copy(placedAtEpochMillis = origin), origin, 1000L).placedAtEpochMillis,
      origin
    )
  }
}
