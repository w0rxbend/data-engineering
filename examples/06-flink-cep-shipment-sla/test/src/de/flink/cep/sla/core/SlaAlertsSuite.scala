package de.flink.cep.sla.core

import de.common.domain.{OrderId, Shipment, ShipmentStatus}

/** The business rules of the example, checked without Apache Flink, Apache Kafka or Docker anywhere in sight. */
final class SlaAlertsSuite extends munit.FunSuite {

  private val oneHour = 3600000L
  private val policy  = SlaPolicy(dispatchWithinMillis = 4 * oneHour, deliverWithinMillis = 48 * oneHour)
  private val orderId = OrderId("order-0000042")

  private def milestone(status: ShipmentStatus, atHour: Long): Shipment =
    Shipment(orderId, status, atHour * oneHour)

  test("a deadline is the anchoring event plus the promised window") {
    assertEquals(policy.dispatchDeadline(milestone(ShipmentStatus.Created, 1)), 5 * oneHour)
    assertEquals(policy.deliveryDeadline(milestone(ShipmentStatus.Dispatched, 5)), 53 * oneHour)
  }

  test("an event exactly on the deadline still counts as in time") {
    val created    = milestone(ShipmentStatus.Created, 1)
    val dispatched = milestone(ShipmentStatus.Dispatched, 5)
    assert(policy.isDispatchedInTime(created, dispatched))
    assert(!policy.isDispatchedInTime(created, milestone(ShipmentStatus.Dispatched, 6)))
  }

  test("a kept promise is reported as a non-breach") {
    val alert = SlaAlerts.deliveredInTime(
      milestone(ShipmentStatus.Dispatched, 5),
      milestone(ShipmentStatus.Delivered, 20),
      policy
    )
    assertEquals(alert.outcome, SlaOutcome.DeliveredInTime)
    assert(!alert.outcome.isBreach)
    assertEquals(alert.lastObservedStatus, ShipmentStatus.Delivered)
    assertEquals(alert.deadlineEpochMillis, 53 * oneHour)
    assertEquals(alert.latenessMillis, -33 * oneHour)
    assertEquals(alert.message, "Order order-0000042 was delivered 15.0 hours after dispatch.")
  }

  test("a missing dispatch scan reports the observed creation time separately from the deadline") {
    val alert = SlaAlerts.notDispatchedInTime(milestone(ShipmentStatus.Created, 1), policy)
    assertEquals(alert.outcome, SlaOutcome.NotDispatchedInTime)
    assert(alert.outcome.isBreach)
    assertEquals(alert.lastObservedStatus, ShipmentStatus.Created)
    assertEquals(alert.lastObservedAtEpochMillis, oneHour)
    assertEquals(alert.evaluatedAtEpochMillis, 5 * oneHour)
    assertEquals(alert.latenessMillis, 0L)
    assertEquals(alert.message, "Order order-0000042 had no dispatch scan 4.0 hours after it was created.")
  }

  test("a parcel lost with the carrier is reported as a delivery breach") {
    val alert = SlaAlerts.notDeliveredInTime(milestone(ShipmentStatus.Dispatched, 5), policy)
    assertEquals(alert.outcome, SlaOutcome.NotDeliveredInTime)
    assertEquals(alert.lastObservedStatus, ShipmentStatus.Dispatched)
    assertEquals(alert.deadlineEpochMillis, 53 * oneHour)
    assertEquals(alert.message, "Order order-0000042 was still undelivered 48.0 hours after dispatch.")
  }

  test("outcome names survive a round trip through their wire form") {
    SlaOutcome.all.foreach(outcome => assertEquals(SlaOutcome.fromString(outcome.name), Some(outcome)))
    assertEquals(SlaOutcome.fromString("Something else"), None)
  }

  test("a policy without a positive window is rejected on construction") {
    intercept[IllegalArgumentException](SlaPolicy(0L, oneHour))
    intercept[IllegalArgumentException](SlaPolicy(oneHour, -1L))
  }
}
