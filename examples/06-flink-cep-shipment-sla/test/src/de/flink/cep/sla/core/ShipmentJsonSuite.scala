package de.flink.cep.sla.core

import de.common.domain.{OrderId, Shipment, ShipmentStatus}

final class ShipmentJsonSuite extends munit.FunSuite {

  private val shipment = Shipment(OrderId("order-0000042"), ShipmentStatus.Dispatched, 1700000000000L)

  private def decode(payload: String): Either[ShipmentJson.DecodeFailure, Shipment] =
    ShipmentJson.decode(payload.getBytes("UTF-8"))

  test("an event written by the shared codec is read back unchanged") {
    assertEquals(decode(ShipmentJson.encodeShipment(shipment)), Right(shipment))
  }

  test("every shipment status of the shared domain is understood") {
    ShipmentStatus.all.foreach { status =>
      val event = shipment.copy(status = status)
      assertEquals(decode(ShipmentJson.encodeShipment(event)), Right(event))
    }
  }

  test("a record with an unknown status is rejected instead of guessed") {
    val failure = decode("""{"orderId":"order-1","status":"Teleported","occurredAt":1}""")
    assert(failure.isLeft, failure)
    assert(failure.left.exists(_.reason.contains("Teleported")), failure)
  }

  test("a record that is not JSON at all is returned as a failure, never thrown") {
    assert(decode("not json").isLeft)
  }

  test("an alert renders every field a downstream consumer needs") {
    val alert = SlaAlerts.notDeliveredInTime(shipment, SlaPolicy(3600000L, 7200000L))
    val json  = ShipmentJson.encodeAlert(alert)
    assert(json.contains("\"orderId\":\"order-0000042\""), json)
    assert(json.contains("\"outcome\":\"NotDeliveredInTime\""), json)
    assert(json.contains("\"breach\":true"), json)
    assert(json.contains("\"lastStatus\":\"Dispatched\""), json)
    assert(json.contains("\"deadline\":1700007200000"), json)
    assert(json.contains("\"latenessMs\":0"), json)
  }
}
