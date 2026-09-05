package de.kafkastreams.fraud

import de.common.domain.{CustomerId, Money, PaymentStatus}
import de.common.gen.DataGenerator

/**
 * Round-trip tests: whatever the writers put on a topic, the readers must turn back into the very same value. A broken
 * Serde is otherwise only noticed at runtime, deep inside a stream thread.
 */
class EventJsonSuite extends munit.FunSuite {

  private val generator = new DataGenerator(seed = 7L)

  test("an order survives the trip through JSON") {
    val order = generator.nextOrder()
    assertEquals(EventJson.readOrder(EventJson.writeOrder(order)), order)
  }

  test("a payment survives the trip through JSON") {
    val payment = generator.paymentFor(generator.nextOrder())
    assertEquals(EventJson.readPayment(EventJson.writePayment(payment)), payment)
  }

  test("a fraud alert survives the trip through JSON") {
    val alert = FraudAlert(CustomerId("cust-0007"), 4, 796L, 1000L, 601000L, "watchlist")
    assertEquals(EventJson.readFraudAlert(EventJson.writeFraudAlert(alert)), alert)
  }

  test("the Serde produces the same bytes the shared codecs produce") {
    val order      = generator.nextOrder()
    val serialized = JsonSerdes.order.serializer().serialize(FraudTopology.OrdersTopic, order)
    assertEquals(new String(serialized, "UTF-8"), de.common.json.Codecs.order(order))
    assertEquals(JsonSerdes.order.deserializer().deserialize(FraudTopology.OrdersTopic, serialized), order)
  }

  test("a null value serializes to null instead of throwing") {
    assert(JsonSerdes.order.serializer().serialize(FraudTopology.OrdersTopic, null) == null)
    assert(JsonSerdes.order.deserializer().deserialize(FraudTopology.OrdersTopic, null) == null)
  }

  test("an unknown payment status is rejected loudly") {
    val broken = """{"orderId":"o-1","amount":{"cents":1,"currency":"EUR"},"status":"Refunded","occurredAt":0}"""
    intercept[IllegalArgumentException](EventJson.readPayment(broken))
  }

  test("a paid order keeps its money and status") {
    val paid = PaidOrder(
      orderId = de.common.domain.OrderId("order-9"),
      customerId = CustomerId("cust-0009"),
      country = "PL",
      amount = Money.eur(1234L),
      status = PaymentStatus.Declined,
      occurredAtEpochMillis = 42L
    )
    assertEquals(EventJson.readPaidOrder(EventJson.writePaidOrder(paid)), paid)
  }
}
