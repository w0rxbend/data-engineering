package de.flink.cep.sla.job

import de.common.domain.{OrderId, Shipment, ShipmentStatus}
import de.flink.cep.sla.core.{ShipmentRecords, SlaPolicy}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.JavaConverters._

/**
 * Runs the real pattern matcher end to end on a local mini cluster.
 *
 * `StreamExecutionEnvironment.getExecutionEnvironment` returns a local environment when no cluster is configured: Flink
 * starts a job manager and a task manager inside this Java virtual machine, runs the job, and shuts them down again. No
 * Docker, no Kafka, and the whole suite finishes in a few seconds.
 *
 * The input is a *bounded* stream. When a bounded stream ends, Flink emits a final watermark at the largest possible
 * timestamp, which is what makes every still-incomplete partial match time out before the job finishes.
 */
final class ShipmentSlaPipelineSuite extends munit.FunSuite {

  private val hour   = 3600000L
  private val origin = 1700000000000L
  private val policy = SlaPolicy(dispatchWithinMillis = 4 * hour, deliverWithinMillis = 48 * hour)

  private def milestone(order: String, status: ShipmentStatus, atHour: Long): Shipment =
    Shipment(OrderId(order), status, origin + atHour * hour)

  /**
   * The four orders under test, interleaved in event time the way a carrier feed arrives.
   *
   *   - `order-happy` completes both promises.
   *   - `order-stuck` is created and never dispatched.
   *   - `order-lost` is dispatched and never delivered.
   *   - `order-slow` is dispatched three hours too late.
   */
  private val milestones: List[Shipment] = List(
    milestone("order-happy", ShipmentStatus.Created, 0),
    milestone("order-stuck", ShipmentStatus.Created, 1),
    milestone("order-happy", ShipmentStatus.Dispatched, 1),
    milestone("order-lost", ShipmentStatus.Created, 2),
    milestone("order-slow", ShipmentStatus.Created, 2),
    milestone("order-lost", ShipmentStatus.Dispatched, 3),
    milestone("order-happy", ShipmentStatus.Delivered, 10),
    milestone("order-slow", ShipmentStatus.Dispatched, 9),
    milestone("order-slow", ShipmentStatus.Delivered, 20)
  ).sortBy(_.occurredAtEpochMillis)

  /** `(orderId, outcome)` for every alert the job produced, sorted so the assertions are stable. */
  private lazy val alerts: List[(String, String)] = runPipeline()

  private def runPipeline(): List[(String, String)] = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val events = env
      .fromData(milestones.map(ShipmentRecords.fromShipment).asJava, ShipmentRecords.eventTypeInformation)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forMonotonousTimestamps[ShipmentRecords.Event]()
          .withTimestampAssigner(new ShipmentEventTimeAssigner)
      )

    val streams = ShipmentSlaPipeline.detect(events, policy)

    streams.completions
      .union(streams.breaches)
      .executeAndCollect("shipment-sla-test")
      .asScala
      .toList
      .map(alert => (alert.f0, outcomeOf(ShipmentRecords.alertJsonOf(alert))))
      .sorted
  }

  /** Pulls the `outcome` field out of the alert JSON without dragging a parser into the test. */
  private def outcomeOf(json: String): String =
    json.split("\"outcome\":\"")(1).takeWhile(_ != '"')

  test("an order that keeps both promises produces two kept-promise statements") {
    assertEquals(
      alerts.filter(_._1 == "order-happy"),
      List(("order-happy", "DeliveredInTime"), ("order-happy", "DispatchedInTime"))
    )
  }

  test("an order that is never dispatched is reported as a dispatch breach") {
    assertEquals(alerts.filter(_._1 == "order-stuck"), List(("order-stuck", "NotDispatchedInTime")))
  }

  test("an order that is dispatched but never delivered is reported as a delivery breach") {
    assertEquals(
      alerts.filter(_._1 == "order-lost"),
      List(("order-lost", "DispatchedInTime"), ("order-lost", "NotDeliveredInTime"))
    )
  }

  test("a dispatch that arrives after the deadline is a breach, and the delivery promise still holds") {
    assertEquals(
      alerts.filter(_._1 == "order-slow"),
      List(("order-slow", "DeliveredInTime"), ("order-slow", "NotDispatchedInTime"))
    )
  }

  test("the two patterns of one order do not interfere with another order's state machine") {
    assertEquals(alerts.size, 7)
  }
}
