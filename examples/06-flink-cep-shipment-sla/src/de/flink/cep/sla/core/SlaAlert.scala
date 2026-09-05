package de.flink.cep.sla.core

import de.common.domain.{OrderId, Shipment, ShipmentStatus}

/** What the pattern matcher found out about one order. */
sealed trait SlaOutcome extends Product with Serializable {

  /** Stable name used on the wire and in the Apache Kafka topic. */
  def name: String

  /** Whether this outcome means the promise to the customer was broken. */
  def isBreach: Boolean
}

object SlaOutcome {

  /** The parcel was handed to the carrier inside the promised window. */
  case object DispatchedInTime extends SlaOutcome {
    val name     = "DispatchedInTime"
    val isBreach = false
  }

  /** The parcel reached the customer inside the promised window. */
  case object DeliveredInTime extends SlaOutcome {
    val name     = "DeliveredInTime"
    val isBreach = false
  }

  /** The order exists, but no dispatch scan arrived before the deadline. */
  case object NotDispatchedInTime extends SlaOutcome {
    val name     = "NotDispatchedInTime"
    val isBreach = true
  }

  /** The parcel left the warehouse, but no delivery scan arrived before the deadline. */
  case object NotDeliveredInTime extends SlaOutcome {
    val name     = "NotDeliveredInTime"
    val isBreach = true
  }

  val all: List[SlaOutcome] = List(DispatchedInTime, DeliveredInTime, NotDispatchedInTime, NotDeliveredInTime)

  def fromString(raw: String): Option[SlaOutcome] = all.find(_.name.equalsIgnoreCase(raw))
}

/**
 * One statement about one order's delivery promise.
 *
 * @param orderId
 *   the order the statement is about
 * @param outcome
 *   what happened: promise kept, or promise broken
 * @param lastObservedStatus
 *   the newest shipment milestone participating in this partial or completed match; skipped, out-of-order milestones
 *   are deliberately not represented here
 * @param lastObservedAtEpochMillis
 *   event time of that milestone
 * @param evaluatedAtEpochMillis
 *   event time at which the outcome became knowable; the matching event for a completion, or the deadline for a timeout
 * @param deadlineEpochMillis
 *   event time by which the next milestone was due
 * @param message
 *   one sentence a human on the operations team can act on
 */
final case class SlaAlert(
    orderId: OrderId,
    outcome: SlaOutcome,
    lastObservedStatus: ShipmentStatus,
    lastObservedAtEpochMillis: Long,
    evaluatedAtEpochMillis: Long,
    deadlineEpochMillis: Long,
    message: String
) {

  /** Negative while there is still time left, positive once the deadline has passed. */
  def latenessMillis: Long = evaluatedAtEpochMillis - deadlineEpochMillis
}

/**
 * Builds an alert out of the shipment events a pattern matched.
 *
 * These four functions carry every business rule of this example, and none of them mentions Apache Flink, Apache Kafka
 * or JSON. That is deliberate: they can be called from a plain unit test in microseconds, and the streaming job below
 * is left with nothing but wiring.
 */
object SlaAlerts {

  private val MillisPerHour = 3600000.0

  /** The happy path of the first promise: `Created` was followed by `Dispatched` inside the window. */
  def dispatchedInTime(created: Shipment, dispatched: Shipment, policy: SlaPolicy): SlaAlert =
    SlaAlert(
      orderId = dispatched.orderId,
      outcome = SlaOutcome.DispatchedInTime,
      lastObservedStatus = ShipmentStatus.Dispatched,
      lastObservedAtEpochMillis = dispatched.occurredAtEpochMillis,
      evaluatedAtEpochMillis = dispatched.occurredAtEpochMillis,
      deadlineEpochMillis = policy.dispatchDeadline(created),
      message = s"Order ${created.orderId.value} was dispatched ${hours(
          dispatched.occurredAtEpochMillis - created.occurredAtEpochMillis
        )} after it was created."
    )

  /** The happy path of the second promise: `Dispatched` was followed by `Delivered` inside the window. */
  def deliveredInTime(dispatched: Shipment, delivered: Shipment, policy: SlaPolicy): SlaAlert =
    SlaAlert(
      orderId = delivered.orderId,
      outcome = SlaOutcome.DeliveredInTime,
      lastObservedStatus = ShipmentStatus.Delivered,
      lastObservedAtEpochMillis = delivered.occurredAtEpochMillis,
      evaluatedAtEpochMillis = delivered.occurredAtEpochMillis,
      deadlineEpochMillis = policy.deliveryDeadline(dispatched),
      message = s"Order ${delivered.orderId.value} was delivered ${hours(
          delivered.occurredAtEpochMillis - dispatched.occurredAtEpochMillis
        )} after dispatch."
    )

  /**
   * A breach of the first promise. Only the matching `Created` event is represented here -- the whole point is that no
   * `Dispatched` event arrived -- so the deadline, not an observed event, dates the alert. An unrelated or out-of-order
   * milestone may have passed through the stream without satisfying the missing dispatch scan.
   */
  def notDispatchedInTime(created: Shipment, policy: SlaPolicy): SlaAlert = {
    val deadline = policy.dispatchDeadline(created)
    SlaAlert(
      orderId = created.orderId,
      outcome = SlaOutcome.NotDispatchedInTime,
      lastObservedStatus = ShipmentStatus.Created,
      lastObservedAtEpochMillis = created.occurredAtEpochMillis,
      evaluatedAtEpochMillis = deadline,
      deadlineEpochMillis = deadline,
      message = s"Order ${created.orderId.value} had no dispatch scan ${hours(
          policy.dispatchWithinMillis
        )} after it was created."
    )
  }

  /** A breach of the second promise: the parcel left the warehouse but was never scanned as delivered. */
  def notDeliveredInTime(dispatched: Shipment, policy: SlaPolicy): SlaAlert = {
    val deadline = policy.deliveryDeadline(dispatched)
    SlaAlert(
      orderId = dispatched.orderId,
      outcome = SlaOutcome.NotDeliveredInTime,
      lastObservedStatus = ShipmentStatus.Dispatched,
      lastObservedAtEpochMillis = dispatched.occurredAtEpochMillis,
      evaluatedAtEpochMillis = deadline,
      deadlineEpochMillis = deadline,
      message = s"Order ${dispatched.orderId.value} was still undelivered ${hours(
          policy.deliverWithinMillis
        )} after dispatch."
    )
  }

  /** Renders a duration the way an operations dashboard would, for example `4.5 hours`. */
  private def hours(millis: Long): String = f"${millis / MillisPerHour}%.1f hours"
}
