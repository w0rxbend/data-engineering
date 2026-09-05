package de.flink.cep.sla.core

import de.common.domain.Shipment

/**
 * The delivery promise the online shop makes to its customers.
 *
 * A service-level agreement (SLA) is a promise about *time*: "we hand your parcel to the carrier within four hours, and
 * the carrier delivers it within two days". This value holds those two promises as plain numbers so that both the
 * pattern definitions and the alert texts can be derived from one place instead of repeating a magic constant.
 *
 * @param dispatchWithinMillis
 *   how long an order may sit between the `Created` and the `Dispatched` shipment event
 * @param deliverWithinMillis
 *   how long a parcel may travel between the `Dispatched` and the `Delivered` shipment event
 */
final case class SlaPolicy(dispatchWithinMillis: Long, deliverWithinMillis: Long) {
  require(dispatchWithinMillis > 0L, s"dispatchWithinMillis must be positive, but was $dispatchWithinMillis")
  require(deliverWithinMillis > 0L, s"deliverWithinMillis must be positive, but was $deliverWithinMillis")

  /** The instant by which the parcel of `created` has to be handed to the carrier. */
  def dispatchDeadline(created: Shipment): Long = created.occurredAtEpochMillis + dispatchWithinMillis

  /** The instant by which the parcel of `dispatched` has to be at the customer's door. */
  def deliveryDeadline(dispatched: Shipment): Long = dispatched.occurredAtEpochMillis + deliverWithinMillis

  def isDispatchedInTime(created: Shipment, dispatched: Shipment): Boolean =
    dispatched.occurredAtEpochMillis <= dispatchDeadline(created)

  def isDeliveredInTime(dispatched: Shipment, delivered: Shipment): Boolean =
    delivered.occurredAtEpochMillis <= deliveryDeadline(dispatched)
}

object SlaPolicy {

  /** The promise used when nothing else is configured: dispatch within four hours, deliver within two days. */
  val default: SlaPolicy = SlaPolicy(dispatchWithinMillis = 4L * 3600000L, deliverWithinMillis = 2L * 86400000L)
}
