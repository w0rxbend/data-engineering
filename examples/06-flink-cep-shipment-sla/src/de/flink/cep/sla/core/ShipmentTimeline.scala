package de.flink.cep.sla.core

import de.common.domain.{Order, Shipment}

/**
 * Shapes the demonstration data: which shipment milestones an order actually reports, and how far apart in event time
 * the orders are.
 *
 * A pattern matcher is only interesting when some patterns fail to complete, so this is where the example deliberately
 * loses events. The rules are arithmetic on the order's position in the batch rather than random draws, which means a
 * reader can predict exactly which alerts a run will produce.
 */
object ShipmentTimeline {

  /**
   * Which milestones go missing.
   *
   * @param missingDispatchEvery
   *   every n-th order reports `Created` and nothing else: the parcel never left the warehouse
   * @param missingDeliveryEvery
   *   every n-th order reports `Created` and `Dispatched` but no `Delivered`: the parcel is lost with the carrier
   */
  final case class Faults(missingDispatchEvery: Int, missingDeliveryEvery: Int) {
    require(missingDispatchEvery > 0, s"missingDispatchEvery must be positive, but was $missingDispatchEvery")
    require(missingDeliveryEvery > 0, s"missingDeliveryEvery must be positive, but was $missingDeliveryEvery")

    /** How many of the three milestones order number `index` (counted from one) reports. */
    def milestoneCount(index: Int): Int =
      if (index % missingDispatchEvery == 0) 1
      else if (index % missingDeliveryEvery == 0) 2
      else 3
  }

  object Faults {

    /** Every seventh order is never dispatched, every fourth of the rest is never delivered. */
    val default: Faults = Faults(missingDispatchEvery = 7, missingDeliveryEvery = 4)
  }

  /** The milestones order number `index` reports, in event-time order. */
  def milestonesFor(index: Int, milestones: List[Shipment], faults: Faults): List[Shipment] =
    milestones.take(faults.milestoneCount(index))

  /**
   * Pulls the orders of a batch apart in event time.
   *
   * The shared data generator places its orders about half a second apart, while a delivery promise is measured in
   * hours and days. Multiplying the distance from the first order by `factor` stretches a batch of a few hundred orders
   * across weeks of event time, so deadlines actually pass. Only the timestamps inside the data change; the job still
   * processes everything as fast as the machine allows.
   */
  def stretch(order: Order, originEpochMillis: Long, factor: Long): Order = {
    require(factor > 0L, s"factor must be positive, but was $factor")
    order.copy(placedAtEpochMillis = originEpochMillis + (order.placedAtEpochMillis - originEpochMillis) * factor)
  }
}
