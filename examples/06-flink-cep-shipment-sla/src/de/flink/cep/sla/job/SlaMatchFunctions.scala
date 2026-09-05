package de.flink.cep.sla.job

import de.common.domain.Shipment
import de.flink.cep.sla.core._
import org.apache.flink.cep.functions.{PatternProcessFunction, TimedOutPartialMatchHandler}
import org.apache.flink.util.Collector

import java.util.{List => JList, Map => JMap}

/**
 * Shared plumbing for the two pattern handlers.
 *
 * Apache Flink hands a match back as `Map[stepName, List[event]]`, because a step may be repeated (`oneOrMore`) and
 * then holds several events. Both patterns in this example use single-occurrence steps, so every lookup asks for the
 * first event of a step -- and returns an `Option`, because a *timed-out* match only contains the steps that did
 * complete.
 */
private[job] object SlaMatches {

  def firstEventOf(matched: JMap[String, JList[ShipmentRecords.Event]], step: String): Option[Shipment] =
    Option(matched.get(step))
      .filterNot(_.isEmpty)
      .map(events => ShipmentRecords.toShipment(events.get(0)))
}

/**
 * Handles the first promise: an order that was created must be dispatched inside the promised window.
 *
 * A completed match means the promise was kept and leaves through the main output. A partial match that is still
 * incomplete when the watermark passes the deadline means the parcel never left the warehouse; it leaves through the
 * breach side output.
 */
@SerialVersionUID(1L)
final class DispatchSlaFunction(policy: SlaPolicy)
    extends PatternProcessFunction[ShipmentRecords.Event, ShipmentRecords.Alert]
    with TimedOutPartialMatchHandler[ShipmentRecords.Event] {

  override def processMatch(
      matched: JMap[String, JList[ShipmentRecords.Event]],
      context: PatternProcessFunction.Context,
      out: Collector[ShipmentRecords.Alert]
  ): Unit =
    for {
      created    <- SlaMatches.firstEventOf(matched, ShipmentSlaPatterns.CreatedStep)
      dispatched <- SlaMatches.firstEventOf(matched, ShipmentSlaPatterns.DispatchedStep)
    } out.collect(ShipmentRecords.alertOf(SlaAlerts.dispatchedInTime(created, dispatched, policy)))

  override def processTimedOutMatch(
      matched: JMap[String, JList[ShipmentRecords.Event]],
      context: PatternProcessFunction.Context
  ): Unit =
    SlaMatches
      .firstEventOf(matched, ShipmentSlaPatterns.CreatedStep)
      .foreach { created =>
        context.output(SlaOutputTags.breaches, ShipmentRecords.alertOf(SlaAlerts.notDispatchedInTime(created, policy)))
      }
}

/**
 * Handles the second promise: a parcel that was dispatched must be delivered inside the promised window.
 *
 * The two outlets carry the same meaning as in `DispatchSlaFunction`: main output for a promise kept, breach side
 * output for a partial match that ran out of time.
 */
@SerialVersionUID(1L)
final class DeliverySlaFunction(policy: SlaPolicy)
    extends PatternProcessFunction[ShipmentRecords.Event, ShipmentRecords.Alert]
    with TimedOutPartialMatchHandler[ShipmentRecords.Event] {

  override def processMatch(
      matched: JMap[String, JList[ShipmentRecords.Event]],
      context: PatternProcessFunction.Context,
      out: Collector[ShipmentRecords.Alert]
  ): Unit =
    for {
      dispatched <- SlaMatches.firstEventOf(matched, ShipmentSlaPatterns.DispatchedStep)
      delivered  <- SlaMatches.firstEventOf(matched, ShipmentSlaPatterns.DeliveredStep)
    } out.collect(ShipmentRecords.alertOf(SlaAlerts.deliveredInTime(dispatched, delivered, policy)))

  override def processTimedOutMatch(
      matched: JMap[String, JList[ShipmentRecords.Event]],
      context: PatternProcessFunction.Context
  ): Unit =
    SlaMatches
      .firstEventOf(matched, ShipmentSlaPatterns.DispatchedStep)
      .foreach { dispatched =>
        context.output(
          SlaOutputTags.breaches,
          ShipmentRecords.alertOf(SlaAlerts.notDeliveredInTime(dispatched, policy))
        )
      }
}
