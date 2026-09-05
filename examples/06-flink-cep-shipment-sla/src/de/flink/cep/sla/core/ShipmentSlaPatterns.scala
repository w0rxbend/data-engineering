package de.flink.cep.sla.core

import de.common.domain.ShipmentStatus
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition

import java.time.Duration

/**
 * The two event sequences this job looks for, written in Apache Flink's CEP (Complex Event Processing) pattern
 * language.
 *
 * A pattern is a description, not a computation: `begin`, `next`, `followedBy` and `within` build a small immutable
 * object that the Flink runtime later compiles into a state machine. Because building one has no side effects, the
 * definitions live here in the pure core next to the business rules, and a test can inspect or run them without a
 * cluster.
 *
 * The vocabulary used below:
 *   - `begin(name)` starts a pattern and gives its first step a name; the name is how a match is read back later.
 *   - `next(name)` demands *strict contiguity*: the very next event for the same order must match, and any other event
 *     in between destroys the partial match.
 *   - `followedBy(name)` is *relaxed*: unrelated events in between are skipped over.
 *   - `within(duration)` bounds the whole sequence in event time. A partial match that is still incomplete when the
 *     watermark passes the bound is reported as a *timed-out partial match*, and that is precisely what an SLA breach
 *     is.
 */
object ShipmentSlaPatterns {

  /** Step names. A matched sequence is handed back as `Map[stepName, List[event]]`, so these names are the API. */
  val CreatedStep    = "created"
  val DispatchedStep = "dispatched"
  val DeliveredStep  = "delivered"

  /**
   * `Created` immediately followed by `Dispatched`, within the promised handover time.
   *
   * `next` is the right operator here: between creation and dispatch there is no legitimate other milestone. An order
   * that jumps straight from `Created` to `Delivered` (a missing warehouse scan) therefore breaks the partial match and
   * is reported as "never dispatched", which is exactly the operational problem worth alerting on.
   */
  def dispatchPattern(policy: SlaPolicy): Pattern[ShipmentRecords.Event, ShipmentRecords.Event] =
    Pattern
      .begin[ShipmentRecords.Event](CreatedStep)
      .where(hasStatus(ShipmentStatus.Created))
      .next(DispatchedStep)
      .where(hasStatus(ShipmentStatus.Dispatched))
      .within(Duration.ofMillis(policy.dispatchWithinMillis))

  /**
   * `Dispatched` eventually followed by `Delivered`, within the promised travel time.
   *
   * `followedBy` is the right operator here: while a parcel is with the carrier, other events for the same order (a
   * re-scan, a partial shipment) may legitimately appear and must not cancel the match.
   */
  def deliveryPattern(policy: SlaPolicy): Pattern[ShipmentRecords.Event, ShipmentRecords.Event] =
    Pattern
      .begin[ShipmentRecords.Event](DispatchedStep)
      .where(hasStatus(ShipmentStatus.Dispatched))
      .followedBy(DeliveredStep)
      .where(hasStatus(ShipmentStatus.Delivered))
      .within(Duration.ofMillis(policy.deliverWithinMillis))

  private def hasStatus(status: ShipmentStatus): SimpleCondition[ShipmentRecords.Event] =
    SimpleCondition.of(new StatusFilter(status.toString))
}

/**
 * "Is this event of that status?" as a named class rather than a Scala lambda.
 *
 * Flink ships every condition to the worker machines by serialising it. A Scala 2.12 lambda is compiled into a
 * synthetic method plus a `SerializedLambda` record, and reconstructing it on the cluster needs the exact Scala runtime
 * helper that produced it. When the job jar and the Flink distribution each bring their own copy of the Scala library,
 * that reconstruction fails with a puzzling `InvalidObjectException`. A plain class is serialised by name and has no
 * such problem.
 */
@SerialVersionUID(1L)
final class StatusFilter(expectedStatus: String) extends FilterFunction[ShipmentRecords.Event] {
  override def filter(record: ShipmentRecords.Event): Boolean =
    ShipmentRecords.statusOf(record) == expectedStatus
}
