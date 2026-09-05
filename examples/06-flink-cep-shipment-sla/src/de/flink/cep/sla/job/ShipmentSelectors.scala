package de.flink.cep.sla.job

import de.flink.cep.sla.core.ShipmentRecords
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.java.functions.KeySelector

/**
 * The two small functions Apache Flink has to ship to every worker machine.
 *
 * They are named classes rather than Scala lambdas for the serialisation reason explained in `StatusFilter`.
 */
/**
 * Keys the stream by order.
 *
 * This is what makes the patterns *per order*: Flink runs one independent state machine per key, so the `Dispatched`
 * event of one order can never complete the pattern started by another order's `Created` event.
 */
@SerialVersionUID(1L)
final class OrderKeySelector extends KeySelector[ShipmentRecords.Event, String] {
  override def getKey(record: ShipmentRecords.Event): String = ShipmentRecords.orderIdOf(record)
}

/** Tells Flink which field of a record carries its event time. */
@SerialVersionUID(1L)
final class ShipmentEventTimeAssigner extends SerializableTimestampAssigner[ShipmentRecords.Event] {
  override def extractTimestamp(record: ShipmentRecords.Event, recordTimestamp: Long): Long =
    ShipmentRecords.eventTimeOf(record)
}
