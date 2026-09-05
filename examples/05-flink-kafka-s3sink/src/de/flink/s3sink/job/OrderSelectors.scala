package de.flink.s3sink.job

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.java.functions.KeySelector

/**
 * The two small functions Apache Flink has to ship to every worker machine.
 *
 * They are written as named classes rather than as Scala lambdas on purpose. A Scala 2.12 lambda is compiled into a
 * synthetic method plus a `SerializedLambda` record, and reconstructing it on the cluster requires the exact Scala
 * runtime helper that produced it. When the job jar and the Flink distribution each bring their own copy of the Scala
 * library, that reconstruction fails with a puzzling `InvalidObjectException`. A plain class is serialised by name and
 * has no such problem.
 */
@SerialVersionUID(1L)
final class CustomerKeySelector extends KeySelector[OrderRecords.Incoming, String] {
  override def getKey(record: OrderRecords.Incoming): String = OrderRecords.customerIdOf(record)
}

/** Tells Flink which field of a record carries its event time. */
@SerialVersionUID(1L)
final class OrderEventTimeAssigner extends SerializableTimestampAssigner[OrderRecords.Incoming] {
  override def extractTimestamp(record: OrderRecords.Incoming, recordTimestamp: Long): Long =
    OrderRecords.eventTimeOf(record)
}
