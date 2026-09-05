package de.flink.s3sink.job

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.tuple.{Tuple2 => FlinkTuple2, Tuple3 => FlinkTuple3}

/**
 * The shapes that travel between Apache Flink operators.
 *
 * Flink has to serialise every record that crosses an operator boundary. It ships fast, purpose-built serialisers for
 * its own `Tuple` types and for Java primitives, but it falls back to the general-purpose Kryo library for Scala case
 * classes that contain Scala collections -- which is slow and, on some Scala versions, outright fragile.
 *
 * The domain model therefore stays pure Scala inside the operators, and only these flat tuples cross the wire. The cost
 * is that the JSON payload is parsed a second time inside the windowing operator; the benefit is a pipeline that never
 * depends on Kryo.
 */
object OrderRecords {

  /** `(customerId, eventTimeMillis, rawOrderJson)` flowing from Kafka into the window. */
  type Incoming = FlinkTuple3[String, java.lang.Long, String]

  /** `(bucketDirectory, jsonLine)` flowing from the window into object storage. */
  type Outgoing = FlinkTuple2[String, String]

  def incoming(customerId: String, eventTimeMillis: Long, rawOrderJson: String): Incoming =
    FlinkTuple3.of(customerId, java.lang.Long.valueOf(eventTimeMillis), rawOrderJson)

  def outgoing(bucketDirectory: String, jsonLine: String): Outgoing =
    FlinkTuple2.of(bucketDirectory, jsonLine)

  /** Flink cannot infer a type through a Scala type alias, so both are named explicitly. */
  val incomingTypeInformation: TypeInformation[Incoming] =
    Types.TUPLE[Incoming](Types.STRING, Types.LONG, Types.STRING)

  val outgoingTypeInformation: TypeInformation[Outgoing] =
    Types.TUPLE[Outgoing](Types.STRING, Types.STRING)

  def customerIdOf(record: Incoming): String   = record.f0
  def eventTimeOf(record: Incoming): Long      = record.f1.longValue()
  def rawOrderJsonOf(record: Incoming): String = record.f2
}
