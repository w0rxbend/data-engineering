package de.flink.s3sink.job

import de.common.domain.CustomerId
import de.flink.s3sink.core._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.tuple.{Tuple2 => FlinkTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Buffers one customer's orders until their event-time window closes, then emits a single summary record.
 *
 * How the timer works, step by step:
 *
 *   1. An order arrives. Its event time -- `placedAtEpochMillis`, the moment the shop accepted it -- decides which
 *      window it belongs to.
 *   2. If the watermark has already closed that window, the late order is logged and dropped. Otherwise, the raw order
 *      is appended to per-customer Flink state, tagged with that window.
 *   3. An *event-time* timer is registered for the last millisecond of each open window. Flink fires it when the
 *      watermark passes that timestamp, not when the wall clock does. Replaying a week of history therefore produces
 *      exactly the same windows as reading the topic live.
 *   4. `onTimer` hands the buffer to the pure `BatchWindowing.close`, emits the resulting record and writes back only
 *      the orders of still-open windows, so state does not grow without bound.
 *
 * All buffering lives in Flink state, so it is part of every checkpoint: if a machine dies mid-window, the buffered
 * orders come back with it.
 */
@SerialVersionUID(1L)
final class CustomerBatchProcessFunction(windowSizeMillis: Long)
    extends KeyedProcessFunction[String, OrderRecords.Incoming, OrderRecords.Outgoing] {

  import CustomerBatchProcessFunction._

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[CustomerBatchProcessFunction])

  /** `(windowStartMillis, rawOrderJson)` per buffered order. */
  @transient private var buffer: ListState[FlinkTuple2[java.lang.Long, String]] = _

  override def open(parameters: Configuration): Unit =
    buffer = getRuntimeContext.getListState(
      new ListStateDescriptor[FlinkTuple2[java.lang.Long, String]]("buffered-orders", bufferTypeInformation)
    )

  override def processElement(
      record: OrderRecords.Incoming,
      ctx: KeyedProcessFunction[String, OrderRecords.Incoming, OrderRecords.Outgoing]#Context,
      out: Collector[OrderRecords.Outgoing]
  ): Unit = {
    val windowStart = EventTimeWindows.windowStart(OrderRecords.eventTimeOf(record), windowSizeMillis)
    val windowTimer = windowStart + windowSizeMillis - 1L
    if (windowTimer <= ctx.timerService().currentWatermark()) {
      logger.warn(
        "Dropping late order for customer '{}' at event time {}; its window closed at watermark {}",
        OrderRecords.customerIdOf(record),
        Long.box(OrderRecords.eventTimeOf(record)),
        Long.box(ctx.timerService().currentWatermark())
      )
    } else {
      buffer.add(FlinkTuple2.of(java.lang.Long.valueOf(windowStart), OrderRecords.rawOrderJsonOf(record)))

      // Registering the same timestamp twice is a no-op in Flink, so there is no
      // need to remember whether this window already has a timer.
      ctx.timerService().registerEventTimeTimer(windowTimer)
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[String, OrderRecords.Incoming, OrderRecords.Outgoing]#OnTimerContext,
      out: Collector[OrderRecords.Outgoing]
  ): Unit = {
    val windowStart = timestamp + 1L - windowSizeMillis
    val closed      = BatchWindowing.close(CustomerId(ctx.getCurrentKey), readBuffer(), windowStart, windowSizeMillis)

    closed.batch.foreach { batch =>
      out.collect(OrderRecords.outgoing(BucketPath.forBatch(batch), OrderJson.encodeBatch(batch)))
    }
    writeBuffer(closed.remaining)
  }

  private def readBuffer(): List[BatchWindowing.BufferedOrder] =
    Option(buffer.get())
      .map(_.asScala.toList)
      .getOrElse(Nil)
      .flatMap { entry =>
        OrderJson.decode(entry.f1.getBytes("UTF-8")) match {
          case Right(order)  => Some(BatchWindowing.BufferedOrder(entry.f0.longValue(), order))
          case Left(failure) =>
            logger.warn("Discarding a buffered order that can no longer be parsed: {}", failure.reason)
            None
        }
      }

  private def writeBuffer(remaining: List[BatchWindowing.BufferedOrder]): Unit =
    if (remaining.isEmpty) buffer.clear()
    else
      buffer.update(
        remaining
          .map(entry =>
            FlinkTuple2.of(java.lang.Long.valueOf(entry.windowStartMillis), OrderJson.encodeOrder(entry.order))
          )
          .asJava
      )
}

object CustomerBatchProcessFunction {

  /** Flink cannot infer a type through a Scala type alias, so it is named explicitly. */
  private val bufferTypeInformation: TypeInformation[FlinkTuple2[java.lang.Long, String]] =
    Types.TUPLE[FlinkTuple2[java.lang.Long, String]](Types.LONG, Types.STRING)

}
