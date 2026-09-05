package de.kafka.eos

import de.common.domain.Payment

/** Where a record came from: its topic, partition and position in that partition. */
final case class SourceOffset(topic: String, partition: Int, offset: Long)

/** One record as it was read from the `orders` topic, before it is understood. */
final case class ConsumedRecord(source: SourceOffset, payload: String)

/** Why a single record could not be settled. */
enum SettlementFailure {

  /** The bytes on the topic are not a well-formed order. */
  case Unreadable(source: SourceOffset, detail: String)

  /** The order was understood, but the business rule refused to charge it. */
  case Refused(source: SourceOffset, rejection: SettlementRejection)

  def describe: String = this match {
    case Unreadable(source, detail) => s"record at ${render(source)} $detail"
    case Refused(source, rejection) => s"record at ${render(source)}: ${rejection.describe}"
  }

  private def render(source: SourceOffset): String =
    s"${source.topic}-${source.partition}@${source.offset}"
}

/** What happened to one polled batch. */
enum BatchOutcome {

  /** The poll returned nothing, so no transaction was opened at all. */
  case Empty

  /** Every record settled; payments and input offsets were committed together. */
  case Committed(payments: List[Payment])

  /** At least one record failed; the whole transaction was rolled back. */
  case Aborted(failures: List[SettlementFailure])
}

/**
 * The side-effecting half of the loop, expressed as five operations.
 *
 * Keeping it behind a trait is what lets `TransactionalSettlement.settleBatch` be unit-tested without a broker: the
 * test supplies a recording double, and production supplies `KafkaSettlementTransaction`.
 */
trait SettlementTransaction {

  /** Opens a new Kafka transaction. Nothing sent after this is visible yet. */
  def begin(): Unit

  /** Queues one payment record inside the open transaction. */
  def emit(payment: Payment): Unit

  /**
   * Closes the transaction, making both the emitted payments *and* the progress through the input topic visible in the
   * same atomic step.
   *
   * @param consumed
   *   the input records covered by this transaction; their offsets are committed as part of the transaction rather than
   *   separately, which is the detail that removes the window in which a crash could cause a double charge
   */
  def commit(consumed: List[SourceOffset]): Unit

  /** Throws the transaction away. Nothing it emitted ever becomes visible. */
  def abort(): Unit

  /**
   * Puts the reader back to the start of the given records.
   *
   * This is the half of "abort" that is easy to forget. Aborting discards the *output*, but the consumer has already
   * moved its own in-memory position past the records it handed over, so without an explicit rewind those inputs would
   * be skipped for the rest of the process's life - the pipeline would quietly lose them rather than double-count them.
   *
   * @param consumed
   *   the records to read again; the reader moves back to the lowest offset seen per topic-partition
   */
  def rewindTo(consumed: List[SourceOffset]): Unit
}

/**
 * The exactly-once loop body: one polled batch in, one atomic outcome out.
 *
 * This is where "exactly once" actually lives. Payments are emitted while the transaction is open, and the offsets of
 * the records that produced them are committed *by the same transaction*. Either both land or neither does, so a crash
 * halfway through a batch replays that batch from the start and charges each customer exactly once.
 */
object TransactionalSettlement {

  /**
   * @param batch
   *   the records returned by a single poll
   * @param now
   *   supplies the timestamp stamped on each payment; passed in so that tests are deterministic
   * @param transaction
   *   the Kafka transaction to run the batch inside
   */
  def settleBatch(
      batch: List[ConsumedRecord],
      now: () => Long,
      transaction: SettlementTransaction
  ): BatchOutcome =
    if (batch.isEmpty) {
      BatchOutcome.Empty
    } else {
      transaction.begin()
      emitUntilFailure(batch, now, transaction)
    }

  /**
   * Emits payments one by one and stops at the first failure.
   *
   * Failing *after* some payments were already emitted is the interesting case, and the one this example wants to show:
   * those payments are discarded by `abort`, and a consumer reading with `isolation.level=read_committed` never sees
   * them.
   */
  private def emitUntilFailure(
      batch: List[ConsumedRecord],
      now: () => Long,
      transaction: SettlementTransaction
  ): BatchOutcome = {
    var emitted: List[Payment]            = Nil
    var failures: List[SettlementFailure] = Nil
    val records                           = batch.iterator

    while (records.hasNext && failures.isEmpty)
      settleRecord(records.next(), now) match {
        case Right(payment) =>
          transaction.emit(payment)
          emitted = payment :: emitted
        case Left(failure) =>
          failures = failure :: failures
      }

    if (failures.isEmpty) {
      transaction.commit(batch.map(_.source))
      BatchOutcome.Committed(emitted.reverse)
    } else {
      transaction.abort()
      transaction.rewindTo(batch.map(_.source))
      BatchOutcome.Aborted(failures.reverse)
    }
  }

  /** Decodes one record and applies the business rule to it. */
  private def settleRecord(record: ConsumedRecord, now: () => Long): Either[SettlementFailure, Payment] =
    OrderJson
      .decode(record.payload)
      .left
      .map(detail => SettlementFailure.Unreadable(record.source, detail))
      .flatMap { order =>
        Settlement
          .settle(order, now())
          .left
          .map(rejection => SettlementFailure.Refused(record.source, rejection))
      }
}
