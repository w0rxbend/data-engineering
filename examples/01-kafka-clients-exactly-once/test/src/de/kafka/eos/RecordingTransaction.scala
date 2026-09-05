package de.kafka.eos

import de.common.domain.Payment

/** One thing that happened to the transaction, in the order it happened. */
enum TransactionStep {
  case Began
  case Emitted(payment: Payment)
  case Committed(consumed: List[SourceOffset])
  case Aborted
  case RewoundTo(consumed: List[SourceOffset])
}

/**
 * A stand-in for a Kafka transaction that only writes down what it was asked to do.
 *
 * Recording the calls, rather than checking them one at a time, lets a test assert on the whole story - "began, emitted
 * two, then aborted" - which is what the exactly-once guarantee is actually about.
 */
final class RecordingTransaction extends SettlementTransaction {

  // A mutable buffer is acceptable here: this class exists only to stand in for
  // an external system inside a single-threaded test.
  private val recorded = scala.collection.mutable.ListBuffer.empty[TransactionStep]

  def begin(): Unit = record(TransactionStep.Began)

  def emit(payment: Payment): Unit = record(TransactionStep.Emitted(payment))

  def commit(consumed: List[SourceOffset]): Unit = record(TransactionStep.Committed(consumed))

  def abort(): Unit = record(TransactionStep.Aborted)

  def rewindTo(consumed: List[SourceOffset]): Unit = record(TransactionStep.RewoundTo(consumed))

  /** Everything that happened, oldest first. */
  def steps: List[TransactionStep] = recorded.toList

  private def record(step: TransactionStep): Unit = {
    recorded += step
    ()
  }
}
