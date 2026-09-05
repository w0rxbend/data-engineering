package de.couchdb.cdc

/** Where mapped changes are published. Implemented by the Kafka producer, and by an in-memory double in the tests. */
trait ChangeSink {
  def publish(record: CatalogueRecord): Unit
}

/** Where the bookmark is kept. Implemented on top of a CouchDB `_local` document. */
trait CheckpointStore {

  def load(): StoredCheckpoint

  /** Writes the checkpoint and returns it with the revision the write produced, ready for the next write. */
  def save(stored: StoredCheckpoint): StoredCheckpoint
}

/** Everything the connector wants to tell the operator. Kept behind a trait so tests can assert on it. */
trait ConnectorLog {
  def note(message: String): Unit
}

/** How far the connector has got, and what it has seen along the way. */
final case class Progress(
    stored: StoredCheckpoint,
    published: Long,
    ignored: Long,
    unreadable: Long,
    sinceLastPersist: Int
) {

  def summary: String =
    s"published=$published ignored=$ignored unreadable=$unreadable since=${stored.checkpoint.since.value}"
}

object Progress {
  def startingAt(stored: StoredCheckpoint): Progress = Progress(stored, 0L, 0L, 0L, 0)
}

/**
 * The connector's decision loop, with every side effect behind a trait.
 *
 * One line of the `_changes` feed goes in, an updated `Progress` comes out, and along the way at most one record is
 * published and at most one checkpoint written. Because the three collaborators are traits, the whole loop - including
 * the "publish first, checkpoint afterwards" order that makes delivery at-least-once - is exercised by unit tests with
 * no CouchDB and no Kafka running.
 */
final class ChangeProcessor(
    sink: ChangeSink,
    checkpoints: CheckpointStore,
    log: ConnectorLog,
    checkpointEveryNChanges: Int
) {

  def handleLine(progress: Progress, line: String): Progress =
    ChangesFeed.parseLine(line) match {
      case Left(problem) =>
        log.note(s"skipping unreadable feed line: ${problem.message}")
        progress.copy(unreadable = progress.unreadable + 1L)
      case Right(FeedEvent.Heartbeat)     => progress
      case Right(FeedEvent.Change(row))   => handleChange(progress, row)
      case Right(FeedEvent.EndOfFeed(at)) =>
        log.note(s"CouchDB closed the feed at ${at.value}; ${progress.summary}")
        flush(progress)
    }

  /** Writes the checkpoint even if it is not yet due. Called when a feed connection ends and on shutdown. */
  def flush(progress: Progress): Progress =
    if (progress.sinceLastPersist == 0) progress else persist(progress)

  private def handleChange(progress: Progress, row: ChangeRow): Progress = {
    if (row.isConflicted)
      log.note(s"${row.id.value} has ${row.revisions.size} leaf revisions; publishing the winning one")

    val counted = ChangeMapper.map(row) match {
      case Left(problem) =>
        // An unmappable document must not block the feed: it is reported, counted, and stepped over.
        log.note(s"skipping change: ${problem.message}")
        progress.copy(unreadable = progress.unreadable + 1L)
      case Right(ChangeOutcome.Ignore(reason)) =>
        log.note(s"ignoring ${row.id.value}: ${reason.message}")
        progress.copy(ignored = progress.ignored + 1L)
      case Right(ChangeOutcome.Publish(record)) =>
        sink.publish(record)
        log.note(publishedMessage(record))
        progress.copy(published = progress.published + 1L)
    }

    checkpointAfter(counted, row)
  }

  /**
   * Moves the bookmark past a change that has already been handled, and writes it back every so often.
   *
   * The order matters: the record is published *before* the checkpoint moves. A crash in between replays the change.
   * Stable keys make the compacted latest-value view converge, but raw consumers can observe both records and may
   * de-duplicate by sequence. The other order would lose the change.
   */
  private def checkpointAfter(progress: Progress, row: ChangeRow): Progress = {
    val advanced = progress.copy(
      stored = progress.stored.copy(checkpoint = Checkpoint.after(progress.stored.checkpoint, row)),
      sinceLastPersist = progress.sinceLastPersist + 1
    )
    if (Checkpoint.dueForPersist(advanced.stored.checkpoint, checkpointEveryNChanges)) persist(advanced) else advanced
  }

  private def persist(progress: Progress): Progress =
    progress.copy(stored = checkpoints.save(progress.stored), sinceLastPersist = 0)

  private def publishedMessage(record: CatalogueRecord): String =
    record.value match {
      case None    => s"published tombstone for ${record.key}"
      case Some(_) => s"published ${record.key}"
    }
}
