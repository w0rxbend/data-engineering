package de.couchdb.cdc

/**
 * How far the connector has got through the `_changes` feed.
 *
 * @param since
 *   the bookmark to hand back to CouchDB on the next connection. Everything up to and including this position has been
 *   handled according to the mapping policy; publishable rows were acknowledged by Apache Kafka first.
 * @param changesHandled
 *   how many feed rows this connector has handled in total, including ignored or deliberately skipped rows. It is only
 *   used to schedule persistence and report progress; the feed resumes from `since`.
 */
final case class Checkpoint(since: SequenceId, changesHandled: Long)

object Checkpoint {

  /** The checkpoint of a connector that has never run: replay the database's whole history. */
  val fresh: Checkpoint = Checkpoint(SequenceId.beginning, 0L)

  /** Moves the checkpoint past a feed row that has been handled according to the mapping policy. */
  def after(current: Checkpoint, row: ChangeRow): Checkpoint =
    Checkpoint(since = row.seq, changesHandled = current.changesHandled + 1L)

  /**
   * Whether the checkpoint should be written back to CouchDB now.
   *
   * Writing after every single change would double the write load for no benefit. Writing every `everyNChanges` changes
   * means a crash replays at most that many changes - which is safe here because delivery is *at-least-once* with
   * idempotent keys: republishing a change produces the same Kafka record for the same document identifier, so a
   * latest-value consumer eventually reaches the same state. A streaming consumer can still observe the replay before
   * compaction and should de-duplicate by the `seq` carried in each value if that matters to it.
   */
  def dueForPersist(checkpoint: Checkpoint, everyNChanges: Int): Boolean =
    everyNChanges > 0 && checkpoint.changesHandled > 0L && checkpoint.changesHandled % everyNChanges == 0L
}

/**
 * A checkpoint as it lives inside CouchDB, together with the revision needed to overwrite it.
 *
 * The document is stored under an identifier starting with `_local/`. A local document is invisible to the `_changes`
 * feed and is never replicated to another node, which is exactly what a bookmark should be - and it is how CouchDB's
 * own replicator records its progress. Storing the bookmark in the database being followed also means the connector
 * carries no state of its own: delete its container and it resumes where it left off.
 */
final case class StoredCheckpoint(checkpoint: Checkpoint, revision: Option[Revision])

object StoredCheckpoint {

  /** The `_id` of the checkpoint document. */
  val documentId: DocId = DocId("_local/catalogue-connector-checkpoint")

  /** A checkpoint that has never been written, so it has no revision to replace. */
  val fresh: StoredCheckpoint = StoredCheckpoint(Checkpoint.fresh, None)

  def toJson(stored: StoredCheckpoint): ujson.Obj = {
    val fields = ujson.Obj(
      "_id"            -> ujson.Str(documentId.value),
      "since"          -> ujson.Str(stored.checkpoint.since.value),
      "changesHandled" -> ujson.Num(stored.checkpoint.changesHandled.toDouble)
    )
    stored.revision.foreach(revision => fields("_rev") = ujson.Str(revision.value))
    fields
  }

  /**
   * Reads the stored document back.
   *
   * This decoder reports malformed data as `None`. The CouchDB adapter distinguishes that from a missing document and
   * stops with repair instructions: silently replaying without the existing `_rev` would make every later save
   * conflict.
   */
  def fromJson(json: ujson.Value): Option[StoredCheckpoint] = json match {
    case obj: ujson.Obj =>
      obj.value.get("since").collect { case ujson.Str(since) =>
        // `changesPublished` is accepted for checkpoints written by the first version of this example, where the name
        // was inaccurate: ignored and unreadable rows were counted too.
        val handled = obj.value
          .get("changesHandled")
          .orElse(obj.value.get("changesPublished"))
          .collect { case ujson.Num(count) => count.toLong }
        val revision = obj.value.get("_rev").collect { case ujson.Str(rev) => Revision(rev) }
        StoredCheckpoint(Checkpoint(SequenceId(since), handled.getOrElse(0L)), revision)
      }
    case _ => None
  }
}
