package de.couchdb.cdc

/**
 * How far the connector has got through the `_changes` feed.
 *
 * @param since
 *   the bookmark to hand back to CouchDB on the next connection. Everything up to and including this position has been
 *   published to Apache Kafka.
 * @param changesPublished
 *   how many changes this connector has published in total. Only reported to the operator; the feed does not use it.
 */
final case class Checkpoint(since: SequenceId, changesPublished: Long)

object Checkpoint {

  /** The checkpoint of a connector that has never run: replay the database's whole history. */
  val fresh: Checkpoint = Checkpoint(SequenceId.beginning, 0L)

  /** Moves the checkpoint to the position of a change that has just been published. */
  def after(current: Checkpoint, row: ChangeRow): Checkpoint =
    Checkpoint(since = row.seq, changesPublished = current.changesPublished + 1L)

  /**
   * Whether the checkpoint should be written back to CouchDB now.
   *
   * Writing after every single change would double the write load for no benefit. Writing every `everyNChanges` changes
   * means a crash replays at most that many changes - which is safe here because delivery is *at-least-once* with
   * idempotent keys: republishing a change produces the same Kafka record for the same document identifier, so a
   * consumer of the compacted topic cannot tell the difference.
   */
  def dueForPersist(checkpoint: Checkpoint, everyNChanges: Int): Boolean =
    everyNChanges > 0 && checkpoint.changesPublished > 0L && checkpoint.changesPublished % everyNChanges == 0L
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
      "_id"              -> ujson.Str(documentId.value),
      "since"            -> ujson.Str(stored.checkpoint.since.value),
      "changesPublished" -> ujson.Num(stored.checkpoint.changesPublished.toDouble)
    )
    stored.revision.foreach(revision => fields("_rev") = ujson.Str(revision.value))
    fields
  }

  /**
   * Reads the stored document back.
   *
   * A checkpoint that cannot be read is not an error worth stopping for: the connector falls back to replaying from the
   * beginning, which is correct, only slower.
   */
  def fromJson(json: ujson.Value): Option[StoredCheckpoint] = json match {
    case obj: ujson.Obj =>
      obj.value.get("since").collect { case ujson.Str(since) =>
        val published = obj.value.get("changesPublished").collect { case ujson.Num(count) => count.toLong }
        val revision  = obj.value.get("_rev").collect { case ujson.Str(rev) => Revision(rev) }
        StoredCheckpoint(Checkpoint(SequenceId(since), published.getOrElse(0L)), revision)
      }
    case _ => None
  }
}
