package de.couchdb.cdc

/** The two CouchDB document operations needed by the checkpoint adapter. */
trait CheckpointDocumentClient {
  def fetch(id: DocId): Option[ujson.Value]
  def saveAtExpectedRevision(id: DocId, body: ujson.Obj): Revision
}

/**
 * Keeps the connector's bookmark in a CouchDB `_local` document.
 *
 * Storing progress in the database being followed is the idiomatic CouchDB answer, and it is what CouchDB's own
 * replicator does. The connector itself stays stateless, while CouchDB's revision check prevents two connector
 * processes from silently overwriting one another. This document is not transactional with Kafka; publish-before-save
 * intentionally leaves an at-least-once replay window.
 */
final class CouchDbCheckpointStore(client: CheckpointDocumentClient) extends CheckpointStore {

  def load(): StoredCheckpoint =
    client.fetch(StoredCheckpoint.documentId) match {
      case None       => StoredCheckpoint.fresh
      case Some(json) =>
        StoredCheckpoint.fromJson(json).filter(_.revision.nonEmpty).getOrElse {
          throw new CouchDbFailure(
            s"checkpoint document ${StoredCheckpoint.documentId.value} is malformed; " +
              "repair it with a string 'since' and its current '_rev', or delete it to replay from the beginning"
          )
        }
    }

  def save(stored: StoredCheckpoint): StoredCheckpoint = {
    val revision = client.saveAtExpectedRevision(StoredCheckpoint.documentId, StoredCheckpoint.toJson(stored))
    stored.copy(revision = Some(revision))
  }
}
