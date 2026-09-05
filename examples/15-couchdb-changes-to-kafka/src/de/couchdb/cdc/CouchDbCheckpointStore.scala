package de.couchdb.cdc

/**
 * Keeps the connector's bookmark in a CouchDB `_local` document.
 *
 * Storing progress in the database being followed is the idiomatic CouchDB answer, and it is what CouchDB's own
 * replicator does: the bookmark and the data it describes can never drift apart, and the connector itself stays
 * stateless.
 */
final class CouchDbCheckpointStore(client: CouchDbClient) extends CheckpointStore {

  def load(): StoredCheckpoint =
    client.fetch(StoredCheckpoint.documentId).flatMap(StoredCheckpoint.fromJson).getOrElse(StoredCheckpoint.fresh)

  def save(stored: StoredCheckpoint): StoredCheckpoint = {
    val revision = client.save(StoredCheckpoint.documentId, StoredCheckpoint.toJson(stored))
    stored.copy(revision = Some(revision))
  }
}
