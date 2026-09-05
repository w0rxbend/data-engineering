package de.couchdb.cdc

import scala.collection.mutable

/** An in-memory stand-in for the Kafka producer; a test double may hold mutable state, production code may not. */
private final class RecordingSink extends ChangeSink {
  val records: mutable.Buffer[CatalogueRecord] = mutable.Buffer.empty
  def publish(record: CatalogueRecord): Unit   = records += record
}

/** An in-memory stand-in for the CouchDB `_local` checkpoint document. */
private final class RecordingCheckpointStore(start: StoredCheckpoint = StoredCheckpoint.fresh) extends CheckpointStore {
  val saved: mutable.Buffer[Checkpoint]                = mutable.Buffer.empty
  def load(): StoredCheckpoint                         = start
  def save(stored: StoredCheckpoint): StoredCheckpoint = {
    saved += stored.checkpoint
    stored.copy(revision = Some(Revision(s"${saved.size}-test")))
  }
}

private final class SilentLog extends ConnectorLog {
  val messages: mutable.Buffer[String] = mutable.Buffer.empty
  def note(message: String): Unit      = messages += message
}

class ChangeProcessorSuite extends munit.FunSuite {

  private def replay(checkpointEvery: Int): (RecordingSink, RecordingCheckpointStore, SilentLog, Progress) = {
    val sink        = new RecordingSink
    val checkpoints = new RecordingCheckpointStore
    val log         = new SilentLog
    val processor   = new ChangeProcessor(sink, checkpoints, log, checkpointEvery)
    val progress    = RecordedFeed.payload.linesIterator
      .foldLeft(Progress.startingAt(checkpoints.load()))(processor.handleLine)
    (sink, checkpoints, log, progress)
  }

  test("the recorded feed produces one record per catalogue document and nothing for the others") {
    val (sink, _, _, progress) = replay(checkpointEvery = 5)
    assertEquals(sink.records.map(_.key).toList, List("SKU-COFFEE", "SKU-MUG", "SKU-KETTLE", "SKU-FILTER"))
    assertEquals(progress.published, 4L)
    assertEquals(progress.ignored, 2L)
    assertEquals(progress.unreadable, 0L)
  }

  test("the deleted product is published as a tombstone") {
    val (sink, _, _, _) = replay(checkpointEvery = 5)
    assertEquals(sink.records.find(_.key == "SKU-FILTER").flatMap(_.value), None)
  }

  test("the checkpoint ends on the sequence of the last change handled") {
    val (_, _, _, progress) = replay(checkpointEvery = 5)
    assertEquals(progress.stored.checkpoint.since, SequenceId("6-g1AAAAB5eJzLYWBg"))
  }

  test("the checkpoint is written every n changes and once more when the feed closes") {
    val (_, checkpoints, _, _) = replay(checkpointEvery = 5)
    assertEquals(checkpoints.saved.map(_.changesPublished).toList, List(5L, 6L))
  }

  test("a checkpoint write returns the revision needed by the next write") {
    val (_, _, _, progress) = replay(checkpointEvery = 5)
    assertEquals(progress.stored.revision, Some(Revision("2-test")))
  }

  test("a malformed line is counted and stepped over instead of stopping the connector") {
    val sink      = new RecordingSink
    val log       = new SilentLog
    val processor = new ChangeProcessor(sink, new RecordingCheckpointStore, log, 5)
    val progress  = processor.handleLine(Progress.startingAt(StoredCheckpoint.fresh), "{not json")
    assertEquals(progress.unreadable, 1L)
    assertEquals(sink.records.toList, Nil)
    assert(log.messages.exists(_.contains("unreadable feed line")))
  }

  test("a conflicted document is reported to the operator") {
    val (_, _, log, _) = replay(checkpointEvery = 5)
    assert(log.messages.exists(_.contains("SKU-KETTLE has 2 leaf revisions")))
  }

  test("nothing is written when there is no progress to record") {
    val checkpoints = new RecordingCheckpointStore
    val processor   = new ChangeProcessor(new RecordingSink, checkpoints, new SilentLog, 5)
    processor.flush(Progress.startingAt(StoredCheckpoint.fresh))
    assertEquals(checkpoints.saved.toList, Nil)
  }
}
