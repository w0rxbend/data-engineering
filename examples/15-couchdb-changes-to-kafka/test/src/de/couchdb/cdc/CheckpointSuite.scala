package de.couchdb.cdc

class CheckpointSuite extends munit.FunSuite {

  private def rowAt(seq: String): ChangeRow =
    ChangeRow(SequenceId(seq), DocId("SKU-MUG"), List(Revision("1-aa")), DocumentState.Live, None)

  test("advancing moves the bookmark to the change just handled and counts it") {
    val advanced = Checkpoint.after(Checkpoint.fresh, rowAt("7-abc"))
    assertEquals(advanced, Checkpoint(SequenceId("7-abc"), 1L))
  }

  test("a checkpoint is written every n changes, not on every change") {
    val every = 5
    assertEquals(Checkpoint.dueForPersist(Checkpoint(SequenceId("4-abc"), 4L), every), false)
    assertEquals(Checkpoint.dueForPersist(Checkpoint(SequenceId("5-abc"), 5L), every), true)
  }

  test("a connector that has published nothing has nothing to write") {
    assertEquals(Checkpoint.dueForPersist(Checkpoint.fresh, 5), false)
  }

  test("the stored document keeps the revision so the next write replaces it") {
    val stored = StoredCheckpoint(Checkpoint(SequenceId("9-abc"), 12L), Some(Revision("4-cc")))
    val json   = StoredCheckpoint.toJson(stored)
    assertEquals(json("_id").str, "_local/catalogue-connector-checkpoint")
    assertEquals(json("_rev").str, "4-cc")
    assertEquals(StoredCheckpoint.fromJson(json), Some(stored))
  }

  test("a first write carries no revision, because there is no document to replace") {
    assert(!StoredCheckpoint.toJson(StoredCheckpoint.fresh).value.contains("_rev"))
  }

  test("an unreadable checkpoint document is simply absent, so the connector replays from the beginning") {
    assertEquals(StoredCheckpoint.fromJson(ujson.Obj("changesPublished" -> ujson.Num(3))), None)
  }
}
