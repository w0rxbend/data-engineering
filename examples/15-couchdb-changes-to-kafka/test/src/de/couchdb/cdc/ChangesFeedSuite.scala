package de.couchdb.cdc

class ChangesFeedSuite extends munit.FunSuite {

  test("a blank line is a heartbeat, not a change") {
    assertEquals(ChangesFeed.parseLine("  "), Right(FeedEvent.Heartbeat))
  }

  test("a change line yields the sequence, the identifier and the winning revision") {
    val line = """{"seq":"3-abc","id":"SKU-MUG","changes":[{"rev":"2-77b0"}]}"""
    val row  = changeOf(line)
    assertEquals(row.seq, SequenceId("3-abc"))
    assertEquals(row.id, DocId("SKU-MUG"))
    assertEquals(row.winningRevision, Some(Revision("2-77b0")))
    assertEquals(row.state, DocumentState.Live)
  }

  test("a deleted document is reported as a deletion") {
    val line = """{"seq":"5-abc","id":"SKU-FILTER","changes":[{"rev":"2-0f5d"}],"deleted":true}"""
    assertEquals(changeOf(line).state, DocumentState.Deleted)
  }

  test("more than one leaf revision means the document is conflicted") {
    val line = """{"seq":"4-abc","id":"SKU-KETTLE","changes":[{"rev":"3-11aa"},{"rev":"3-bb22"}]}"""
    assert(changeOf(line).isConflicted)
  }

  test("a numeric sequence, as CouchDB 1.x reported it, is kept as its string form") {
    val line = """{"seq":42,"id":"SKU-MUG","changes":[{"rev":"1-aa"}]}"""
    assertEquals(changeOf(line).seq, SequenceId("42"))
  }

  test("the closing line carries the sequence to resume from") {
    assertEquals(
      ChangesFeed.parseLine("""{"last_seq":"6-abc","pending":0}"""),
      Right(FeedEvent.EndOfFeed(SequenceId("6-abc")))
    )
  }

  test("a line that is not JSON is reported rather than thrown") {
    assert(ChangesFeed.parseLine("{oops").left.exists(_.isInstanceOf[FeedProblem.NotJson]))
  }

  test("a change without a changes array is rejected") {
    assertEquals(ChangesFeed.parseLine("""{"seq":"1-a","id":"x"}"""), Left(FeedProblem.MissingField("changes")))
  }

  test("the recorded feed payload parses to six changes and one end marker") {
    val events = ChangesFeed.parseAll(RecordedFeed.payload)
    assert(events.forall(_.isRight), events.collect { case Left(problem) => problem.message }.mkString(", "))
    assertEquals(events.count { case Right(FeedEvent.Change(_)) => true; case _ => false }, 6)
    assertEquals(events.lastOption, Some(Right(FeedEvent.EndOfFeed(SequenceId("6-g1AAAAB5eJzLYWBg")))))
  }

  private def changeOf(line: String): ChangeRow = ChangesFeed.parseLine(line) match {
    case Right(FeedEvent.Change(row)) => row
    case other                        => fail(s"expected a change, got $other")
  }
}
