package de.couchdb.cdc

import scala.util.Try

/** Whether the change describes a document that still exists or one that has been deleted. */
enum DocumentState {
  case Live, Deleted
}

/**
 * One entry of the `_changes` feed.
 *
 * @param seq
 *   the bookmark to store once this change has been published; handing it back as `since` resumes here.
 * @param id
 *   the identifier of the document that changed.
 * @param revisions
 *   the revisions the feed reports for this document. With `style=all_docs` CouchDB lists every leaf of the document's
 *   revision tree, so more than one entry means the document is *conflicted*: two writers changed it independently and
 *   CouchDB kept both branches. The first entry is the "winning" revision CouchDB serves by default.
 * @param state
 *   whether the document is live or was deleted.
 * @param doc
 *   the document body, present only when the feed was requested with `include_docs=true`.
 */
final case class ChangeRow(
    seq: SequenceId,
    id: DocId,
    revisions: List[Revision],
    state: DocumentState,
    doc: Option[ujson.Value]
) {

  /** True when CouchDB is holding more than one leaf revision for this document. */
  def isConflicted: Boolean = revisions.sizeIs > 1

  /** The revision CouchDB serves when the document is fetched without asking for a specific one. */
  def winningRevision: Option[Revision] = revisions.headOption
}

/** Everything a single line of the continuous feed can mean. */
enum FeedEvent {

  /** A blank line. CouchDB sends one every `heartbeat` milliseconds so idle connections are not dropped. */
  case Heartbeat

  /** A document changed. */
  case Change(row: ChangeRow)

  /** CouchDB is closing this response; `last_seq` is the bookmark to resume from. */
  case EndOfFeed(lastSeq: SequenceId)
}

/** Why a line of the feed could not be understood. */
enum FeedProblem {
  case NotJson(line: String, reason: String)
  case NotAnObject(line: String)
  case Unrecognised(line: String)
  case MissingField(field: String)
  case WrongType(field: String, expected: String)

  def message: String = this match {
    case NotJson(line, reason)      => s"line is not valid JSON ($reason): $line"
    case NotAnObject(line)          => s"line is not a JSON object: $line"
    case Unrecognised(line)         => s"line is neither a change nor an end-of-feed marker: $line"
    case MissingField(field)        => s"change has no '$field' field"
    case WrongType(field, expected) => s"field '$field' is not $expected"
  }
}

/**
 * Parsing of CouchDB's `_changes` feed, one line at a time.
 *
 * In *continuous* mode the response never ends on its own: CouchDB keeps the HTTP connection open and writes one
 * newline-terminated JSON object per change, plus a blank line as a heartbeat. That makes the feed a stream of lines,
 * and reading it correctly is a matter of understanding each line in isolation - which is exactly what makes this
 * object pure and testable against a recorded payload.
 */
object ChangesFeed {

  /** Interprets one line of the feed. */
  def parseLine(line: String): Either[FeedProblem, FeedEvent] =
    if (line.trim.isEmpty) Right(FeedEvent.Heartbeat)
    else
      for {
        json  <- parseJson(line)
        obj   <- asObject(json, line)
        event <- interpret(obj, line)
      } yield event

  /**
   * Interprets a whole recorded payload, dropping heartbeats.
   *
   * Anything that cannot be parsed is returned on the left so a caller can log it and keep going; a single malformed
   * line must not stop a connector that is otherwise healthy.
   */
  def parseAll(payload: String): List[Either[FeedProblem, FeedEvent]] =
    payload.linesIterator
      .map(parseLine)
      .filter {
        case Right(FeedEvent.Heartbeat) => false
        case _                          => true
      }
      .toList

  private def interpret(obj: ujson.Obj, line: String): Either[FeedProblem, FeedEvent] =
    if (obj.value.contains("id")) changeOf(obj).map(FeedEvent.Change.apply)
    else if (obj.value.contains("last_seq"))
      stringField(obj, "last_seq").map(seq => FeedEvent.EndOfFeed(SequenceId(seq)))
    else Left(FeedProblem.Unrecognised(line))

  private def changeOf(obj: ujson.Obj): Either[FeedProblem, ChangeRow] =
    for {
      seq       <- sequenceField(obj)
      id        <- stringField(obj, "id")
      revisions <- revisionsOf(obj)
    } yield ChangeRow(
      seq = seq,
      id = DocId(id),
      revisions = revisions,
      state = if (booleanFlag(obj, "deleted")) DocumentState.Deleted else DocumentState.Live,
      doc = obj.value.get("doc").filterNot(_.isNull)
    )

  /**
   * Reads `seq`.
   *
   * CouchDB 1.x reported a number here and CouchDB 2.x and later report a string, so both are accepted and kept as the
   * opaque string this example treats every bookmark as.
   */
  private def sequenceField(obj: ujson.Obj): Either[FeedProblem, SequenceId] =
    obj.value.get("seq").toRight(FeedProblem.MissingField("seq")).flatMap {
      case ujson.Str(value) => Right(SequenceId(value))
      case ujson.Num(value) => Right(SequenceId(BigDecimal(value).toBigInt.toString))
      case _                => Left(FeedProblem.WrongType("seq", "a string or a number"))
    }

  private def revisionsOf(obj: ujson.Obj): Either[FeedProblem, List[Revision]] =
    obj.value.get("changes").toRight(FeedProblem.MissingField("changes")).flatMap {
      case ujson.Arr(entries) =>
        val revisions = entries.toList.collect { case entry: ujson.Obj =>
          entry.value.get("rev").collect { case ujson.Str(rev) => Revision(rev) }
        }.flatten
        if (revisions.isEmpty) Left(FeedProblem.WrongType("changes", "a non-empty array of {\"rev\":...} objects"))
        else Right(revisions)
      case _ => Left(FeedProblem.WrongType("changes", "an array"))
    }

  private def booleanFlag(obj: ujson.Obj, name: String): Boolean =
    obj.value.get(name).contains(ujson.True)

  private def stringField(obj: ujson.Obj, name: String): Either[FeedProblem, String] =
    obj.value.get(name).toRight(FeedProblem.MissingField(name)).flatMap {
      case ujson.Str(value) => Right(value)
      case _                => Left(FeedProblem.WrongType(name, "a string"))
    }

  private def asObject(json: ujson.Value, line: String): Either[FeedProblem, ujson.Obj] = json match {
    case obj: ujson.Obj => Right(obj)
    case _              => Left(FeedProblem.NotAnObject(line))
  }

  private def parseJson(line: String): Either[FeedProblem, ujson.Value] =
    Try(ujson.read(line)).toEither.left.map(error => FeedProblem.NotJson(line, error.getMessage))
}
