package de.couchdb.cdc

import ox.{useCloseableInScope, Ox}
import sttp.client4.*
import sttp.model.{StatusCode, Uri}

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.Duration
import scala.util.Try

/** An HTTP call to CouchDB that did not do what the connector needs. */
final class CouchDbFailure(message: String) extends RuntimeException(message)

/**
 * The input/output shell around Apache CouchDB's HTTP interface.
 *
 * CouchDB has no binary protocol and no driver: everything is plain HTTP and JSON, which is why an HTTP client is the
 * only dependency needed. sttp is used in *direct style* here - `send` blocks and returns the response, with no
 * `Future` or `IO` wrapper - which keeps this file readable as a list of requests.
 *
 * Create it with `CouchDbClient.open`, which registers the backend with the enclosing Ox scope so its connection pool
 * is closed when the scope ends.
 */
final class CouchDbClient private (settings: Settings, backend: SyncBackend) extends CheckpointDocumentClient {

  /** Creates the database, treating "it already exists" as success so the command can be re-run. */
  def createDatabaseIfAbsent(): Unit = {
    val response     = authenticated(basicRequest.put(databaseUri())).response(asStringAlways).send(backend)
    val alreadyThere = response.code == StatusCode.PreconditionFailed
    if (!response.code.isSuccess && !alreadyThere)
      throw new CouchDbFailure(s"could not create database ${settings.database.value}: ${response.body}")
  }

  /** Fetches a document, or `None` when CouchDB answers 404. */
  def fetch(id: DocId): Option[ujson.Value] = {
    val response = authenticated(basicRequest.get(databaseUri(id))).response(asStringAlways).send(backend)
    if (response.code == StatusCode.NotFound) None
    else if (response.code.isSuccess) Try(ujson.read(response.body)).toOption
    else throw new CouchDbFailure(s"could not read ${id.value}: ${response.body}")
  }

  /**
   * Writes a document and returns its new revision.
   *
   * CouchDB rejects an update that does not carry the revision being replaced, which is how it detects that two writers
   * raced. This method therefore reads the current revision first and copies it into the body.
   */
  def save(id: DocId, body: ujson.Obj): Revision = {
    withCurrentRevision(id, body)
    put(id, body)
  }

  /**
   * Writes exactly the revision carried by `body`, without first replacing it with the latest revision from CouchDB.
   *
   * Checkpoints use this operation so a second connector instance cannot silently move a bookmark backwards. If its
   * loaded revision is stale, CouchDB answers 409 and the losing instance stops for an operator to resolve.
   */
  def saveAtExpectedRevision(id: DocId, body: ujson.Obj): Revision = put(id, body)

  private def put(id: DocId, body: ujson.Obj): Revision = {
    val response = authenticated(basicRequest.put(databaseUri(id)))
      .body(ujson.write(body))
      .contentType("application/json")
      .response(asStringAlways)
      .send(backend)
    if (!response.code.isSuccess)
      throw new CouchDbFailure(s"could not write ${id.value}: HTTP ${response.code}: ${response.body}")
    revisionOf(response.body).getOrElse(throw new CouchDbFailure(s"write of ${id.value} returned no revision"))
  }

  /** Deletes a document. CouchDB keeps a stub with `_deleted: true`, which is the tombstone the feed reports. */
  def delete(id: DocId): Unit = {
    val revision = fetch(id).flatMap(revisionOf).getOrElse(throw new CouchDbFailure(s"${id.value} does not exist"))
    val response = authenticated(basicRequest.delete(databaseUri(id).addParam("rev", revision.value)))
      .response(asStringAlways)
      .send(backend)
    if (!response.code.isSuccess) throw new CouchDbFailure(s"could not delete ${id.value}: ${response.body}")
  }

  /** Runs a Mango query against `_find`. */
  def find(query: ujson.Obj): ujson.Value = postJson(databaseUri(DocId("_find")), query)

  /** Reads a view of a design document, grouped so the built-in `_count` reduce returns one row per key. */
  def groupedView(design: DocId, view: String): ujson.Value = {
    val uri      = databaseUri(design).addPath("_view", view).addParam("group", "true")
    val response = authenticated(basicRequest.get(uri)).response(asStringAlways).send(backend)
    if (!response.code.isSuccess) throw new CouchDbFailure(s"could not read view $view: ${response.body}")
    ujson.read(response.body)
  }

  /**
   * Opens one continuous `_changes` response and hands every line to `onLine` until CouchDB closes it.
   *
   * Why the response ends at all: `timeout` tells CouchDB to close an idle feed after that many milliseconds. The
   * request deliberately omits `heartbeat`, because CouchDB documents that heartbeat overrides timeout and holds the
   * response open indefinitely. On Java 21 the socket read is interruptible; the timeout also provides a regular
   * reconnect and checkpoint boundary.
   *
   * `style=all_docs` asks CouchDB to list every leaf revision, which is what makes a conflicted document visible;
   * `include_docs=true` ships the document body with the change so no second request per change is needed.
   */
  def readChanges(since: SequenceId)(onLine: String => Unit): Unit = {
    val uri = databaseUri(DocId("_changes"))
      .addParam("feed", "continuous")
      .addParam("since", since.value)
      .addParam("include_docs", "true")
      .addParam("style", "all_docs")
      .addParam("timeout", settings.feedTimeoutMillis.toString)

    val response = authenticated(basicRequest.get(uri))
      .readTimeout(Duration.Inf)
      .response(asInputStreamAlways { stream =>
        val reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
        Iterator.continually(reader.readLine()).takeWhile(_ != null).foreach(onLine)
      })
      .send(backend)
    if (!response.code.isSuccess) {
      throw new CouchDbFailure(s"changes feed failed with HTTP ${response.code}")
    }
  }

  private def postJson(uri: Uri, body: ujson.Obj): ujson.Value = {
    val response = authenticated(basicRequest.post(uri))
      .body(ujson.write(body))
      .contentType("application/json")
      .response(asStringAlways)
      .send(backend)
    if (!response.code.isSuccess) throw new CouchDbFailure(s"request to $uri failed: ${response.body}")
    ujson.read(response.body)
  }

  /** Copies the revision of the stored document into the body, so the write replaces it instead of being rejected. */
  private def withCurrentRevision(id: DocId, body: ujson.Obj): Unit =
    fetch(id).flatMap(revisionOf).foreach(revision => body("_rev") = ujson.Str(revision.value))

  private def revisionOf(json: ujson.Value): Option[Revision] = json match {
    case obj: ujson.Obj =>
      obj.value.get("_rev").orElse(obj.value.get("rev")).collect { case ujson.Str(value) => Revision(value) }
    case _ => None
  }

  private def revisionOf(rawBody: String): Option[Revision] = Try(ujson.read(rawBody)).toOption.flatMap(revisionOf)

  private def authenticated(request: Request[Either[String, String]]): Request[Either[String, String]] =
    request.auth.basic(settings.credentials.user, settings.credentials.password)

  /** `<url>/<database>` plus the segments of a document identifier, so `_local/x` keeps its slash. */
  private def databaseUri(id: DocId*): Uri =
    uri"${settings.couchDbUrl.value}".addPath(settings.database.value +: id.toSeq.flatMap(_.value.split('/')))
}

object CouchDbClient {

  /** Opens a client whose HTTP connection pool is closed when the enclosing Ox scope ends. */
  def open(settings: Settings)(using Ox): CouchDbClient =
    new CouchDbClient(settings, useCloseableInScope(DefaultSyncBackend()))
}
