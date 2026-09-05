package de.couchdb.cdc

import scala.io.Source
import scala.util.Using

/**
 * A `_changes` response recorded from a real CouchDB run, kept as a test resource.
 *
 * Recording the payload once means every parsing and mapping rule can be checked against the bytes CouchDB actually
 * sends, with no server involved: `./mill __.test` passes with Docker stopped.
 */
object RecordedFeed {

  val payload: String =
    Using.resource(Source.fromResource("changes-feed.txt"))(_.mkString)
}
