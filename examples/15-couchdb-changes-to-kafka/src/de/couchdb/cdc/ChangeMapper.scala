package de.couchdb.cdc

import de.common.json.Codecs

/**
 * A message ready to be handed to the Apache Kafka producer.
 *
 * @param key
 *   the document identifier. Using it as the Kafka key is what makes redelivery harmless: the same document always
 *   lands on the same partition and overwrites its own previous version.
 * @param value
 *   the rendered JSON, or `None` for a *tombstone*. A tombstone is a record with a null value; on a topic configured
 *   with `cleanup.policy=compact` it tells Kafka - and every consumer - that the key no longer exists, and log
 *   compaction eventually removes the key entirely.
 */
final case class CatalogueRecord(key: String, value: Option[String])

/** What the connector decided to do with one change. */
enum ChangeOutcome {
  case Publish(record: CatalogueRecord)
  case Ignore(reason: IgnoreReason)
}

/** Why a change was not published. */
enum IgnoreReason {

  /** A CouchDB internal document, such as a `_design/...` document holding views. */
  case InternalDocument

  /** A document that lives in the same database but is not part of the catalogue. */
  case NotAProduct

  def message: String = this match {
    case InternalDocument => "CouchDB internal document"
    case NotAProduct      => "not a catalogue product"
  }
}

/** Why a change could not be turned into a record. */
enum MappingProblem {
  case DocumentNotIncluded(id: DocId)
  case Undecodable(id: DocId, problem: DocumentProblem)

  def message: String = this match {
    case DocumentNotIncluded(id)  => s"change for ${id.value} carries no document; request the feed with include_docs"
    case Undecodable(id, problem) => s"document ${id.value} could not be read: ${problem.message}"
  }
}

/**
 * Turns one entry of the `_changes` feed into at most one Kafka record.
 *
 * This is the whole business rule of the connector, and it is a pure function: no HTTP client, no producer, nothing to
 * start. That is what lets the tests below drive it with a recorded feed payload.
 */
object ChangeMapper {

  def map(row: ChangeRow): Either[MappingProblem, ChangeOutcome] =
    if (isInternal(row.id)) Right(ChangeOutcome.Ignore(IgnoreReason.InternalDocument))
    else
      row.state match {
        case DocumentState.Deleted => Right(ChangeOutcome.Publish(tombstone(row)))
        case DocumentState.Live    => liveRecord(row)
      }

  /**
   * A deleted document arrives as a change with `"deleted": true` and a stub body - the fields are gone, only the
   * identifier and the new revision remain. That is CouchDB's own tombstone, and it maps one-to-one onto Kafka's.
   */
  private def tombstone(row: ChangeRow): CatalogueRecord = CatalogueRecord(row.id.value, None)

  private def liveRecord(row: ChangeRow): Either[MappingProblem, ChangeOutcome] =
    row.doc match {
      case None                         => Left(MappingProblem.DocumentNotIncluded(row.id))
      case Some(doc) if !isProduct(doc) => Right(ChangeOutcome.Ignore(IgnoreReason.NotAProduct))
      case Some(doc)                    =>
        CatalogueDocument
          .fromJson(doc)
          .left
          .map(problem => MappingProblem.Undecodable(row.id, problem))
          .map(product => ChangeOutcome.Publish(CatalogueRecord(row.id.value, Some(render(product, row)))))
    }

  /**
   * The published message.
   *
   * The CouchDB revision and sequence travel with the payload so a downstream consumer can tell which version of the
   * document it is looking at, and `conflicted` is carried through rather than hidden: a consumer that cares can react
   * to a document CouchDB is holding two branches of.
   */
  private def render(product: CatalogueProduct, row: ChangeRow): String =
    ujson.write(
      ujson.Obj(
        "sku"          -> ujson.Str(product.sku.value),
        "name"         -> ujson.Str(product.name),
        "category"     -> ujson.Str(product.category),
        "price"        -> ujson.read(Codecs.money(product.price)),
        "availability" -> ujson.Str(product.availability.toString),
        "rev"          -> ujson.Str(row.winningRevision.map(_.value).getOrElse("")),
        "seq"          -> ujson.Str(row.seq.value),
        "conflicted"   -> ujson.Bool(row.isConflicted)
      )
    )

  private def isInternal(id: DocId): Boolean = id.value.startsWith("_")

  private def isProduct(doc: ujson.Value): Boolean = doc match {
    case obj: ujson.Obj => obj.value.get("type").contains(ujson.Str(CatalogueDocument.documentType))
    case _              => false
  }
}
