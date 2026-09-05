package de.couchdb.cdc

import de.common.domain.{Money, Sku}
import de.common.json.Codecs

/** Why a JSON document could not be read as a catalogue product. */
enum DocumentProblem {
  case NotAnObject
  case MissingField(field: String)
  case WrongType(field: String, expected: String)
  case UnknownAvailability(raw: String)

  def message: String = this match {
    case NotAnObject                => "document is not a JSON object"
    case MissingField(field)        => s"document has no '$field' field"
    case WrongType(field, expected) => s"field '$field' is not $expected"
    case UnknownAvailability(raw)   => s"'$raw' is not a known availability"
  }
}

/**
 * Translates between a `CatalogueProduct` and the JSON document Apache CouchDB stores.
 *
 * Both directions are pure functions over `ujson.Value`, so the tests can feed in a document recorded from a real
 * CouchDB response without any server running.
 */
object CatalogueDocument {

  /** Marks the documents this connector cares about, so the checkpoint document and design documents can be skipped. */
  val documentType: String = "product"

  /**
   * Renders a product as the document body CouchDB will store.
   *
   * The `_id` is the SKU, which makes the write idempotent: seeding twice updates the same five documents instead of
   * creating ten. The monetary amount is rendered by the shared `de.common.json.Codecs`, so a price on this Kafka topic
   * is byte-for-byte the same shape as a price inside an order elsewhere in the repository.
   */
  def toJson(product: CatalogueProduct): ujson.Obj =
    ujson.Obj(
      "_id"          -> ujson.Str(product.sku.value),
      "type"         -> ujson.Str(documentType),
      "name"         -> ujson.Str(product.name),
      "category"     -> ujson.Str(product.category),
      "price"        -> ujson.read(Codecs.money(product.price)),
      "availability" -> ujson.Str(product.availability.toString)
    )

  /** Reads a stored document back into the domain model. */
  def fromJson(json: ujson.Value): Either[DocumentProblem, CatalogueProduct] =
    for {
      obj          <- objectOf(json)
      id           <- stringField(obj, "_id")
      name         <- stringField(obj, "name")
      category     <- stringField(obj, "category")
      price        <- money(obj)
      rawAvailable <- stringField(obj, "availability")
      availability <- Availability.parse(rawAvailable).toRight(DocumentProblem.UnknownAvailability(rawAvailable))
    } yield CatalogueProduct(Sku(id), name, category, price, availability)

  private def money(obj: ujson.Obj): Either[DocumentProblem, Money] =
    for {
      priceValue <- field(obj, "price")
      priceObj   <- objectOf(priceValue).left.map(_ => DocumentProblem.WrongType("price", "an object"))
      cents      <- longField(priceObj, "cents")
      currency   <- stringField(priceObj, "currency")
    } yield Money(cents, currency)

  private def objectOf(json: ujson.Value): Either[DocumentProblem, ujson.Obj] = json match {
    case obj: ujson.Obj => Right(obj)
    case _              => Left(DocumentProblem.NotAnObject)
  }

  private def field(obj: ujson.Obj, name: String): Either[DocumentProblem, ujson.Value] =
    obj.value.get(name).toRight(DocumentProblem.MissingField(name))

  private def stringField(obj: ujson.Obj, name: String): Either[DocumentProblem, String] =
    field(obj, name).flatMap {
      case ujson.Str(value) => Right(value)
      case _                => Left(DocumentProblem.WrongType(name, "a string"))
    }

  private def longField(obj: ujson.Obj, name: String): Either[DocumentProblem, Long] =
    field(obj, name).flatMap {
      case ujson.Num(value) => Right(value.toLong)
      case _                => Left(DocumentProblem.WrongType(name, "a number"))
    }
}
