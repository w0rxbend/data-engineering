package de.couchdb.cdc

import de.common.domain.{Money, Sku}

/**
 * The vocabulary this example borrows from Apache CouchDB, plus the product catalogue it stores there.
 *
 * CouchDB is a document database: it stores JSON documents, each with an identifier (`_id`) and a revision (`_rev`).
 * Every write produces a new revision, and the revision of the document you are replacing has to be sent with the
 * write. If two clients both start from revision 1 and both write, the second one is rejected - unless the write is
 * replicated in from another node, in which case CouchDB keeps both revisions and marks the document *conflicted*. A
 * conflict is therefore not an error CouchDB hides from you; it is a state your application is expected to resolve.
 */

/** A document identifier, the `_id` field. In this example it is the product's stock keeping unit (SKU). */
final case class DocId(value: String) extends AnyVal

/**
 * A document revision, the `_rev` field.
 *
 * The format is `<generation>-<hash>`, for example `2-8f4c...`. The generation counts how many times the document has
 * been written; the hash identifies which branch of the revision tree this one is.
 */
final case class Revision(value: String) extends AnyVal {

  /** How many times the document has been written, or `None` if the revision is not in the documented format. */
  def generation: Option[Int] = value.split('-').headOption.flatMap(_.toIntOption)
}

/**
 * A position in the `_changes` feed, CouchDB's `seq` value.
 *
 * It is an opaque string, not a number: in a clustered CouchDB it encodes one position per shard. Treat it as a
 * bookmark to hand back, never as something to compare or do arithmetic on.
 */
final case class SequenceId(value: String) extends AnyVal

object SequenceId {

  /** The bookmark meaning "start at the very beginning of the database's history". */
  val beginning: SequenceId = SequenceId("0")
}

/** Whether a catalogue product can currently be ordered. A two-case type rather than a bare `Boolean`. */
enum Availability {
  case InStock, OutOfStock
}

object Availability {

  def fromInStockFlag(inStock: Boolean): Availability = if (inStock) InStock else OutOfStock

  def parse(raw: String): Option[Availability] = Availability.values.find(_.toString.equalsIgnoreCase(raw))
}

/**
 * One entry of the online shop's product catalogue.
 *
 * `Sku` and `Money` come from the repository-wide `de.common.domain` model, so the messages this example publishes line
 * up with the orders the Kafka and Flink examples produce.
 */
final case class CatalogueProduct(
    sku: Sku,
    name: String,
    category: String,
    price: Money,
    availability: Availability
)

/** The small starting catalogue the seed command writes into CouchDB. */
object Catalogue {

  val initial: List[CatalogueProduct] = List(
    CatalogueProduct(
      Sku("SKU-COFFEE"),
      "Single origin coffee beans, 1 kg",
      "beans",
      Money.eur(2450),
      Availability.InStock
    ),
    CatalogueProduct(Sku("SKU-GRINDER"), "Flat burr grinder", "hardware", Money.eur(18900), Availability.InStock),
    CatalogueProduct(Sku("SKU-KETTLE"), "Gooseneck kettle, 1 l", "hardware", Money.eur(7900), Availability.InStock),
    CatalogueProduct(
      Sku("SKU-FILTER"),
      "Paper filters, pack of 100",
      "consumables",
      Money.eur(690),
      Availability.InStock
    ),
    CatalogueProduct(Sku("SKU-MUG"), "Stoneware mug, 300 ml", "accessories", Money.eur(1490), Availability.OutOfStock)
  )
}
