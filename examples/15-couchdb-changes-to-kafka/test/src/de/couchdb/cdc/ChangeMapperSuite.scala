package de.couchdb.cdc

import de.common.domain.{Money, Sku}

class ChangeMapperSuite extends munit.FunSuite {

  private val kettle =
    CatalogueProduct(Sku("SKU-KETTLE"), "Gooseneck kettle, 1 l", "hardware", Money.eur(7900), Availability.InStock)

  private def liveChange(doc: ujson.Value, revisions: List[Revision] = List(Revision("3-11aa"))): ChangeRow =
    ChangeRow(SequenceId("4-abc"), DocId("SKU-KETTLE"), revisions, DocumentState.Live, Some(doc))

  test("a changed product becomes a record keyed by the document identifier") {
    val outcome = ChangeMapper.map(liveChange(CatalogueDocument.toJson(kettle)))
    val record  = publishedBy(outcome)
    assertEquals(record.key, "SKU-KETTLE")
    val body = ujson.read(record.value.getOrElse(fail("expected a value, not a tombstone")))
    assertEquals(body("sku").str, "SKU-KETTLE")
    assertEquals(body("rev").str, "3-11aa")
    assertEquals(body("seq").str, "4-abc")
    assertEquals(body("conflicted").bool, false)
  }

  test("a conflicted document is published with the conflict flagged rather than hidden") {
    val revisions = List(Revision("3-11aa"), Revision("3-bb22"))
    val record    = publishedBy(ChangeMapper.map(liveChange(CatalogueDocument.toJson(kettle), revisions)))
    assertEquals(ujson.read(record.value.getOrElse(fail("expected a value")))("conflicted").bool, true)
  }

  test("a deleted document becomes a tombstone: same key, no value") {
    val deletion =
      ChangeRow(SequenceId("5-abc"), DocId("SKU-FILTER"), List(Revision("2-0f5d")), DocumentState.Deleted, None)
    assertEquals(publishedBy(ChangeMapper.map(deletion)), CatalogueRecord("SKU-FILTER", None))
  }

  test("a CouchDB internal document is ignored") {
    val designDoc =
      ChangeRow(SequenceId("2-abc"), DocId("_design/catalogue"), List(Revision("1-3c11")), DocumentState.Live, None)
    assertEquals(ChangeMapper.map(designDoc), Right(ChangeOutcome.Ignore(IgnoreReason.InternalDocument)))
  }

  test("a document of another kind sharing the database is ignored") {
    val cart = liveChange(ujson.Obj("_id" -> ujson.Str("cart-1"), "type" -> ujson.Str("cart")))
    assertEquals(ChangeMapper.map(cart), Right(ChangeOutcome.Ignore(IgnoreReason.NotAProduct)))
  }

  test("a change without its document is a problem the operator should hear about") {
    val row = ChangeRow(SequenceId("4-abc"), DocId("SKU-KETTLE"), List(Revision("3-11aa")), DocumentState.Live, None)
    assertEquals(ChangeMapper.map(row), Left(MappingProblem.DocumentNotIncluded(DocId("SKU-KETTLE"))))
  }

  private def publishedBy(outcome: Either[MappingProblem, ChangeOutcome]): CatalogueRecord = outcome match {
    case Right(ChangeOutcome.Publish(record)) => record
    case other                                => fail(s"expected a published record, got $other")
  }
}
