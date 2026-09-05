package de.couchdb.cdc

import de.common.domain.{Money, Sku}

class CatalogueDocumentSuite extends munit.FunSuite {

  private val mug =
    CatalogueProduct(Sku("SKU-MUG"), "Stoneware mug, 300 ml", "accessories", Money.eur(1490), Availability.OutOfStock)

  test("a product survives a trip through the CouchDB document shape") {
    assertEquals(CatalogueDocument.fromJson(CatalogueDocument.toJson(mug)), Right(mug))
  }

  test("the document identifier is the SKU, so writing the same product twice updates one document") {
    assertEquals(CatalogueDocument.toJson(mug)("_id").str, "SKU-MUG")
  }

  test("the price uses the repository-wide money encoding") {
    val price = CatalogueDocument.toJson(mug)("price")
    assertEquals(price("cents").num.toLong, 1490L)
    assertEquals(price("currency").str, "EUR")
  }

  test("a missing field is reported, not guessed") {
    val withoutCategory = ujson.Obj("_id" -> ujson.Str("SKU-MUG"), "name" -> ujson.Str("Mug"))
    assertEquals(CatalogueDocument.fromJson(withoutCategory), Left(DocumentProblem.MissingField("category")))
  }

  test("the generation of a revision is the number in front of the hash") {
    assertEquals(Revision("2-8f4c9b1e").generation, Some(2))
    assertEquals(Revision("not-a-revision").generation, None)
  }

  test("an availability value nobody knows is reported") {
    val document = CatalogueDocument.toJson(mug)
    document("availability") = ujson.Str("Maybe")
    assertEquals(CatalogueDocument.fromJson(document), Left(DocumentProblem.UnknownAvailability("Maybe")))
  }
}
