package de.couchdb.cdc

class CatalogueQueriesSuite extends munit.FunSuite {

  test("the Mango selector asks for products of one category that are in stock") {
    val selector = CatalogueQueries.inStockInCategory("hardware", limit = 25)("selector")
    assertEquals(selector("type").str, "product")
    assertEquals(selector("category").str, "hardware")
    assertEquals(selector("availability").str, "InStock")
  }

  test("the design document declares a counted view keyed by category") {
    val view = CatalogueQueries.designDocument("views")(CatalogueQueries.byCategoryView)
    assertEquals(view("reduce").str, "_count")
    assert(view("map").str.contains("emit(doc.category"))
  }

  test("a Mango response is read into products, skipping anything unreadable") {
    val response = ujson.Obj(
      "docs" -> ujson.Arr(
        CatalogueDocument.toJson(Catalogue.initial.head),
        ujson.Obj("_id" -> ujson.Str("broken"))
      )
    )
    assertEquals(CatalogueQueries.productsFrom(response).map(_.sku), List(Catalogue.initial.head.sku))
  }

  test("a grouped view response is read into category counts, sorted by category") {
    val response = ujson.Obj(
      "rows" -> ujson.Arr(
        ujson.Obj("key" -> ujson.Str("hardware"), "value" -> ujson.Num(2)),
        ujson.Obj("key" -> ujson.Str("beans"), "value"    -> ujson.Num(1))
      )
    )
    assertEquals(CatalogueQueries.categoryCountsFrom(response), List("beans" -> 1L, "hardware" -> 2L))
  }
}
