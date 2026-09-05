package de.couchdb.cdc

/**
 * The two ways this example reads the catalogue back out of Apache CouchDB.
 *
 * CouchDB offers a choice of query styles, and the example shows both:
 *
 *   - a *design document* holding a *view*. A view is a pair of JavaScript functions (a map function, and optionally a
 *     reduce function) that CouchDB runs over every document once and then keeps up to date incrementally. The result
 *     is a persistent, sorted index - fast to read, but you have to decide up front what to index.
 *   - a *Mango query*: a declarative JSON selector, close in spirit to a MongoDB query or a SQL `WHERE` clause. No
 *     JavaScript, no design document, and the query can be written the moment a question comes up.
 *
 * Everything here is a pure JSON builder or reader, so the tests can check the exact documents that would be sent.
 */
object CatalogueQueries {

  /** The `_id` of the design document holding the view. */
  val designDocumentId: DocId = DocId("_design/catalogue")

  /** The view's name; the full read URL is `<database>/_design/catalogue/_view/by_category`. */
  val byCategoryView: String = "by_category"

  /**
   * The design document.
   *
   * The map function is JavaScript, executed by CouchDB itself: for every product document it emits one index entry
   * keyed by category. The reduce function is the built-in `_count`, so asking for the view with `group=true` returns
   * one row per category with the number of products in it.
   */
  val designDocument: ujson.Obj =
    ujson.Obj(
      "_id"      -> ujson.Str(designDocumentId.value),
      "language" -> ujson.Str("javascript"),
      "views"    -> ujson.Obj(
        byCategoryView -> ujson.Obj(
          "map" ->
            ujson.Str(
              s"function (doc) { if (doc.type === '${CatalogueDocument.documentType}') { emit(doc.category, 1); } }"
            ),
          "reduce" -> ujson.Str("_count")
        )
      )
    )

  /**
   * A Mango query for every in-stock product of one category.
   *
   * `selector` is the filter, `fields` limits which fields come back, and `limit` bounds the answer. Without a matching
   * Mango index CouchDB still answers, by scanning every document, and warns about it in the response.
   */
  def inStockInCategory(category: String, limit: Int): ujson.Obj =
    ujson.Obj(
      "selector" -> ujson.Obj(
        "type"         -> ujson.Str(CatalogueDocument.documentType),
        "category"     -> ujson.Str(category),
        "availability" -> ujson.Str(Availability.InStock.toString)
      ),
      "fields" -> ujson.Arr("_id", "name", "category", "price", "availability"),
      "limit"  -> ujson.Num(limit.toDouble)
    )

  /** Reads the `docs` array of a Mango response, dropping anything that is not a readable product. */
  def productsFrom(response: ujson.Value): List[CatalogueProduct] =
    response match {
      case obj: ujson.Obj =>
        obj.value.get("docs").collect { case ujson.Arr(docs) => docs.toList }.getOrElse(Nil).flatMap { doc =>
          CatalogueDocument.fromJson(doc).toOption
        }
      case _ => Nil
    }

  /** Reads a grouped view response into `category -> product count` pairs, sorted by category. */
  def categoryCountsFrom(response: ujson.Value): List[(String, Long)] =
    response match {
      case obj: ujson.Obj =>
        obj.value
          .get("rows")
          .collect { case ujson.Arr(rows) => rows.toList }
          .getOrElse(Nil)
          .collect { case row: ujson.Obj => countRow(row) }
          .flatten
          .sortBy(_._1)
      case _ => Nil
    }

  private def countRow(row: ujson.Obj): Option[(String, Long)] =
    for {
      key   <- row.value.get("key").collect { case ujson.Str(value) => value }
      count <- row.value.get("value").collect { case ujson.Num(value) => value.toLong }
    } yield key -> count
}
