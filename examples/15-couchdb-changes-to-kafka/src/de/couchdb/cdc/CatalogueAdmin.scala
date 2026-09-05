package de.couchdb.cdc

import de.common.domain.Sku

/**
 * The commands that change or read the catalogue in Apache CouchDB, as opposed to the connector that follows it.
 *
 * They exist so the example can be driven end to end from one program: seed the catalogue, watch the connector pick the
 * changes up, take a product out of stock, delete one, and read the catalogue back through both query styles.
 */
object CatalogueAdmin {

  /** Creates the database, installs the design document, and writes the starting catalogue. */
  def seed(client: CouchDbClient, log: ConnectorLog): Unit = {
    client.createDatabaseIfAbsent()
    client.save(CatalogueQueries.designDocumentId, CatalogueQueries.designDocument)
    Catalogue.initial.foreach { product =>
      val revision = client.save(DocId(product.sku.value), CatalogueDocument.toJson(product))
      log.note(s"stored ${product.sku.value} as revision ${revision.value}")
    }
  }

  /** Changes a product's availability, which produces exactly one new revision and one entry in the feed. */
  def setAvailability(client: CouchDbClient, sku: Sku, availability: Availability, log: ConnectorLog): Unit = {
    val id = DocId(sku.value)
    client.fetch(id).flatMap(CatalogueDocument.fromJson(_).toOption) match {
      case None          => log.note(s"${sku.value} is not in the catalogue")
      case Some(product) =>
        val revision = client.save(id, CatalogueDocument.toJson(product.copy(availability = availability)))
        log.note(s"${sku.value} is now $availability at revision ${revision.value}")
    }
  }

  /** Removes a product. The connector turns the resulting CouchDB tombstone into a Kafka tombstone. */
  def remove(client: CouchDbClient, sku: Sku, log: ConnectorLog): Unit = {
    client.delete(DocId(sku.value))
    log.note(s"deleted ${sku.value}")
  }

  /** Reads the catalogue back with a Mango query and with the design document's view, and prints both answers. */
  def report(client: CouchDbClient, category: String, log: ConnectorLog): Unit = {
    val products = CatalogueQueries.productsFrom(client.find(CatalogueQueries.inStockInCategory(category, limit = 25)))
    log.note(s"Mango query: ${products.size} in-stock product(s) in category '$category'")
    products.foreach(product => log.note(s"  ${product.sku.value} ${product.name} ${product.price}"))

    val counts =
      CatalogueQueries.categoryCountsFrom(
        client.groupedView(CatalogueQueries.designDocumentId, CatalogueQueries.byCategoryView)
      )
    log.note("view by_category: products per category")
    counts.foreach { case (name, count) => log.note(s"  $name: $count") }
  }
}
