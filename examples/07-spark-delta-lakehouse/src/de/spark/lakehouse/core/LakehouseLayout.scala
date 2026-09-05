package de.spark.lakehouse.core

/**
 * Where every table lives underneath one warehouse root.
 *
 * A Delta Lake table is a directory, not a row in a catalogue: the data files and the transaction log that describes
 * them sit side by side under a single path. Concentrating that path arithmetic here means no other file has to build a
 * string out of a root and a table name, and swapping a local folder for object storage changes exactly one value.
 *
 * @param root
 *   the warehouse root, for example `/tmp/lakehouse` locally or `s3a://lakehouse/warehouse` against object storage. A
 *   trailing slash is tolerated and removed.
 */
final case class LakehouseLayout(root: String) {

  private val normalisedRoot: String = root.replaceAll("/+$", "")

  def bronzeOrders: String    = table("bronze", "orders")
  def bronzePayments: String  = table("bronze", "payments")
  def bronzeShipments: String = table("bronze", "shipments")

  def silverOrders: String    = table("silver", "orders")
  def silverPayments: String  = table("silver", "payments")
  def silverShipments: String = table("silver", "shipments")

  /** The slowly changing customer dimension, maintained with `MERGE INTO` rather than rewritten. */
  def silverCustomers: String = table("silver", "customers")

  def goldDailyRevenue: String     = table("gold", "daily_revenue_by_country")
  def goldCustomerLifetime: String = table("gold", "customer_lifetime_value")

  /** The Delta Lake transaction log of a table: the directory that turns a pile of Parquet files into a table. */
  def transactionLogOf(tablePath: String): String = s"$tablePath/_delta_log"

  private def table(layer: String, name: String): String = s"$normalisedRoot/$layer/$name"
}
