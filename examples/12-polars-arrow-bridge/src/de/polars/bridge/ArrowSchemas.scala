package de.polars.bridge

import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import scala.jdk.CollectionConverters.*

/**
 * The Apache Arrow schemas of the two tables this example exchanges.
 *
 * An Arrow schema is a list of named fields, each with a physical type and a nullability flag. Both sides of the bridge -
 * the Scala writer here and the Polars reader in the container - agree on nothing else: the schema travels inside the
 * file, so Polars never needs a copy of this code.
 *
 * Every field below is declared non-nullable. That is a real promise: the writer must set a value in every row, and in
 * exchange Polars can skip the null-checking branch of its kernels.
 */
object ArrowSchemas {

  private def utf8(name: String): Field =
    new Field(name, new FieldType(false, ArrowType.Utf8.INSTANCE, null), null)

  private def int32(name: String): Field =
    new Field(name, new FieldType(false, new ArrowType.Int(32, true), null), null)

  private def int64(name: String): Field =
    new Field(name, new FieldType(false, new ArrowType.Int(64, true), null), null)

  /**
   * Milliseconds since 1970-01-01, with no time zone attached.
   *
   * Arrow distinguishes "an instant in a named zone" from "a naive wall-clock reading". The shared domain stores plain
   * epoch milliseconds, so the honest mapping is the zone-less variant; Polars surfaces it as `datetime[ms]`.
   */
  private def timestampMillis(name: String): Field =
    new Field(name, new FieldType(false, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), null), null)

  /** Column names of the fact table, in schema order. */
  object OrderLineColumns {
    val orderId        = "order_id"
    val customerId     = "customer_id"
    val country        = "country"
    val placedAt       = "placed_at"
    val sku            = "sku"
    val quantity       = "quantity"
    val unitPriceCents = "unit_price_cents"
    val lineTotalCents = "line_total_cents"
  }

  /** Column names of the dimension table. */
  object RegionColumns {
    val country = "country"
    val region  = "region"
  }

  /** Column names of the aggregate table that Polars hands back. */
  object RevenueColumns {
    val country      = "country"
    val region       = "region"
    val orderCount   = "order_count"
    val units        = "units"
    val revenueCents = "revenue_cents"
  }

  val orderLines: Schema = new Schema(
    List(
      utf8(OrderLineColumns.orderId),
      utf8(OrderLineColumns.customerId),
      utf8(OrderLineColumns.country),
      timestampMillis(OrderLineColumns.placedAt),
      utf8(OrderLineColumns.sku),
      int32(OrderLineColumns.quantity),
      int64(OrderLineColumns.unitPriceCents),
      int64(OrderLineColumns.lineTotalCents)
    ).asJava
  )

  val regions: Schema = new Schema(
    List(utf8(RegionColumns.country), utf8(RegionColumns.region)).asJava
  )
}
