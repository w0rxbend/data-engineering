package de.parquet.arrow

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.jdk.CollectionConverters.*

/**
 * The record shape handed to and received from the Parquet library.
 *
 * Parquet has its own schema language, but the `parquet-avro` bridge used here accepts an Apache Avro schema and
 * translates it. Avro appears for that reason alone: no Avro file is written anywhere in this example.
 *
 * The namespace intentionally differs from this Scala package. Avro's reader looks for a compiled Java class whose full
 * name matches the record's, and `de.parquet.arrow.ArchiveRow` does exist here - it is a Scala case class with no
 * no-argument constructor, so Avro could not instantiate it.
 */
object ArchiveSchema {

  val avro: Schema = new Schema.Parser().parse(
    """{
      |  "type": "record",
      |  "name": "OrderArchiveRow",
      |  "namespace": "de.parquet.arrow.avro",
      |  "fields": [
      |    {"name": "order_id",         "type": "string"},
      |    {"name": "customer_id",      "type": "string"},
      |    {"name": "country",          "type": "string"},
      |    {"name": "placed_at",        "type": "long"},
      |    {"name": "sku",              "type": "string"},
      |    {"name": "quantity",         "type": "int"},
      |    {"name": "unit_price_cents", "type": "long"},
      |    {"name": "line_total_cents", "type": "long"}
      |  ]
      |}""".stripMargin
  )

  /**
   * A schema holding only the named fields, used to push a projection down into the reader.
   *
   * Handing this reduced schema to the Parquet reader is what makes it seek past the other column chunks instead of
   * decoding them and discarding the result. The field order of the full schema is preserved so that the projected
   * schema stays a readable subset of it.
   */
  def projection(columns: Set[String]): Schema = {
    val kept = avro.getFields.asScala.iterator
      .filter(field => columns.contains(field.name))
      .map(field => new Schema.Field(field.name, field.schema, field.doc, field.defaultVal))
      .toList
    require(kept.size == columns.size, s"unknown column(s): ${columns.diff(avro.getFields.asScala.map(_.name).toSet)}")
    Schema.createRecord(avro.getName + "Projection", avro.getDoc, avro.getNamespace, false, kept.asJava)
  }

  /** Converts an archive row into the Avro record the Parquet writer consumes. */
  def toRecord(row: ArchiveRow): GenericRecord = {
    val record = new GenericData.Record(avro)
    record.put("order_id", row.orderId)
    record.put("customer_id", row.customerId)
    record.put("country", row.country)
    record.put("placed_at", row.placedAtEpochMillis)
    record.put("sku", row.sku)
    record.put("quantity", row.quantity)
    record.put("unit_price_cents", row.unitPriceCents)
    record.put("line_total_cents", row.lineTotalCents)
    record
  }

  /**
   * Converts a fully populated Avro record back into an archive row.
   *
   * Avro hands string fields back as `org.apache.avro.util.Utf8`, a mutable buffer rather than a `java.lang.String`, so
   * every text field is copied out with `toString`. Skipping that step is a classic source of bugs: the same buffer is
   * reused for the next record, and a collection of unconverted values ends up holding the last row many times over.
   */
  def toRow(record: GenericRecord): ArchiveRow =
    ArchiveRow(
      orderId = text(record, "order_id"),
      customerId = text(record, "customer_id"),
      country = text(record, "country"),
      placedAtEpochMillis = record.get("placed_at").asInstanceOf[Long],
      sku = text(record, "sku"),
      quantity = record.get("quantity").asInstanceOf[Int],
      unitPriceCents = record.get("unit_price_cents").asInstanceOf[Long],
      lineTotalCents = record.get("line_total_cents").asInstanceOf[Long]
    )

  private def text(record: GenericRecord, field: String): String = record.get(field).toString
}
