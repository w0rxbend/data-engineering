package de.spark.streaming

import org.apache.spark.sql.types._

/**
 * The explicit shape of an `Order` message as it is written to Apache Kafka by `de.common.json.Codecs.order`.
 *
 * Apache Spark can infer a JSON (JavaScript Object Notation) schema by scanning the data, but only for files it can
 * read twice. A stream has no "read it twice": the schema of a streaming source has to be known before the first record
 * arrives. Spelling the schema out has a second benefit that matters far beyond streaming - the job keeps producing the
 * same columns even when a producer starts sending an extra field, so a schema change upstream cannot silently change
 * the meaning of a downstream table.
 */
object OrderSchema {

  /** `{"cents":1234,"currency":"EUR"}` */
  val money: StructType = StructType(
    Seq(
      StructField("cents", LongType, nullable = false),
      StructField("currency", StringType, nullable = false)
    )
  )

  /** `{"sku":"SKU-MUG","quantity":2,"unitPrice":{...}}` */
  val orderLine: StructType = StructType(
    Seq(
      StructField("sku", StringType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
      StructField("unitPrice", money, nullable = false)
    )
  )

  /**
   * The whole order.
   *
   * `placedAt` is milliseconds since 1970-01-01 UTC, which is how every example in this repository transports a
   * timestamp. It is turned into a real Spark timestamp column by [[OrderStreams.parseOrders]].
   */
  val order: StructType = StructType(
    Seq(
      StructField("id", StringType, nullable = false),
      StructField("customerId", StringType, nullable = false),
      StructField("lines", ArrayType(orderLine, containsNull = false), nullable = false),
      StructField("placedAt", LongType, nullable = false),
      StructField("country", StringType, nullable = false)
    )
  )
}
