package de.spark.streaming

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame

/**
 * Writes each micro-batch of orders into a Delta Lake table as an *upsert*: insert the order if its id is new, replace
 * the stored row if the id is already there.
 *
 * Why not simply append? Because Apache Kafka promises at-least-once delivery and a shop may republish a corrected
 * order under the same id. An append-only table would then contain the same `orderId` twice and every dashboard
 * querying it would double-count that order. `MERGE` - the SQL statement Delta Lake implements on top of plain files in
 * object storage - makes the id genuinely unique.
 *
 * The mechanism that makes this safe is `foreachBatch`. A Spark streaming sink can normally only append rows; inside
 * `foreachBatch` the micro-batch is handed to you as an ordinary batch `DataFrame`, so the full batch API - including
 * Delta's `MERGE` - becomes available. Spark passes a `batchId` along with it, which is stable across retries: if the
 * job dies mid-batch and restarts, the very same batch is replayed with the same id. Delta records the last committed
 * batch id in its transaction log and ignores a replay of a batch it has already committed, which is what turns
 * "at-least-once from Kafka" into "exactly-once in the table".
 */
object DeltaOrderUpsert {

  /** Column that identifies an order uniquely, and therefore the column the merge matches on. */
  val KeyColumn = "orderId"

  /**
   * Builds the function passed to `writeStream.foreachBatch`.
   *
   * It is a factory rather than a plain method because `foreachBatch` wants a `(DataFrame, Long) => Unit` and the table
   * location has to be captured somewhere.
   *
   * @param tablePath
   *   where the Delta table lives; a local directory or an `s3a://` location
   */
  def intoTable(tablePath: String): (DataFrame, Long) => Unit = { (batch: DataFrame, _: Long) =>
    // The micro-batch is scanned several times below - once by the
    // deduplication, then twice more by MERGE, which reads its source to find
    // the matching rows and again to write them. An uncached DataFrame is
    // recomputed for every scan, and recomputing this one means re-reading the
    // same records from Kafka. Caching it makes the batch arrive exactly once.
    val cached = batch.cache()
    try {
      val deduplicated = OrderStreams.deduplicateByOrderId(cached)
      createTableIfMissing(deduplicated, tablePath)

      DeltaTable
        .forPath(batch.sparkSession, tablePath)
        .as("target")
        .merge(deduplicated.as("source"), s"target.$KeyColumn = source.$KeyColumn")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    } finally
      cached.unpersist()
  }

  /**
   * `MERGE` needs a table to merge into, and the very first micro-batch has none. Writing zero rows creates the
   * transaction log and the schema without inserting anything, which is the cheapest way to bootstrap.
   */
  private def createTableIfMissing(batch: DataFrame, tablePath: String): Unit =
    if (!DeltaTable.isDeltaTable(batch.sparkSession, tablePath)) {
      batch.limit(0).write.format("delta").mode("append").save(tablePath)
    }
}
