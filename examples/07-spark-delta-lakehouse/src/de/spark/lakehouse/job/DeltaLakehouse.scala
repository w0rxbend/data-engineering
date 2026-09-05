package de.spark.lakehouse.job

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Every conversation this example has with Delta Lake, in one place.
 *
 * The transformations under `core` know nothing about storage; this file is the adapter that turns "here is a
 * `DataFrame`" into "this is now a committed version of a table". Grouping the storage calls here means a reader can
 * see the whole Delta Lake feature set - transactions, upserts, schema rules, time travel, maintenance - as a short
 * list instead of hunting for it inside business logic.
 *
 * What makes a Delta Lake table different from a directory of Apache Parquet files is the transaction log in
 * `_delta_log`. Every write appends a numbered JSON file to that log describing which data files the table consists of
 * afterwards. A reader picks a log version and only sees the files that version lists, which is where atomic writes,
 * concurrent readers and time travel all come from.
 */
final class DeltaLakehouse(spark: SparkSession) {

  /**
   * Appends rows to a table, creating it when it does not exist yet.
   *
   * The write is atomic: either the new log entry is committed and every row becomes visible at once, or nothing
   * changes. A reader that starts while the write is in flight sees the previous version in full, never a half-written
   * table.
   */
  def append(rows: DataFrame, path: String): Unit =
    rows.write.format("delta").mode("append").save(path)

  /** Replaces the whole contents of a table. Previous versions stay in the log and remain readable via time travel. */
  def overwrite(rows: DataFrame, path: String): Unit =
    rows.write.format("delta").mode("overwrite").save(path)

  /**
   * Appends rows whose schema has grown, for example because the source system started sending a new field.
   *
   * By default Delta Lake refuses such a write - that is schema enforcement, and it is the feature that stops a
   * mistyped column from silently becoming part of a table. `mergeSchema` is the explicit opt-in: it says "this new
   * column is intended", and Delta Lake widens the table schema as part of the same transaction, backfilling the
   * missing value as null for every row that was already there.
   */
  def appendWithSchemaEvolution(rows: DataFrame, path: String): Unit =
    rows.write.format("delta").mode("append").option("mergeSchema", "true").save(path)

  /**
   * Upserts a customer dimension with `MERGE INTO`: rows that already exist are updated in place, rows that are new are
   * inserted, all inside one transaction.
   *
   * This is a slowly changing dimension of type 1 - the corrected attribute overwrites the old value and no history row
   * is kept, because the table already has history in its transaction log. Only the attributes a correction can
   * legitimately change are updated; the running totals are left alone, since a correction batch carries a fresh
   * customer's worth of counts rather than a new grand total. Doing the same on plain Parquet would mean reading the
   * table, joining in memory and rewriting the whole thing, with a window during which the table is missing or half
   * written.
   *
   * The update is guarded by a condition: an incoming row only wins when it is at least as recent as the stored one, so
   * replaying an old batch cannot move the dimension backwards.
   *
   * @param updates
   *   the incoming rows, keyed by `customer_id`
   * @param path
   *   the dimension table
   * @return
   *   how many rows the table holds afterwards
   */
  def mergeCustomerDimension(updates: DataFrame, path: String): Long = {
    if (!DeltaTable.isDeltaTable(spark, path)) {
      overwrite(updates, path)
    } else {
      DeltaTable
        .forPath(spark, path)
        .as("current")
        .merge(updates.as("incoming"), "current.customer_id = incoming.customer_id")
        .whenMatched("incoming.last_seen_epoch_millis >= current.last_seen_epoch_millis")
        .updateExpr(
          Map(
            "country"                -> "incoming.country",
            "last_seen_epoch_millis" -> "incoming.last_seen_epoch_millis"
          )
        )
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    spark.read.format("delta").load(path).count()
  }

  /** Reads a table as it looked at a specific version number, which is the position in the transaction log. */
  def readAtVersion(path: String, version: Long): DataFrame =
    spark.read.format("delta").option("versionAsOf", version).load(path)

  /**
   * Reads a table as it looked at a point in time.
   *
   * Version numbers are exact but meaningless to a human; a timestamp is how an analyst actually asks the question
   * ("what did yesterday's report see?"). Delta Lake resolves the timestamp to the latest version committed at or
   * before it.
   *
   * @param timestamp
   *   a value Apache Spark can parse as a timestamp, for example `2026-09-05 10:00:00`
   */
  def readAtTimestamp(path: String, timestamp: String): DataFrame =
    spark.read.format("delta").option("timestampAsOf", timestamp).load(path)

  /**
   * The moment a given version was committed, formatted the way `timestampAsOf` expects.
   *
   * The formatting is done inside Apache Spark on purpose. A `java.sql.Timestamp` prints itself in the time zone of the
   * machine, while `timestampAsOf` parses its argument in the session time zone, so handing one to the other shifts the
   * instant by the offset between them and asks for a version that does not exist yet.
   */
  def commitTimestampOf(path: String, version: Long): String =
    history(path)
      .where(col("version") === version)
      .select(date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"))
      .head()
      .getString(0)

  /** The commit history of a table: one row per version, with the operation that produced it. */
  def history(path: String): DataFrame =
    DeltaTable.forPath(spark, path).history()

  /** The newest version number of a table, which is also how many commits minus one it has seen. */
  def latestVersion(path: String): Long =
    DeltaTable.forPath(spark, path).history(1).select("version").head().getLong(0)

  /**
   * Compacts many small files into few large ones.
   *
   * Streaming and frequent appends leave a table with thousands of tiny files, and a query then spends more time
   * opening files than reading rows. `OPTIMIZE` rewrites them into larger ones and commits the result as a new version;
   * the old files stay on disk until they are vacuumed, so readers in flight are undisturbed.
   *
   * @return
   *   the number of files that existed before and after the compaction
   */
  def optimize(path: String): (Long, Long) = {
    val before = dataFileCount(path)
    DeltaTable.forPath(spark, path).optimize().executeCompaction()
    (before, dataFileCount(path))
  }

  /**
   * Deletes data files that no longer belong to any version young enough to be retained.
   *
   * The default retention is seven days, which protects readers and time travel. A demonstration cannot wait seven
   * days, so this method lowers the threshold - and because deleting recent files can break a running query, Delta Lake
   * requires the safety check to be switched off explicitly. The setting is restored afterwards so the rest of the job
   * runs with the safe default.
   *
   * @param retentionHours
   *   how much history to keep; `0` removes everything not referenced by the current version
   */
  def vacuum(path: String, retentionHours: Double): Unit = {
    val checkKey      = "spark.databricks.delta.retentionDurationCheck.enabled"
    val previousCheck = spark.conf.getOption(checkKey)
    spark.conf.set(checkKey, "false")
    try DeltaTable.forPath(spark, path).vacuum(retentionHours)
    finally
      previousCheck match {
        case Some(value) => spark.conf.set(checkKey, value)
        case None        => spark.conf.unset(checkKey)
      }
  }

  /** How many Apache Parquet data files the current version of the table is made of. */
  def dataFileCount(path: String): Long =
    spark.read.format("delta").load(path).inputFiles.length.toLong

  /**
   * The file names inside `_delta_log`, newest last.
   *
   * Printing them is the quickest way to show a newcomer what a Delta table really is: `00000000000000000000.json` is
   * the first commit, each later commit adds another numbered file, and every tenth commit is summarised into a
   * `.checkpoint.parquet` so that a reader does not have to replay the log from the beginning.
   */
  def transactionLogFiles(logPath: String): Seq[String] = {
    val path       = new Path(logPath)
    val fileSystem = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(path)) Seq.empty
    else fileSystem.listStatus(path).map(_.getPath.getName).sorted.toSeq
  }
}
