package de.spark.lakehouse.job

import org.apache.spark.sql.SparkSession

/**
 * Builds the Apache Spark session, which is the object that owns the connection to the cluster (or, locally, to the
 * threads pretending to be one).
 *
 * This is the composition root of the example: the only place that knows about Delta Lake configuration keys, object
 * store credentials and thread counts. Nothing under `core` imports this file.
 */
object SparkSessions {

  /**
   * Two settings turn a stock Spark session into a Delta Lake session.
   *
   *   - `spark.sql.extensions` adds the SQL grammar and query planning rules for Delta Lake commands such as
   *     `MERGE INTO`, `OPTIMIZE` and time travel.
   *   - `spark.sql.catalog.spark_catalog` replaces the default catalogue so that `saveAsTable`, `DESCRIBE` and friends
   *     understand Delta tables.
   *
   * Without them, `format("delta")` writes files that no Delta Lake feature can act on.
   */
  private val DeltaSettings = Map(
    "spark.sql.extensions"            -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    // Every date in this example is a calendar day in Coordinated Universal Time (UTC). Pinning the session time zone
    // means a report produced in Warsaw and one produced in Kyiv book a sale on the same day.
    "spark.sql.session.timeZone" -> "UTC",
    // The default of 200 shuffle partitions produces 200 tiny files per write on a laptop-sized data set.
    "spark.sql.shuffle.partitions" -> "4"
  )

  /**
   * Creates a session for the given configuration.
   *
   * @param appName
   *   the name shown in the Spark user interface
   * @param config
   *   warehouse location and, when the warehouse is in object storage, how to reach it
   */
  def create(appName: String, config: JobConfig): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)

    // A cluster passes the master address on the spark-submit command line, and overriding it here would ignore that.
    // Only when nothing was supplied does the session fall back to running inside this Java process.
    if (!sys.props.contains("spark.master") && sys.env.get("SPARK_MASTER_URL").isEmpty) {
      builder.master("local[*]")
    }

    DeltaSettings.foreach { case (key, value) => builder.config(key, value) }
    config.objectStore.foreach(store =>
      objectStoreSettings(store).foreach { case (key, value) =>
        builder.config(key, value)
      }
    )

    builder.getOrCreate()
  }

  /**
   * Hadoop's S3A filesystem is what Spark uses to read and write `s3a://` paths. These four keys point it at MinIO
   * instead of Amazon and hand it the credentials directly, which is acceptable for a local demonstration; a production
   * deployment would use an identity provider rather than a literal key pair.
   */
  private def objectStoreSettings(store: ObjectStoreConfig): Map[String, String] = Map(
    "spark.hadoop.fs.s3a.endpoint"                 -> store.endpoint,
    "spark.hadoop.fs.s3a.access.key"               -> store.accessKey,
    "spark.hadoop.fs.s3a.secret.key"               -> store.secretKey,
    "spark.hadoop.fs.s3a.path.style.access"        -> store.pathStyleAccess.toString,
    "spark.hadoop.fs.s3a.connection.ssl.enabled"   -> (!store.endpoint.startsWith("http://")).toString,
    "spark.hadoop.fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  )
}
