package de.spark.streaming

import org.apache.spark.sql.SparkSession

/** Builds the single-process Apache Spark session the example runs in. */
object SparkSessions {

  /**
   * A local session with Delta Lake enabled.
   *
   * Delta Lake is not part of Apache Spark. It plugs itself in through two settings: an *extension* that teaches the
   * SQL parser and planner about Delta commands such as `MERGE`, and a *catalog* that lets `spark.table("name")`
   * resolve Delta tables. Forgetting either one produces the confusing "MERGE is not supported" error even though the
   * Delta jar is on the classpath.
   *
   * `spark.sql.shuffle.partitions` defaults to 200. On a laptop that means 200 tiny files per aggregation batch; four
   * is plenty for a demo. On a real cluster this number is tuned to the data volume instead.
   *
   * @param master
   *   the Spark master URL. `local[*]` runs the whole job inside this JVM, using every available core.
   */
  def local(appName: String, master: String = "local[*]"): SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  /**
   * Points Hadoop's S3 client at an S3-compatible server such as MinIO.
   *
   * Two settings are what make a non-Amazon server work: an explicit `endpoint`, and `path.style.access`. Amazon's own
   * S3 addresses a bucket as `https://bucket.s3.amazonaws.com`; MinIO expects `http://host:9000/bucket`. Path-style
   * access is the second form, and without it every request goes to a hostname that does not resolve.
   */
  def configureS3(spark: SparkSession, endpoint: String, accessKey: String, secretKey: String): Unit = {
    val hadoop = spark.sparkContext.hadoopConfiguration
    hadoop.set("fs.s3a.endpoint", endpoint)
    hadoop.set("fs.s3a.access.key", accessKey)
    hadoop.set("fs.s3a.secret.key", secretKey)
    hadoop.set("fs.s3a.path.style.access", "true")
    hadoop.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
  }
}
