package de.presto.hive

import de.common.gen.DataGenerator

import java.nio.file.{Files, Path as JPath}
import scala.util.Using

/**
 * Settings the example reads from the environment, so that the same program works against the bundled Docker stack and
 * against a cluster somewhere else.
 *
 * @param eventCount
 *   how many clickstream events to generate. The default spans roughly three and a half simulated days across five
 *   countries, which gives around twenty partitions: enough for pruning to be worth measuring, small enough to finish
 *   in seconds.
 */
final case class Settings(
    prestoUrl: String,
    prestoUser: String,
    objectStore: ObjectStoreConfig,
    eventCount: Int,
    generatorSeed: Long,
    generatorStartEpochMillis: Long
)

object Settings {

  /** 2023-11-14T00:00:00Z, so that the first simulated day is a whole day rather than a stub. */
  val DefaultStartEpochMillis: Long = 1699920000000L

  def fromEnvironment(): Settings = {
    def env(name: String, fallback: String): String = Option(System.getenv(name)).filter(_.nonEmpty).getOrElse(fallback)
    Settings(
      prestoUrl = env("PRESTO_URL", "jdbc:presto://localhost:11080/hive/shop"),
      prestoUser = env("PRESTO_USER", "analyst"),
      objectStore = ObjectStoreConfig(
        endpoint = env("S3_ENDPOINT", "http://localhost:11000"),
        bucket = env("S3_BUCKET", "lake"),
        accessKey = env("S3_ACCESS_KEY", "minioadmin"),
        secretKey = env("S3_SECRET_KEY", "minioadmin"),
        prefix = env("S3_PREFIX", "clickstream")
      ),
      eventCount = env("EVENT_COUNT", "600000").toInt,
      generatorSeed = env("GENERATOR_SEED", "42").toLong,
      generatorStartEpochMillis = env("GENERATOR_START_MILLIS", DefaultStartEpochMillis.toString).toLong
    )
  }
}

/**
 * Loads a partitioned clickstream into object storage and then analyses it with PrestoDB.
 *
 * The program is one straight line on purpose: generate, write, upload, register, analyse, report. Every step that
 * contains a decision worth testing lives in its own object (`Clickstream`, `HivePartition`, `HiveSql`, `Reports`);
 * what is left here is the wiring.
 */
object Main {

  private val Sql = HiveSql()

  def main(args: Array[String]): Unit = {
    val settings = Settings.fromEnvironment()

    val rows = generate(settings)
    println(s"generated ${rows.size} clickstream events")

    val written = writeAndUpload(settings, rows)
    val days    = written.map(_.partition.day).distinct.sorted
    println(s"wrote ${written.size} Parquet files covering ${days.size} days and ${Clickstream.Countries.size} markets")

    Using.resource(new PrestoClient(settings.prestoUrl, settings.prestoUser)) { presto =>
      registerTable(presto, settings)
      // The middle day is the one every country has a full set of events for, so it is
      // the fair choice for the pruning comparison.
      val probe = HivePartition("DE", days(days.size / 2))
      reportPartitionPruning(presto, probe)
      reportFunnel(presto, days)
      reportConversion(presto, days)
    }
  }

  private def generate(settings: Settings): Vector[ClickRow] = {
    val generator = new DataGenerator(settings.generatorSeed, settings.generatorStartEpochMillis)
    Clickstream.rowsFrom(generator.clickStream.take(settings.eventCount).toVector)
  }

  /** Writes the Parquet tree to a temporary directory, uploads it, and removes the local copy. */
  private def writeAndUpload(settings: Settings, rows: Vector[ClickRow]): Seq[WrittenPartitionFile] = {
    val staging = Files.createTempDirectory("de-10-clickstream")
    try {
      val written = ParquetClickstreamWriter.write(staging, rows)
      ObjectStore.withClient(settings.objectStore) { client =>
        ObjectStore.ensureBucket(client, settings.objectStore.bucket)
        val removed = ObjectStore.clearPrefix(client, settings.objectStore.bucket, settings.objectStore.prefix)
        if (removed > 0) println(s"removed $removed objects left by a previous run")
        val keys = ObjectStore.uploadDirectory(client, settings.objectStore, staging)
        println(s"uploaded ${keys.size} objects to ${settings.objectStore.tableLocation}")
      }
      written
    } finally deleteRecursively(staging)
  }

  private def registerTable(presto: PrestoClient, settings: Settings): Unit = {
    // The schema location is only a default for tables created *without* an explicit
    // location. This example never creates one, but the metastore insists on having it.
    presto.execute(Sql.createSchema(s"s3a://${settings.objectStore.bucket}/warehouse"))
    presto.execute(Sql.dropTable)
    presto.execute(Sql.createExternalTable(settings.objectStore.tableLocation))
    presto.execute(Sql.syncPartitionMetadata)
    presto.execute(Sql.analyze)
    println(s"registered ${Sql.qualifiedTable} and collected statistics")
  }

  /**
   * Shows the two query plans side by side and measures what the difference costs.
   *
   * The plans are printed before the numbers because the plan explains the numbers: the pruned plan constrains the
   * partition columns, so the connector never opens the other partitions' files.
   */
  private def reportPartitionPruning(presto: PrestoClient, probe: HivePartition): Unit = {
    val wholeTable   = Sql.countAll
    val onePartition = Sql.countInPartition(probe)

    heading(s"EXPLAIN without a partition predicate")
    println(presto.explain(wholeTable, Sql))
    heading(s"EXPLAIN with the partition predicate ${probe.relativePath}")
    println(presto.explain(onePartition, Sql))

    val full   = presto.queryOne(wholeTable)(_.getLong(1))
    val pruned = presto.queryOne(onePartition)(_.getLong(1))

    heading("bytes read for the same count")
    println(s"whole table:   ${full.value} events")
    println(s"one partition: ${pruned.value} events")
    println(Reports.scanComparison(ScanComparison(full.processedBytes, pruned.processedBytes)))
  }

  private def reportFunnel(presto: PrestoClient, days: Seq[java.time.LocalDate]): Unit = {
    val counts = presto
      .queryOne(Sql.funnel(days)) { row =>
        FunnelCounts(row.getLong(1), row.getLong(2), row.getLong(3), row.getLong(4), row.getLong(5))
      }
      .value
    heading("funnel: /home -> /search -> /product -> /cart -> /checkout")
    println(Reports.funnel(counts))
  }

  private def reportConversion(presto: PrestoClient, days: Seq[java.time.LocalDate]): Unit = {
    val rows = presto
      .query(Sql.conversionByCountry(days))(row => ConversionRow(row.getString(1), row.getLong(2), row.getLong(3)))
      .value
    heading("conversion by country")
    println(Reports.conversion(rows))
  }

  private def heading(text: String): Unit = println(s"\n=== $text ===")

  private def deleteRecursively(root: JPath): Unit = {
    val stream = Files.walk(root)
    try {
      val paths = stream.toArray.map(_.asInstanceOf[JPath])
      paths.reverse.foreach(Files.deleteIfExists(_): Unit)
    } finally stream.close()
  }
}
