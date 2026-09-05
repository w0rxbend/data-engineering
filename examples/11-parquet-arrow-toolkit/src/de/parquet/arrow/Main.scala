package de.parquet.arrow

import de.common.gen.DataGenerator
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import java.nio.file.{Files, Path as JPath}

/**
 * Everything the example reads from the environment, so the same program works against a scratch directory and against
 * object storage.
 *
 * @param orderCount
 *   how many orders to archive. Each order has one to four lines, so the default produces roughly thirty thousand rows:
 *   large enough to fill several row groups at the demonstration row-group size, small enough to run in seconds.
 * @param uploadToObjectStore
 *   whether to copy the finished archive to MinIO at the end. Off by default, because the rest of the example needs no
 *   containers at all.
 */
final case class Settings(
    archiveDirectory: JPath,
    orderCount: Int,
    generatorSeed: Long,
    arrowBatchRows: Int,
    uploadToObjectStore: Boolean,
    objectStore: ObjectStoreConfig
)

object Settings {

  def fromEnvironment(): Settings = {
    def env(name: String, fallback: String): String = Option(System.getenv(name)).filter(_.nonEmpty).getOrElse(fallback)
    Settings(
      archiveDirectory = JPath.of(env("ARCHIVE_DIR", "out/11-parquet-arrow-toolkit")),
      orderCount = env("ORDER_COUNT", "12000").toInt,
      generatorSeed = env("GENERATOR_SEED", "42").toLong,
      arrowBatchRows = env("ARROW_BATCH_ROWS", "4096").toInt,
      uploadToObjectStore = env("UPLOAD_TO_OBJECT_STORE", "false").toBoolean,
      objectStore = ObjectStoreConfig(
        endpoint = env("S3_ENDPOINT", "http://localhost:11100"),
        bucket = env("S3_BUCKET", "archive"),
        accessKey = env("S3_ACCESS_KEY", "minioadmin"),
        secretKey = env("S3_SECRET_KEY", "minioadmin"),
        prefix = env("S3_PREFIX", "orders")
      )
    )
  }
}

/**
 * Writes an order archive to Apache Parquet, takes it apart, and moves a slice of it through Apache Arrow.
 *
 * The program is one straight line: generate, write, inspect, compare read strategies, compare codecs, cross into
 * Arrow, and optionally upload. Everything that contains a decision worth testing lives in its own object; what is left
 * here is wiring and printing.
 */
object Main {

  private val StatisticsColumn = "placed_at"

  def main(args: Array[String]): Unit = {
    val settings = Settings.fromEnvironment()
    Files.createDirectories(settings.archiveDirectory)

    val rows = generate(settings)
    heading(s"1. archive: ${rows.size} order lines from ${settings.orderCount} orders")

    val archive = ParquetArchiveWriter.write(settings.archiveDirectory.resolve("orders.parquet"), rows)
    val layout  = FooterReader.read(archive.path)
    println(Reports.fileSummary(layout))

    heading("2. row groups, as the footer records them")
    println(Reports.rowGroups(layout, StatisticsColumn))

    heading("3. columns, as the footer records them")
    println(Reports.columns(layout))

    heading("4. what each read strategy has to touch")
    reportReadStrategies(archive.path, layout, rows)

    heading("5. compression codecs and dictionary encoding, same rows each time")
    reportCodecs(settings.archiveDirectory, rows)

    heading("6. the same data in Apache Arrow, in memory and as an IPC file")
    reportArrow(settings, rows)

    if (settings.uploadToObjectStore) {
      heading("7. the archive in object storage")
      reportUpload(settings)
    } else {
      heading("7. object storage")
      println("skipped; set UPLOAD_TO_OBJECT_STORE=true with the MinIO container running to copy the archive there")
    }
  }

  private def generate(settings: Settings): Vector[ArchiveRow] = {
    val generator = new DataGenerator(seed = settings.generatorSeed)
    OrderArchive.rowsFrom(generator.orders(settings.orderCount))
  }

  /**
   * Runs the same question three ways and prints both the predicted and the measured cost.
   *
   * The prediction comes from `ScanPlanner`, which only looks at footer metadata. The measurement comes from Hadoop's
   * own byte counter. They will not match exactly - the reader also reads the footer, and page headers, and rounds
   * reads up to buffer boundaries - but they move together, which is the point.
   */
  private def reportReadStrategies(file: JPath, layout: ParquetLayout, rows: Vector[ArchiveRow]): Unit = {
    val allColumns = layout.columnNames.toSet
    // The middle fifth of the archive's time range: a window narrow enough that most row
    // groups can be skipped, wide enough that the answer is not trivially empty.
    val (lowerBound, upperBound) = middleWindow(rows)

    val predicted = Seq(
      "full scan (all columns, all groups)"                                        -> ScanPlanner.fullScan(layout),
      s"projection (${OrderArchive.ProjectedColumns.toSeq.sorted.mkString(", ")})" ->
        ScanPlanner.projectedScan(layout, OrderArchive.ProjectedColumns),
      s"predicate ($StatisticsColumn in a 20% window)" ->
        ScanPlanner.filteredScan(layout, StatisticsColumn, lowerBound, upperBound, allColumns)
    )
    println(Reports.scanComparison(predicted))

    val full      = ArchiveReader.readAll(file)
    val projected = ArchiveReader.readProjected(file, OrderArchive.ProjectedColumns) { record =>
      (record.get("sku").toString, record.get("line_total_cents").asInstanceOf[Long])
    }
    val filtered = ArchiveReader.readFiltered(file, StatisticsColumn, lowerBound, upperBound)

    println()
    println(
      Reports.table(
        Seq("read strategy", "rows returned", "bytes actually read"),
        Seq(
          Seq("full scan", full.value.size.toString, Reports.bytes(full.bytesRead)),
          Seq("projection", projected.value.size.toString, Reports.bytes(projected.bytesRead)),
          Seq("predicate", filtered.value.size.toString, Reports.bytes(filtered.bytesRead))
        )
      )
    )

    val revenueFromProjection = projected.value.groupMapReduce(_._1)(_._2)(_ + _)
    val revenueFromFullScan   = OrderArchive.revenueBySku(full.value)
    println()
    println(s"revenue per stock keeping unit agrees across strategies: ${revenueFromProjection == revenueFromFullScan}")
  }

  /** The middle fifth of the archive's timestamp range. */
  private def middleWindow(rows: Vector[ArchiveRow]): (Long, Long) = {
    val timestamps = rows.map(_.placedAtEpochMillis)
    val earliest   = timestamps.min
    val span       = timestamps.max - earliest
    (earliest + (span * 2) / 5, earliest + (span * 3) / 5)
  }

  private def reportCodecs(directory: JPath, rows: Vector[ArchiveRow]): Unit = {
    val variants = Seq(
      WriteOptions(CompressionCodecName.UNCOMPRESSED),
      WriteOptions(CompressionCodecName.SNAPPY),
      WriteOptions(CompressionCodecName.ZSTD),
      WriteOptions(CompressionCodecName.ZSTD, dictionaryEncoding = false)
    )
    val written = ParquetArchiveWriter.writeVariants(directory, "orders", rows, variants)
    println(Reports.codecComparison(written.map { file =>
      val label =
        s"${file.options.codec.name.toLowerCase}${if (file.options.dictionaryEncoding) "" else ", no dictionary"}"
      CodecMeasurement(label, file.sizeBytes, file.options.dictionaryEncoding)
    }))

    val snappyLayout = FooterReader.read(written(1).path)
    println()
    println(
      s"country is dictionary encoded: ${snappyLayout.isDictionaryEncoded("country")}, " +
        s"and holds ${Reports.bytes(snappyLayout.compressedBytesOf("country"))} " +
        s"for ${snappyLayout.rowCount} values"
    )
  }

  private def reportArrow(settings: Settings, rows: Vector[ArchiveRow]): Unit = {
    val batch = rows.take(settings.arrowBatchRows)
    ArrowBridge.withBatch(batch) { root =>
      println(Reports.arrowFootprint(ArrowBridge.footprint(root)))
      val fromArrow = ArrowBridge.sumColumn(root, "line_total_cents")
      val fromRows  = batch.map(_.lineTotalCents).sum
      println()
      println(
        s"sum of line_total_cents read straight from the Arrow buffer: $fromArrow (matches: ${fromArrow == fromRows})"
      )
    }

    val ipcFile      = settings.archiveDirectory.resolve("orders.arrow")
    val ipcBytes     = ArrowBridge.writeIpcFile(ipcFile, rows, settings.arrowBatchRows)
    val roundTrip    = ArrowBridge.readIpcFile(ipcFile)
    val parquetBytes = Files.size(settings.archiveDirectory.resolve("orders.parquet"))
    println()
    println(
      s"Arrow IPC file: ${Reports.bytes(ipcBytes)} in ${roundTrip.batches} record batches, " +
        s"${roundTrip.rows.size} rows read back unchanged: ${roundTrip.rows == rows}"
    )
    println(
      s"the same rows as Parquet: ${Reports.bytes(parquetBytes)} " +
        s"(the Arrow file is ${Reports.factor(ipcBytes.toDouble / parquetBytes.toDouble)} larger, " +
        "because Parquet compresses and encodes while Arrow stays directly usable)"
    )
  }

  private def reportUpload(settings: Settings): Unit =
    ObjectStore.withClient(settings.objectStore) { client =>
      ObjectStore.ensureBucket(client, settings.objectStore.bucket)
      val uploaded = ObjectStore.uploadDirectory(client, settings.objectStore, settings.archiveDirectory)
      println(
        Reports.table(
          Seq("object key", "size"),
          uploaded.map(entry => Seq(entry.key, Reports.bytes(entry.sizeBytes)))
        )
      )
      println()
      println(
        s"objects now under s3://${settings.objectStore.bucket}/${settings.objectStore.prefix}: " +
          ObjectStore.list(client, settings.objectStore).size
      )
    }

  private def heading(title: String): Unit = {
    println()
    println(s"== $title")
    println()
  }
}
