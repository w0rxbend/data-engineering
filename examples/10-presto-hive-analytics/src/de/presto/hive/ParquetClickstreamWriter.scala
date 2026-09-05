package de.presto.hive

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile

import java.nio.file.{Files, Path as JPath}
import scala.jdk.CollectionConverters.*

/** One Parquet file that was written, together with the partition it belongs to and how many rows it holds. */
final case class WrittenPartitionFile(partition: HivePartition, file: JPath, rowCount: Int)

/**
 * Writes clickstream rows to local Apache Parquet files laid out in the Apache Hive partition convention.
 *
 * Two decisions are worth spelling out.
 *
 * The writer targets the *local* filesystem, not object storage. Uploading is a separate step
 * (`ObjectStore.uploadDirectory`), which means this class can be unit tested against a temporary directory with no
 * containers running at all, and the upload step can be tested or replaced on its own.
 *
 * The partition columns `country` and `dt` are deliberately absent from the file schema. In Hive, a partition column
 * lives in the directory name and nowhere else; writing it into the file as well would waste space and, worse, allow
 * the two copies to disagree.
 */
object ParquetClickstreamWriter {

  /**
   * A Hadoop configuration that writes plain files.
   *
   * Hadoop's default local filesystem writes a hidden `.crc` checksum file next to every file it creates. Those sidecar
   * files would be uploaded to object storage along with the data and would then confuse the Hive connector, which
   * treats every object under a partition directory as data. `RawLocalFileSystem` is the same filesystem without the
   * checksums.
   */
  private def plainLocalFilesystem(): Configuration = {
    val configuration = new Configuration()
    configuration.set("fs.file.impl", classOf[org.apache.hadoop.fs.RawLocalFileSystem].getName)
    configuration
  }

  /**
   * The Avro record shape used purely as the in-memory hand-off to the Parquet writer.
   *
   * The namespace deliberately differs from this package. Avro's reader looks for a compiled class whose full name
   * matches the record's, and `de.presto.hive.ClickRow` does exist here - it is the case class above, which has no
   * no-argument constructor and so cannot be instantiated that way.
   */
  val AvroSchema: Schema = new Schema.Parser().parse(
    """{
      |  "type": "record",
      |  "name": "ClickstreamRecord",
      |  "namespace": "de.presto.hive.avro",
      |  "fields": [
      |    {"name": "customer_id", "type": "string"},
      |    {"name": "page",        "type": "string"},
      |    {"name": "sku",         "type": ["null", "string"], "default": null},
      |    {"name": "occurred_at", "type": "long"}
      |  ]
      |}""".stripMargin
  )

  /**
   * Writes one Parquet file per partition under `root` and returns what was written, sorted by partition.
   *
   * The row group size is left at the Parquet default. Tuning it matters for tables of a serious size; at the scale of
   * this example it would only add a knob without a visible effect.
   */
  def write(root: JPath, rows: Iterable[ClickRow]): Seq[WrittenPartitionFile] =
    Clickstream
      .groupByPartition(rows)
      .toSeq
      .sortBy { case (partition, _) => (partition.country, partition.day.toString) }
      .map { case (partition, partitionRows) => writePartition(root, partition, partitionRows) }

  private def writePartition(root: JPath, partition: HivePartition, rows: Seq[ClickRow]): WrittenPartitionFile = {
    val directory = root.resolve(partition.relativePath)
    Files.createDirectories(directory)
    val file = directory.resolve("clicks.parquet")

    val writer = AvroParquetWriter
      .builder[GenericRecord](HadoopOutputFile.fromPath(new Path(file.toUri), plainLocalFilesystem()))
      .withSchema(AvroSchema)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .build()

    try rows.foreach(row => writer.write(toAvro(row)))
    finally writer.close()

    WrittenPartitionFile(partition, file, rows.size)
  }

  private def toAvro(row: ClickRow): GenericRecord = {
    val record = new GenericData.Record(AvroSchema)
    record.put("customer_id", row.customerId)
    record.put("page", row.page)
    record.put("sku", row.sku.orNull)
    record.put("occurred_at", row.occurredAtEpochMillis)
    record
  }

  /**
   * Lists the relative paths of every file under `root`, using `/` as the separator on every operating system.
   *
   * The upload step needs these paths verbatim as object keys, and an object key in S3 always uses `/`.
   */
  def relativeFilePaths(root: JPath): Seq[String] = {
    val stream = Files.walk(root)
    try
      stream
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map(path => root.relativize(path).iterator().asScala.mkString("/"))
        .toVector
        .sorted
    finally stream.close()
  }
}
