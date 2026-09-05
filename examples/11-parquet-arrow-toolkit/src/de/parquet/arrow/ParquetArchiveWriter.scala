package de.parquet.arrow

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile

import java.nio.file.Path as JPath
import scala.util.Using

/**
 * How a Parquet file should be written.
 *
 * @param codec
 *   the compression codec applied to every page. `SNAPPY` is fast and moderate; `ZSTD` (Zstandard) is slower to write
 *   and noticeably smaller; `UNCOMPRESSED` is the baseline the other two are measured against.
 * @param rowGroupBytes
 *   how much data the writer buffers before closing a row group and starting a new one. This example uses a much
 *   smaller value than the 128 MiB default so that a few thousand rows still produce several row groups, which is what
 *   makes row-group skipping observable at demonstration scale. On a real archive, small row groups are a mistake: they
 *   multiply footer metadata and shrink the runs of values that compression feeds on.
 * @param dictionaryEncoding
 *   whether the writer may replace repeated values with dictionary indexes. Turning it off is only ever useful for
 *   showing what it was doing.
 */
final case class WriteOptions(
    codec: CompressionCodecName = CompressionCodecName.SNAPPY,
    rowGroupBytes: Long = WriteOptions.DemonstrationRowGroupBytes,
    dictionaryEncoding: Boolean = true
) {

  /** A file name that says how the file was written, for example `orders-zstd-nodict.parquet`. */
  def fileName(base: String): String = {
    val dictionarySuffix = if (dictionaryEncoding) "" else "-nodict"
    s"$base-${codec.name.toLowerCase}$dictionarySuffix.parquet"
  }
}

object WriteOptions {

  /** 256 KiB: small enough that the sample archive spans several row groups, large enough to stay a readable shape. */
  val DemonstrationRowGroupBytes: Long = 256L * 1024L
}

/** Where a file was written and how large it turned out. */
final case class WrittenFile(path: JPath, sizeBytes: Long, rowCount: Long, options: WriteOptions)

/** Writes archive rows to a local Apache Parquet file. */
object ParquetArchiveWriter {

  /**
   * A Hadoop configuration that writes plain files.
   *
   * Hadoop's default local filesystem writes a hidden `.crc` checksum file next to every file it creates. Those sidecar
   * files are invisible to `ls`, count towards directory sizes, and get copied to object storage along with the data.
   * `RawLocalFileSystem` is the same filesystem without them.
   */
  def localConfiguration(): Configuration = {
    val configuration = new Configuration()
    configuration.set("fs.file.impl", classOf[RawLocalFileSystem].getName)
    configuration
  }

  /**
   * Writes every row to `target`, overwriting any file already there, and reports what was produced.
   *
   * The writer is closed before the file size is read: Parquet only writes the footer - and therefore only reaches its
   * final size - on close.
   */
  def write(target: JPath, rows: Iterable[ArchiveRow], options: WriteOptions = WriteOptions()): WrittenFile = {
    val configuration = localConfiguration()
    val outputFile    = HadoopOutputFile.fromPath(new Path(target.toUri), configuration)

    val builder = AvroParquetWriter
      .builder[GenericRecord](outputFile)
      .withSchema(ArchiveSchema.avro)
      .withConf(configuration)
      .withCompressionCodec(options.codec)
      .withRowGroupSize(options.rowGroupBytes)
      .withDictionaryEncoding(options.dictionaryEncoding)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)

    var written = 0L
    Using.resource(builder.build()) { writer =>
      rows.foreach { row =>
        writer.write(ArchiveSchema.toRecord(row))
        written += 1L
      }
    }

    WrittenFile(target, java.nio.file.Files.size(target), written, options)
  }

  /**
   * Writes the same rows once per set of options, into `directory`, and returns the results in the order given.
   *
   * This is what the codec comparison uses: identical input, identical row-group size, one variable changed at a time.
   */
  def writeVariants(
      directory: JPath,
      base: String,
      rows: Iterable[ArchiveRow],
      variants: Seq[WriteOptions]
  ): Seq[WrittenFile] =
    variants.map(options => write(directory.resolve(options.fileName(base)), rows, options))
}
