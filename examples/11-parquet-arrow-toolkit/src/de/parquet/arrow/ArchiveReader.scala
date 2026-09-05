package de.parquet.arrow

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.nio.file.Path as JPath
import scala.util.Using

/** A result together with how many bytes the local filesystem actually handed to the process while producing it. */
final case class Measured[A](value: A, bytesRead: Long)

/**
 * The three ways of reading the archive that this example compares.
 *
 * All three produce correct answers. They differ only in how much of the file they have to touch, and that difference
 * is the entire argument for a columnar format.
 */
object ArchiveReader {

  /** Reads every column of every row. The baseline. */
  def readAll(file: JPath): Measured[Vector[ArchiveRow]] =
    measuring {
      collect(openReader(file, ParquetArchiveWriter.localConfiguration()))(ArchiveSchema.toRow)
    }

  /**
   * Projection pushdown: reads only the named columns.
   *
   * `AvroReadSupport.setRequestedProjection` puts a reduced schema into the Hadoop configuration. The Parquet reader
   * consults it while planning and never opens the byte ranges of the other column chunks at all - it does not read and
   * discard them. Each record therefore arrives with only the requested fields populated, which is why the caller
   * supplies its own `extract` function instead of getting an `ArchiveRow` back.
   */
  def readProjected[A](file: JPath, columns: Set[String])(extract: GenericRecord => A): Measured[Vector[A]] = {
    val configuration = ParquetArchiveWriter.localConfiguration()
    AvroReadSupport.setRequestedProjection(configuration, ArchiveSchema.projection(columns))
    measuring(collect(openReader(file, configuration))(extract))
  }

  /**
   * Predicate pushdown: reads only the row groups whose recorded statistics allow a match, then drops the rows inside
   * them that do not match.
   *
   * Two levels of filtering happen here, and they are worth telling apart. Skipping a whole row group is decided from
   * the footer, before any data is read, and is where the saved bytes come from. Discarding individual non-matching
   * rows inside a surviving row group happens after decoding and saves no input/output; it only saves the caller from
   * writing the `filter` itself.
   */
  def readFiltered(file: JPath, column: String, lowerBound: Long, upperBound: Long): Measured[Vector[ArchiveRow]] = {
    val configuration = ParquetArchiveWriter.localConfiguration()
    val reader        = AvroParquetReader
      .builder[GenericRecord](HadoopInputFile.fromPath(new Path(file.toUri), configuration))
      .withDataModel(GenericData.get())
      .withConf(configuration)
      .withFilter(FilterCompat.get(between(column, lowerBound, upperBound)))
      .build()
    measuring(collect(reader)(ArchiveSchema.toRow))
  }

  /** The predicate `column >= lowerBound AND column <= upperBound`, in Parquet's own filter language. */
  def between(column: String, lowerBound: Long, upperBound: Long): FilterPredicate = {
    val target = FilterApi.longColumn(column)
    FilterApi.and(
      FilterApi.gtEq(target, java.lang.Long.valueOf(lowerBound)),
      FilterApi.ltEq(target, java.lang.Long.valueOf(upperBound))
    )
  }

  private def openReader(file: JPath, configuration: Configuration): ParquetReader[GenericRecord] =
    AvroParquetReader
      .builder[GenericRecord](HadoopInputFile.fromPath(new Path(file.toUri), configuration))
      // Without an explicit data model, Avro would look for a generated Java class named
      // after the record and fall back only after failing to find one.
      .withDataModel(GenericData.get())
      .withConf(configuration)
      .build()

  /**
   * Drains a reader into a vector, closing it afterwards. `ParquetReader.read` returns `null` at the end of the file.
   */
  private def collect[A](reader: ParquetReader[GenericRecord])(extract: GenericRecord => A): Vector[A] =
    Using.resource(reader) { open =>
      Iterator
        .continually(open.read())
        .takeWhile(_ != null)
        .map(extract)
        .toVector
    }

  /**
   * Runs `body` and reports how many bytes Hadoop's local filesystem read while it ran.
   *
   * Hadoop keeps a running byte counter per filesystem scheme, so the measurement is a difference of two readings
   * rather than an instrumented reader. It is process-wide, which makes it an honest number for a single-threaded
   * program such as `Main` and an unreliable one for parallel tests - which is why the tests assert on the footer
   * arithmetic in `ScanPlanner` instead.
   */
  def measuring[A](body: => A): Measured[A] = {
    val before = localBytesRead()
    val value  = body
    Measured(value, localBytesRead() - before)
  }

  private def localBytesRead(): Long =
    Option(FileSystem.getGlobalStorageStatistics.get("file"))
      .flatMap(statistics => Option(statistics.getLong("bytesRead")))
      .fold(0L)(_.longValue)
}
