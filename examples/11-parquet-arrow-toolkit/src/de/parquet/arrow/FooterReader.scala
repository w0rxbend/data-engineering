package de.parquet.arrow

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.statistics.{IntStatistics, LongStatistics, Statistics}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData}
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.nio.file.{Files, Path as JPath}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/**
 * Reads a Parquet file's footer and turns it into the plain `ParquetLayout` data of `FileLayout.scala`.
 *
 * Reading the footer is cheap and reads almost nothing: the reader seeks to the last eight bytes of the file, finds the
 * footer length and the `PAR1` magic marker, seeks back by that length and reads the metadata. It never touches a data
 * page. That is why a query engine can decide what to read before reading anything, and why this example can print the
 * shape of a hundred-megabyte archive instantly.
 */
object FooterReader {

  /** Opens `file`, reads its footer, and closes it again. */
  def read(file: JPath): ParquetLayout = {
    val configuration = ParquetArchiveWriter.localConfiguration()
    val inputFile     = HadoopInputFile.fromPath(new Path(file.toUri), configuration)

    Using.resource(ParquetFileReader.open(inputFile)) { reader =>
      val footer = reader.getFooter
      ParquetLayout(
        fileBytes = Files.size(file),
        createdBy = Option(footer.getFileMetaData.getCreatedBy).getOrElse("unknown"),
        schema = footer.getFileMetaData.getSchema.toString.trim,
        rowGroups = footer.getBlocks.asScala.toVector.zipWithIndex.map { case (block, ordinal) =>
          rowGroupLayout(block, ordinal)
        }
      )
    }
  }

  private def rowGroupLayout(block: BlockMetaData, ordinal: Int): RowGroupLayout =
    RowGroupLayout(
      ordinal = ordinal,
      rowCount = block.getRowCount,
      columns = block.getColumns.asScala.toVector.map(columnChunkLayout)
    )

  private def columnChunkLayout(chunk: ColumnChunkMetaData): ColumnChunkLayout = {
    val statistics = Option(chunk.getStatistics).filter(_.hasNonNullValue)
    ColumnChunkLayout(
      // `toDotString` renders a nested path such as `address.city`. This archive is flat,
      // so it yields the plain column name.
      column = chunk.getPath.toDotString,
      codec = chunk.getCodec.name,
      compressedBytes = chunk.getTotalSize,
      uncompressedBytes = chunk.getTotalUncompressedSize,
      valueCount = chunk.getValueCount,
      encodings = chunk.getEncodings.asScala.map(_.name).toSet,
      bounds = statistics.map(stats => (stats.minAsString, stats.maxAsString)),
      numericBounds = statistics.flatMap(numericBounds),
      nullCount = Option(chunk.getStatistics).filter(_.isNumNullsSet).map(_.getNumNulls)
    )
  }

  /**
   * Extracts the minimum and maximum of an integer column as numbers.
   *
   * Parquet's statistics are typed, and only the integer-typed ones can be compared numerically without knowing the
   * column's logical type. Text columns still expose their bounds as strings for display, but are not used for the
   * row-group skipping demonstration, which filters on a timestamp.
   */
  private def numericBounds(statistics: Statistics[?]): Option[(Long, Long)] =
    statistics match {
      case longs: LongStatistics => Some((longs.getMin, longs.getMax))
      case ints: IntStatistics   => Some((ints.getMin.toLong, ints.getMax.toLong))
      case _                     => None
    }
}
