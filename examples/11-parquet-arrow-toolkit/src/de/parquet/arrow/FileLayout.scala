package de.parquet.arrow

/**
 * A plain-data description of the inside of a Parquet file, and the pure calculations that can be made from it.
 *
 * Nothing in this file touches a disk or a library. `FooterReader` produces these values from a real file; keeping the
 * shapes and the arithmetic separate means the interesting reasoning - how many bytes must a scan read, which row
 * groups can be skipped - is covered by fast unit tests that build a layout by hand.
 */

/**
 * One column of one row group, which Parquet calls a *column chunk*.
 *
 * A column chunk is the unit of storage that makes a columnar format columnar: all values of `sku` for one horizontal
 * slice of the table sit next to each other in one contiguous byte range, so a reader that wants `sku` and nothing else
 * seeks to that range and reads only it.
 *
 * @param compressedBytes
 *   how many bytes the chunk occupies in the file.
 * @param uncompressedBytes
 *   how many bytes its pages take once decompressed.
 * @param encodings
 *   the Parquet encodings used inside the chunk, for example `PLAIN` or `RLE_DICTIONARY`.
 * @param bounds
 *   the smallest and largest value in the chunk, rendered as text, when Parquet recorded statistics for it.
 * @param numericBounds
 *   the same statistics as numbers, present only for integer columns. Row-group skipping needs numbers, not text.
 * @param nullCount
 *   how many of the chunk's values are null, when recorded.
 */
final case class ColumnChunkLayout(
    column: String,
    codec: String,
    compressedBytes: Long,
    uncompressedBytes: Long,
    valueCount: Long,
    encodings: Set[String],
    bounds: Option[(String, String)],
    numericBounds: Option[(Long, Long)],
    nullCount: Option[Long]
) {

  /**
   * Whether this chunk is dictionary encoded.
   *
   * Dictionary encoding replaces each repeated value with a small integer pointing into a per-chunk dictionary of the
   * distinct values. A column such as `country`, which holds five distinct strings across thousands of rows, shrinks to
   * a five-entry dictionary plus a run of tiny integers. `RLE_DICTIONARY` is the modern spelling; `PLAIN_DICTIONARY` is
   * the pre-2.0 one, still emitted by some writers.
   */
  def usesDictionary: Boolean = encodings.contains("RLE_DICTIONARY") || encodings.contains("PLAIN_DICTIONARY")

  /** How many times smaller the chunk is on disk than in memory. `1.0` means compression achieved nothing. */
  def compressionRatio: Double =
    if (compressedBytes <= 0) 0.0 else uncompressedBytes.toDouble / compressedBytes.toDouble

  /** Whether the chunk's recorded value range can contain any value in `[lowerBound, upperBound]`. */
  def mightContain(lowerBound: Long, upperBound: Long): Boolean =
    numericBounds match {
      case Some((minimum, maximum)) => maximum >= lowerBound && minimum <= upperBound
      case None                     => true // no statistics means no basis for skipping
    }
}

/**
 * One *row group*: a horizontal slice of the table, holding one column chunk per column.
 *
 * A row group is the unit of parallelism and of skipping. Parquet writes a row group once its buffered data reaches the
 * configured size, then starts a new one, and finally records every row group's location and statistics in the footer.
 */
final case class RowGroupLayout(ordinal: Int, rowCount: Long, columns: Vector[ColumnChunkLayout]) {

  /** Total size of this row group in the file. */
  def compressedBytes: Long = columns.map(_.compressedBytes).sum

  def column(name: String): Option[ColumnChunkLayout] = columns.find(_.column == name)

  /** Size of the named columns alone, which is what a projected read of this row group costs. */
  def compressedBytesOf(names: Set[String]): Long =
    columns.iterator.filter(chunk => names.contains(chunk.column)).map(_.compressedBytes).sum
}

/**
 * The whole file as its footer describes it.
 *
 * The footer sits at the *end* of a Parquet file, after all the data. That ordering is deliberate: a writer streaming
 * rows does not know how large its row groups will be until it has written them, so the index can only be written last.
 * A reader therefore starts by seeking to the end of the file, reading the four-byte footer length and the `PAR1` magic
 * bytes, and only then knows where everything else is.
 *
 * @param createdBy
 *   the writer's self-identification, for example `parquet-mr version 1.15.2`.
 * @param schema
 *   the schema as Parquet's own textual notation renders it.
 */
final case class ParquetLayout(
    fileBytes: Long,
    createdBy: String,
    schema: String,
    rowGroups: Vector[RowGroupLayout]
) {

  def rowCount: Long = rowGroups.map(_.rowCount).sum

  def columnNames: Vector[String] = rowGroups.headOption.map(_.columns.map(_.column)).getOrElse(Vector.empty)

  /** The compression codec of the file, taken from its first column chunk. */
  def codec: String = rowGroups.headOption.flatMap(_.columns.headOption).map(_.codec).getOrElse("UNKNOWN")

  /** Every column chunk of a given column, one per row group. */
  def chunksOf(column: String): Vector[ColumnChunkLayout] = rowGroups.flatMap(_.column(column))

  /** How many bytes the named column occupies across the whole file. */
  def compressedBytesOf(column: String): Long = chunksOf(column).map(_.compressedBytes).sum

  /**
   * How many bytes the file's data pages occupy in total. Slightly less than `fileBytes`, which includes the footer.
   */
  def dataBytes: Long = rowGroups.map(_.compressedBytes).sum

  /**
   * The smallest and largest value of a column across the whole file, rendered as text.
   *
   * Integer columns are compared as numbers and text columns lexicographically. Doing it the other way round for
   * numbers would report a maximum of `9489` as smaller than `509`, because `"5"` sorts after `"9"` - the same trap
   * that makes string-typed numeric columns a recurring source of wrong query results.
   */
  def bounds(column: String): Option[(String, String)] = {
    val chunks  = chunksOf(column)
    val numeric = chunks.flatMap(_.numericBounds)
    if (chunks.nonEmpty && numeric.sizeIs == chunks.size) {
      Some((numeric.map(_._1).min.toString, numeric.map(_._2).max.toString))
    } else {
      val textual = chunks.flatMap(_.bounds)
      Option.when(textual.nonEmpty)((textual.map(_._1).min, textual.map(_._2).max))
    }
  }

  /** Whether every chunk of the named column is dictionary encoded. */
  def isDictionaryEncoded(column: String): Boolean = {
    val chunks = chunksOf(column)
    chunks.nonEmpty && chunks.forall(_.usesDictionary)
  }
}

/** What a particular way of reading the file costs, expressed in bytes and row groups. */
final case class ScanCost(rowGroupsRead: Int, rowGroupsSkipped: Int, bytesRead: Long) {

  def rowGroupsTotal: Int = rowGroupsRead + rowGroupsSkipped
}

/**
 * Works out, from footer metadata alone, how much of a file each kind of read has to touch.
 *
 * This is the arithmetic that turns "pushdown is faster" into a number a reader can check. It mirrors what the Parquet
 * reader does internally: pick the row groups whose statistics allow a match, then read only the column chunks the
 * query actually names.
 */
object ScanPlanner {

  /** A read with no pushdown at all: every column of every row group. */
  def fullScan(layout: ParquetLayout): ScanCost =
    ScanCost(rowGroupsRead = layout.rowGroups.size, rowGroupsSkipped = 0, bytesRead = layout.dataBytes)

  /**
   * Projection pushdown: all row groups, but only the named columns.
   *
   * "Pushdown" means the restriction is handed down to the storage layer instead of being applied after the fact. A
   * reader without it would decode all eight columns and then throw six away.
   */
  def projectedScan(layout: ParquetLayout, columns: Set[String]): ScanCost =
    ScanCost(
      rowGroupsRead = layout.rowGroups.size,
      rowGroupsSkipped = 0,
      bytesRead = layout.rowGroups.map(_.compressedBytesOf(columns)).sum
    )

  /**
   * The row groups whose statistics for `column` allow a value inside `[lowerBound, upperBound]`.
   *
   * A row group is skipped only when its recorded range provably excludes the range asked for. Statistics can never
   * cause a match to be missed; at worst they fail to exclude a row group that turns out to hold nothing useful, which
   * costs time but not correctness.
   */
  def survivingRowGroups(
      layout: ParquetLayout,
      column: String,
      lowerBound: Long,
      upperBound: Long
  ): Vector[RowGroupLayout] =
    layout.rowGroups.filter(_.column(column).forall(_.mightContain(lowerBound, upperBound)))

  /** Predicate pushdown, optionally combined with a projection: only the surviving row groups, only those columns. */
  def filteredScan(
      layout: ParquetLayout,
      column: String,
      lowerBound: Long,
      upperBound: Long,
      columns: Set[String]
  ): ScanCost = {
    val surviving = survivingRowGroups(layout, column, lowerBound, upperBound)
    ScanCost(
      rowGroupsRead = surviving.size,
      rowGroupsSkipped = layout.rowGroups.size - surviving.size,
      bytesRead = surviving.map(_.compressedBytesOf(columns)).sum
    )
  }
}
