package de.parquet.arrow

/** One row of the codec comparison: how large the same data became under one set of write options. */
final case class CodecMeasurement(label: String, fileBytes: Long, dictionaryEncoded: Boolean) {

  /** How many times smaller this file is than `baseline`. Larger is better. */
  def savingAgainst(baseline: CodecMeasurement): Double =
    if (fileBytes <= 0) 0.0 else baseline.fileBytes.toDouble / fileBytes.toDouble
}

/**
 * Turns measurements into the text that lands on the console.
 *
 * Rendering lives apart from measuring so that the exact shape of the output is covered by fast unit tests rather than
 * discovered by running the program.
 */
object Reports {

  /** Renders a byte count in the largest unit that keeps the number readable, for example `1.40 MiB`. */
  def bytes(count: Long): String = {
    val units = Vector("B", "KiB", "MiB", "GiB", "TiB")
    val index =
      units.indices.takeWhile(i => count >= Math.pow(1024, (i + 1).toDouble)).lastOption.map(_ + 1).getOrElse(0)
    if (index == 0) s"$count B" else f"${count / Math.pow(1024, index.toDouble)}%.2f ${units(index)}"
  }

  /** Renders a ratio such as `4.7x`. */
  def factor(value: Double): String = f"$value%.1fx"

  /** Renders a percentage with one decimal, for example `12.5%`. */
  def percentage(fraction: Double): String = f"${fraction * 100}%.1f%%"

  /**
   * Lays out rows as a fixed-width table with a header, so columns line up in a terminal.
   *
   * Every cell is padded to the widest cell in its column. The first row is the header and is underlined.
   */
  def table(header: Seq[String], rows: Seq[Seq[String]]): String = {
    val all    = header +: rows
    val widths = header.indices.map(column => all.map(row => row.lift(column).getOrElse("").length).max)
    def line(cells: Seq[String]): String =
      cells.zipWithIndex.map { case (cell, column) => cell.padTo(widths(column), ' ') }.mkString("  ").stripTrailing
    (line(header) +: line(widths.map("-" * _)) +: rows.map(line)).mkString("\n")
  }

  /** The file's overall shape: how it was written, how much it holds, how it is sliced. */
  def fileSummary(layout: ParquetLayout): String = {
    val uncompressed = layout.rowGroups.flatMap(_.columns).map(_.uncompressedBytes).sum
    val ratio        = if (layout.dataBytes <= 0) 0.0 else uncompressed.toDouble / layout.dataBytes.toDouble
    Seq(
      s"file size        ${bytes(layout.fileBytes)}",
      s"written by       ${layout.createdBy}",
      s"rows             ${layout.rowCount}",
      s"row groups       ${layout.rowGroups.size}",
      s"columns          ${layout.columnNames.size} (${layout.columnNames.mkString(", ")})",
      s"codec            ${layout.codec}",
      s"data / decoded   ${bytes(layout.dataBytes)} / ${bytes(uncompressed)}  (${factor(ratio)} compression)"
    ).mkString("\n")
  }

  /** One line per row group: how many rows it holds, how large it is, and the time range its statistics record. */
  def rowGroups(layout: ParquetLayout, statisticsColumn: String): String =
    table(
      Seq("group", "rows", "size", s"$statisticsColumn min", s"$statisticsColumn max"),
      layout.rowGroups.map { group =>
        val bounds = group.column(statisticsColumn).flatMap(_.bounds)
        Seq(
          group.ordinal.toString,
          group.rowCount.toString,
          bytes(group.compressedBytes),
          bounds.map(_._1).getOrElse("-"),
          bounds.map(_._2).getOrElse("-")
        )
      }
    )

  /**
   * One line per column, summed over all row groups: its share of the file, its encodings and its value range.
   *
   * The share is the number that makes projection pushdown intuitive. A column holding two percent of the bytes is a
   * column a projected read gets almost for free.
   */
  def columns(layout: ParquetLayout): String = {
    val total = layout.dataBytes
    table(
      Seq("column", "size", "share", "encodings", "min", "max"),
      layout.columnNames.map { name =>
        val chunks    = layout.chunksOf(name)
        val size      = chunks.map(_.compressedBytes).sum
        val share     = if (total <= 0) 0.0 else size.toDouble / total.toDouble
        val encodings = chunks.flatMap(_.encodings).distinct.sorted.mkString(",")
        val bounds    = layout.bounds(name)
        Seq(
          name,
          bytes(size),
          percentage(share),
          encodings,
          bounds.map(_._1).getOrElse("-"),
          bounds.map(_._2).getOrElse("-")
        )
      }
    )
  }

  /** The three read strategies side by side. */
  def scanComparison(named: Seq[(String, ScanCost)]): String =
    table(
      Seq("read strategy", "row groups", "bytes to read", "vs full scan"),
      named.map { case (label, cost) =>
        val baseline = named.head._2.bytesRead
        val saving   = if (cost.bytesRead <= 0) None else Some(baseline.toDouble / cost.bytesRead.toDouble)
        Seq(
          label,
          s"${cost.rowGroupsRead} of ${cost.rowGroupsTotal}",
          bytes(cost.bytesRead),
          saving.map(factor).getOrElse("-")
        )
      }
    )

  /** The codec and dictionary comparison, measured against the first (uncompressed) measurement. */
  def codecComparison(measurements: Seq[CodecMeasurement]): String = {
    val baseline = measurements.head
    table(
      Seq("variant", "file size", "vs uncompressed", "dictionary"),
      measurements.map { measurement =>
        Seq(
          measurement.label,
          bytes(measurement.fileBytes),
          factor(measurement.savingAgainst(baseline)),
          if (measurement.dictionaryEncoded) "yes" else "no"
        )
      }
    )
  }

  /** How much off-heap memory each Arrow vector holds. */
  def arrowFootprint(vectors: Seq[VectorFootprint]): String =
    table(
      Seq("vector", "values", "buffer size"),
      vectors.map(vector => Seq(vector.column, vector.valueCount.toString, bytes(vector.bufferBytes)))
    )
}
