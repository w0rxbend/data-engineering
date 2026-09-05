package de.parquet.arrow

import de.common.gen.DataGenerator
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import java.nio.file.{Files, Path as JPath}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/**
 * The Parquet half of the example, exercised end to end against a temporary directory.
 *
 * No container is involved: the Parquet library writes and reads ordinary local files, which is exactly why this
 * example can be tested this thoroughly.
 */
class ParquetArchiveSuite extends munit.FunSuite {

  /** A directory created before each test and deleted afterwards, however the test ends. */
  private val archiveDirectory = FunFixture[JPath](
    setup = _ => Files.createTempDirectory("de-11-parquet-"),
    teardown = directory => deleteRecursively(directory)
  )

  /**
   * Removes a directory and everything in it. Deepest entries first, because a directory must be empty to be deleted.
   */
  private def deleteRecursively(directory: JPath): Unit =
    Using.resource(Files.walk(directory)) { entries =>
      entries.iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists(_): Unit)
    }

  /** Enough orders to fill several row groups at a deliberately small row-group size. */
  private val rows: Vector[ArchiveRow] = OrderArchive.rowsFrom(new DataGenerator(seed = 11L).orders(4000))

  private val smallRowGroups = WriteOptions(rowGroupBytes = 32L * 1024L)

  archiveDirectory.test("every written row comes back unchanged and in the order it was written") { directory =>
    val file = ParquetArchiveWriter.write(directory.resolve("orders.parquet"), rows, smallRowGroups)

    assertEquals(file.rowCount, rows.size.toLong)
    assertEquals(ArchiveReader.readAll(file.path).value, rows)
  }

  archiveDirectory.test("the footer describes the file the writer produced") { directory =>
    val file   = ParquetArchiveWriter.write(directory.resolve("orders.parquet"), rows, smallRowGroups)
    val layout = FooterReader.read(file.path)

    assertEquals(layout.rowCount, rows.size.toLong)
    assertEquals(layout.fileBytes, file.sizeBytes)
    assertEquals(layout.columnNames, OrderArchive.Columns)
    assertEquals(layout.codec, "SNAPPY")
    assert(layout.createdBy.startsWith("parquet-mr"), s"unexpected writer: ${layout.createdBy}")
    assert(layout.rowGroups.sizeIs > 1, s"expected several row groups, got ${layout.rowGroups.size}")
  }

  archiveDirectory.test("sorted input gives every row group a distinct, ascending timestamp range") { directory =>
    val file   = ParquetArchiveWriter.write(directory.resolve("orders.parquet"), rows, smallRowGroups)
    val layout = FooterReader.read(file.path)
    val ranges = layout.chunksOf("placed_at").flatMap(_.numericBounds)

    assertEquals(ranges.size, layout.rowGroups.size)
    assert(ranges.forall { case (low, high) => low <= high })
    // Consecutive row groups may share a boundary value but must never move backwards.
    assert(
      ranges.zip(ranges.drop(1)).forall { case ((_, previousHigh), (nextLow, _)) => nextLow >= previousHigh },
      s"row group ranges are not ascending: $ranges"
    )
  }

  archiveDirectory.test("a projected read returns only the requested columns, and the same values") { directory =>
    val file = ParquetArchiveWriter.write(directory.resolve("orders.parquet"), rows, smallRowGroups)

    val projected = ArchiveReader
      .readProjected(file.path, OrderArchive.ProjectedColumns) { record =>
        (record.get("sku").toString, record.get("line_total_cents").asInstanceOf[Long])
      }
      .value

    assertEquals(projected.size, rows.size)
    assertEquals(projected, rows.map(row => (row.sku, row.lineTotalCents)))
  }

  archiveDirectory.test("a projected read costs a fraction of a full read, as the footer predicts") { directory =>
    val file   = ParquetArchiveWriter.write(directory.resolve("orders.parquet"), rows, smallRowGroups)
    val layout = FooterReader.read(file.path)

    val full      = ScanPlanner.fullScan(layout)
    val projected = ScanPlanner.projectedScan(layout, OrderArchive.ProjectedColumns)

    assertEquals(projected.rowGroupsRead, full.rowGroupsRead)
    assert(
      projected.bytesRead * 2 < full.bytesRead,
      s"reading two of eight columns should cost far less than half: ${projected.bytesRead} of ${full.bytesRead}"
    )
  }

  archiveDirectory.test("a predicate returns exactly the matching rows and skips most row groups") { directory =>
    val file   = ParquetArchiveWriter.write(directory.resolve("orders.parquet"), rows, smallRowGroups)
    val layout = FooterReader.read(file.path)

    val timestamps = rows.map(_.placedAtEpochMillis)
    val earliest   = timestamps.min
    val span       = timestamps.max - earliest
    val lowerBound = earliest + (span * 2) / 5
    val upperBound = earliest + (span * 3) / 5

    val matching = rows.filter(row => row.placedAtEpochMillis >= lowerBound && row.placedAtEpochMillis <= upperBound)
    val returned = ArchiveReader.readFiltered(file.path, "placed_at", lowerBound, upperBound).value

    assertEquals(returned, matching)
    assert(matching.nonEmpty, "the test window should not be empty")

    val cost = ScanPlanner.filteredScan(layout, "placed_at", lowerBound, upperBound, layout.columnNames.toSet)
    assert(cost.rowGroupsSkipped > cost.rowGroupsRead, s"expected most row groups to be skipped, got $cost")
  }

  archiveDirectory.test("a predicate that matches nothing returns nothing") { directory =>
    val file = ParquetArchiveWriter.write(directory.resolve("orders.parquet"), rows, smallRowGroups)

    assertEquals(ArchiveReader.readFiltered(file.path, "placed_at", 0L, 1L).value, Vector.empty)
  }

  archiveDirectory.test("zstd compresses better than snappy, and both beat no compression") { directory =>
    val written = ParquetArchiveWriter.writeVariants(
      directory,
      "orders",
      rows,
      Seq(
        smallRowGroups.copy(codec = CompressionCodecName.UNCOMPRESSED),
        smallRowGroups.copy(codec = CompressionCodecName.SNAPPY),
        smallRowGroups.copy(codec = CompressionCodecName.ZSTD)
      )
    )
    val Vector(uncompressed, snappy, zstd) = written.map(_.sizeBytes).toVector: @unchecked

    assert(snappy < uncompressed, s"snappy $snappy should beat uncompressed $uncompressed")
    assert(zstd < snappy, s"zstd $zstd should beat snappy $snappy")
    assertEquals(written.map(_.path.getFileName.toString).toSet.size, 3)
  }

  archiveDirectory.test("dictionary encoding shrinks the low-cardinality columns") { directory =>
    val Vector(withDictionary, withoutDictionary) = ParquetArchiveWriter
      .writeVariants(
        directory,
        "orders",
        rows,
        Seq(smallRowGroups, smallRowGroups.copy(dictionaryEncoding = false))
      )
      .toVector: @unchecked

    val encoded = FooterReader.read(withDictionary.path)
    val plain   = FooterReader.read(withoutDictionary.path)

    assert(encoded.isDictionaryEncoded("country"), "country repeats five values and should be dictionary encoded")
    assert(!plain.isDictionaryEncoded("country"))
    assert(
      encoded.compressedBytesOf("country") < plain.compressedBytesOf("country"),
      "a dictionary should make the country column smaller"
    )
  }

  archiveDirectory.test("writing to an existing path overwrites it rather than failing") { directory =>
    val target = directory.resolve("orders.parquet")
    ParquetArchiveWriter.write(target, rows.take(100), smallRowGroups)
    val second = ParquetArchiveWriter.write(target, rows.take(10), smallRowGroups)

    assertEquals(second.rowCount, 10L)
    assertEquals(FooterReader.read(target).rowCount, 10L)
  }
}
