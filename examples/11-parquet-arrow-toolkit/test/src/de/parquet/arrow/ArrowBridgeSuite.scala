package de.parquet.arrow

import de.common.gen.DataGenerator

import java.nio.file.{Files, Path as JPath}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/**
 * The Arrow half of the example.
 *
 * Every test allocates off-heap memory and frees it again. `RootAllocator.close` throws if any buffer was leaked, so a
 * forgotten `close` in `ArrowBridge` fails these tests rather than quietly growing the process.
 */
class ArrowBridgeSuite extends munit.FunSuite {

  private val arrowDirectory = FunFixture[JPath](
    setup = _ => Files.createTempDirectory("de-11-arrow-"),
    teardown = directory =>
      Using.resource(Files.walk(directory)) { entries =>
        entries.iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists(_): Unit)
      }
  )

  private val rows: Vector[ArchiveRow] = OrderArchive.rowsFrom(new DataGenerator(seed = 11L).orders(500))

  test("the Arrow schema has the same columns, in the same order, as the Parquet schema") {
    assertEquals(ArrowBridge.schema.getFields.asScala.map(_.getName).toVector, OrderArchive.Columns)
  }

  test("a batch round-trips through Arrow vectors unchanged") {
    ArrowBridge.withBatch(rows) { root =>
      assertEquals(root.getRowCount, rows.size)
      assertEquals(ArrowBridge.toRows(root), rows)
    }
  }

  test("an empty batch is a valid batch") {
    ArrowBridge.withBatch(Vector.empty) { root =>
      assertEquals(root.getRowCount, 0)
      assertEquals(ArrowBridge.toRows(root), Vector.empty)
    }
  }

  test("summing a column straight from the buffer agrees with summing the rows") {
    ArrowBridge.withBatch(rows) { root =>
      assertEquals(ArrowBridge.sumColumn(root, "line_total_cents"), rows.map(_.lineTotalCents).sum)
      assertEquals(ArrowBridge.sumColumn(root, "unit_price_cents"), rows.map(_.unitPriceCents).sum)
    }
  }

  test("every column reports a buffer and a value count") {
    ArrowBridge.withBatch(rows) { root =>
      val footprint = ArrowBridge.footprint(root)

      assertEquals(footprint.map(_.column), OrderArchive.Columns)
      assert(footprint.forall(_.bufferBytes > 0L), s"every vector should hold memory: $footprint")
      assert(footprint.forall(_.valueCount == rows.size))
    }
  }

  arrowDirectory.test("an IPC file round-trips every row, split into the requested batches") { directory =>
    val file = directory.resolve("orders.arrow")

    val fileBytes = ArrowBridge.writeIpcFile(file, rows, batchSize = 128)
    val contents  = ArrowBridge.readIpcFile(file)

    assertEquals(contents.rows, rows)
    assertEquals(contents.batches, math.ceil(rows.size / 128.0).toInt)
    assertEquals(contents.fileBytes, fileBytes)
  }

  arrowDirectory.test("one batch large enough for everything produces a single record batch") { directory =>
    val file = directory.resolve("orders.arrow")
    ArrowBridge.writeIpcFile(file, rows, batchSize = rows.size * 2)

    assertEquals(ArrowBridge.readIpcFile(file).batches, 1)
  }

  arrowDirectory.test("an Arrow IPC file is larger than the equivalent Parquet file") { directory =>
    val arrowBytes   = ArrowBridge.writeIpcFile(directory.resolve("orders.arrow"), rows, batchSize = 1024)
    val parquetBytes = ParquetArchiveWriter.write(directory.resolve("orders.parquet"), rows).sizeBytes

    assert(
      arrowBytes > parquetBytes,
      s"Arrow is uncompressed by design, so $arrowBytes should exceed the Parquet $parquetBytes"
    )
  }

  arrowDirectory.test("a batch size of zero is rejected rather than looping forever") { directory =>
    intercept[IllegalArgumentException] {
      ArrowBridge.writeIpcFile(directory.resolve("orders.arrow"), rows, batchSize = 0)
    }
  }
}
