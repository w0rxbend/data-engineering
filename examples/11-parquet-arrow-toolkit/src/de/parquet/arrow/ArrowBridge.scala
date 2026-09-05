package de.parquet.arrow

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter, SeekableReadChannel}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.{BigIntVector, IntVector, VarCharVector, VectorSchemaRoot}

import java.io.{FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path as JPath}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** How much off-heap memory one Arrow column occupies. */
final case class VectorFootprint(column: String, bufferBytes: Long, valueCount: Int)

/** What came back out of an Arrow IPC file. */
final case class IpcContents(batches: Int, rows: Vector[ArchiveRow], fileBytes: Long)

/**
 * Apache Arrow: the same columns as the Parquet file, but laid out for a processor rather than for a disk.
 *
 * Parquet and Arrow are often confused because both are columnar and both come from the same community. The difference
 * is what they optimise for. Parquet is a *storage* format: it compresses, it encodes with dictionaries and run
 * lengths, and every value must be decoded before it can be used. Arrow is a *memory* format: values sit uncompressed
 * in flat buffers at fixed offsets, so a program can point at a buffer and read the ten-thousandth 64-bit integer by
 * arithmetic, with no decoding and no copying.
 *
 * That property is why Arrow is the interchange format between processes and languages. Two programs that agree on
 * Arrow can hand each other a memory region instead of serialising and parsing - which is exactly what example 12
 * builds on.
 */
object ArrowBridge {

  /**
   * The Arrow schema, mirroring the Parquet schema field for field.
   *
   * Every field is declared non-nullable. An Arrow vector always carries a validity buffer - one bit per value saying
   * whether it is null - but declaring the field non-nullable tells any consumer it never has to consult it.
   */
  val schema: Schema = new Schema(
    List(
      Field.notNullable("order_id", new ArrowType.Utf8),
      Field.notNullable("customer_id", new ArrowType.Utf8),
      Field.notNullable("country", new ArrowType.Utf8),
      Field.notNullable("placed_at", new ArrowType.Int(64, true)),
      Field.notNullable("sku", new ArrowType.Utf8),
      Field.notNullable("quantity", new ArrowType.Int(32, true)),
      Field.notNullable("unit_price_cents", new ArrowType.Int(64, true)),
      Field.notNullable("line_total_cents", new ArrowType.Int(64, true))
    ).asJava
  )

  /**
   * Allocates a `VectorSchemaRoot`, fills it with `rows`, hands it to `use`, and frees the memory afterwards.
   *
   * A `VectorSchemaRoot` is a schema plus one vector per field plus a row count: Arrow's name for a batch of records.
   * Its buffers live outside the Java heap, so they are freed explicitly rather than by the garbage collector. The
   * `RootAllocator` tracks every byte it hands out and throws on close if anything was leaked, which turns a memory
   * leak into a failing test rather than a slow crash in production.
   */
  def withBatch[A](rows: Seq[ArchiveRow])(use: VectorSchemaRoot => A): A =
    Using.resource(new RootAllocator(Long.MaxValue)) { allocator =>
      Using.resource(VectorSchemaRoot.create(schema, allocator)) { root =>
        populate(root, rows)
        use(root)
      }
    }

  /** Writes `rows` into an already allocated batch. */
  def populate(root: VectorSchemaRoot, rows: Seq[ArchiveRow]): Unit = {
    root.allocateNew()
    val orderId    = root.getVector("order_id").asInstanceOf[VarCharVector]
    val customerId = root.getVector("customer_id").asInstanceOf[VarCharVector]
    val country    = root.getVector("country").asInstanceOf[VarCharVector]
    val placedAt   = root.getVector("placed_at").asInstanceOf[BigIntVector]
    val sku        = root.getVector("sku").asInstanceOf[VarCharVector]
    val quantity   = root.getVector("quantity").asInstanceOf[IntVector]
    val unitPrice  = root.getVector("unit_price_cents").asInstanceOf[BigIntVector]
    val lineTotal  = root.getVector("line_total_cents").asInstanceOf[BigIntVector]

    rows.iterator.zipWithIndex.foreach { case (row, index) =>
      // `setSafe` grows the underlying buffer if the value does not fit. `set` would be
      // marginally faster but requires the caller to have sized every buffer correctly.
      orderId.setSafe(index, utf8(row.orderId))
      customerId.setSafe(index, utf8(row.customerId))
      country.setSafe(index, utf8(row.country))
      placedAt.setSafe(index, row.placedAtEpochMillis)
      sku.setSafe(index, utf8(row.sku))
      quantity.setSafe(index, row.quantity)
      unitPrice.setSafe(index, row.unitPriceCents)
      lineTotal.setSafe(index, row.lineTotalCents)
    }
    // The row count is separate from the vectors: it is what makes the batch a table
    // rather than eight independent arrays, and readers trust it over buffer sizes.
    root.setRowCount(rows.size)
  }

  /** Copies a batch back into ordinary Scala values. */
  def toRows(root: VectorSchemaRoot): Vector[ArchiveRow] = {
    val orderId    = root.getVector("order_id").asInstanceOf[VarCharVector]
    val customerId = root.getVector("customer_id").asInstanceOf[VarCharVector]
    val country    = root.getVector("country").asInstanceOf[VarCharVector]
    val placedAt   = root.getVector("placed_at").asInstanceOf[BigIntVector]
    val sku        = root.getVector("sku").asInstanceOf[VarCharVector]
    val quantity   = root.getVector("quantity").asInstanceOf[IntVector]
    val unitPrice  = root.getVector("unit_price_cents").asInstanceOf[BigIntVector]
    val lineTotal  = root.getVector("line_total_cents").asInstanceOf[BigIntVector]

    (0 until root.getRowCount).iterator.map { index =>
      ArchiveRow(
        orderId = text(orderId, index),
        customerId = text(customerId, index),
        country = text(country, index),
        placedAtEpochMillis = placedAt.get(index),
        sku = text(sku, index),
        quantity = quantity.get(index),
        unitPriceCents = unitPrice.get(index),
        lineTotalCents = lineTotal.get(index)
      )
    }.toVector
  }

  /**
   * Adds up one numeric column without materialising a single Scala object.
   *
   * This is what "zero copy" buys in practice. `BigIntVector.get` computes an address - the vector's data buffer plus
   * eight times the index - and reads eight bytes. There is no record to construct, no field to look up by name, and
   * nothing for the garbage collector to clean up afterwards. A column-at-a-time aggregation over an Arrow buffer is
   * the shape every vectorised query engine is built around.
   */
  def sumColumn(root: VectorSchemaRoot, column: String): Long = {
    val vector = root.getVector(column).asInstanceOf[BigIntVector]
    var total  = 0L
    var index  = 0
    while (index < root.getRowCount) {
      total += vector.get(index)
      index += 1
    }
    total
  }

  /** How much off-heap memory each column of the batch currently occupies. */
  def footprint(root: VectorSchemaRoot): Vector[VectorFootprint] =
    root.getFieldVectors.asScala.toVector.map { vector =>
      VectorFootprint(vector.getName, vector.getBufferSize.toLong, vector.getValueCount)
    }

  /**
   * Writes the rows to an Arrow IPC file, one record batch per `batchSize` rows.
   *
   * IPC stands for inter-process communication, and the name is the point: the file format is the *same* byte layout
   * that lives in memory, framed by a little metadata. Loading it is closer to a memory map than to parsing. That also
   * explains why these files are much larger than the equivalent Parquet file - they are uncompressed and unencoded on
   * purpose, because the goal is to be usable immediately rather than small on disk.
   */
  def writeIpcFile(target: JPath, rows: Seq[ArchiveRow], batchSize: Int): Long = {
    require(batchSize > 0, s"batchSize must be positive, got $batchSize")
    Using.resource(new RootAllocator(Long.MaxValue)) { allocator =>
      Using.resource(VectorSchemaRoot.create(schema, allocator)) { root =>
        Using.resource(new FileOutputStream(target.toFile)) { output =>
          Using.resource(new ArrowFileWriter(root, null, output.getChannel)) { writer =>
            writer.start()
            rows.grouped(batchSize).foreach { batch =>
              // Each batch reuses the same vectors: `populate` clears and refills them,
              // so peak memory stays proportional to one batch, not to the whole archive.
              populate(root, batch)
              writer.writeBatch()
            }
            writer.end()
          }
        }
      }
    }
    Files.size(target)
  }

  /** Reads an Arrow IPC file back, batch by batch. */
  def readIpcFile(source: JPath): IpcContents =
    Using.resource(new RootAllocator(Long.MaxValue)) { allocator =>
      Using.resource(new FileInputStream(source.toFile)) { input =>
        Using.resource(new ArrowFileReader(new SeekableReadChannel(input.getChannel), allocator)) { reader =>
          val root        = reader.getVectorSchemaRoot
          var batches     = 0
          val rowsBuilder = Vector.newBuilder[ArchiveRow]
          while (reader.loadNextBatch()) {
            batches += 1
            rowsBuilder ++= toRows(root)
          }
          IpcContents(batches, rowsBuilder.result(), Files.size(source))
        }
      }
    }

  private def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  private def text(vector: VarCharVector, index: Int): String =
    new String(vector.get(index), StandardCharsets.UTF_8)
}
