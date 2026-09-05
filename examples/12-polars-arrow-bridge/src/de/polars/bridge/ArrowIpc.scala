package de.polars.bridge

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter}
import org.apache.arrow.vector.types.pojo.Schema

import java.io.{FileInputStream, FileOutputStream}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.util.Using

/**
 * Writes and reads Arrow Inter-Process Communication (IPC) files.
 *
 * IPC is Arrow's framing format: a small header, then the raw column buffers exactly as they sit in memory, then a
 * footer with the schema and the batch offsets. Because the buffers are stored in the same layout the in-memory format
 * uses, a reader can memory-map the file and start computing without parsing or converting anything. That "zero
 * re-serialisation" property is the whole point of this example.
 *
 * The IPC *file* format (used here) has a footer and therefore supports random access to individual batches; the IPC
 * *stream* format is the same batches without the footer, for sending over a socket. Polars reads the file format with
 * `scan_ipc`.
 */
object ArrowIpc {

  /** How many rows go into one Arrow record batch. */
  val DefaultBatchSize: Int = 4096

  private def utf8Bytes(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  /**
   * Writes the fact table to `target` as an Arrow IPC file.
   *
   * The rows are written in batches: a `VectorSchemaRoot` is a reusable set of column buffers, filled up to `batchSize`
   * rows, flushed, and then cleared for the next batch. Reusing it keeps the peak memory bounded no matter how many
   * rows are handed in.
   *
   * @return
   *   the number of rows written
   */
  def writeOrderLines(
      allocator: BufferAllocator,
      rows: Seq[OrderLineRow],
      target: Path,
      batchSize: Int = DefaultBatchSize
  ): Long =
    writeBatched(allocator, ArrowSchemas.orderLines, rows, target, batchSize)(fillOrderLines)

  /** Writes the small country-to-region dimension table to `target` as an Arrow IPC file. */
  def writeRegions(allocator: BufferAllocator, rows: Seq[RegionRow], target: Path): Long =
    writeBatched(allocator, ArrowSchemas.regions, rows, target, DefaultBatchSize)(fillRegions)

  /** Reads an Arrow IPC file written by [[writeOrderLines]] back into ordinary Scala objects. */
  def readOrderLines(allocator: BufferAllocator, source: Path): List[OrderLineRow] = {
    val collected = List.newBuilder[OrderLineRow]
    forEachBatch(allocator, source) { root =>
      val columns = OrderLineVectors(root)
      var row     = 0
      while (row < root.getRowCount) {
        collected += columns.rowAt(row)
        row += 1
      }
    }
    collected.result()
  }

  /**
   * Reads the aggregate table that the Polars script writes back for the JVM side.
   *
   * Unlike the writer above, this reader asks each column for a boxed value instead of casting the vector to a concrete
   * type. That is deliberate: a producer is free to pick any physical type that satisfies the logical one - Polars, for
   * instance, stores text as Arrow's "large string" variant with 64-bit offsets, which is a different Java class from
   * the 32-bit `VarCharVector` this module writes. Reading through the common interface makes the JVM side accept both,
   * at the cost of one boxed object per value. For a five-row summary that cost is irrelevant; for the
   * five-hundred-thousand-row fact table it would not be, which is why the hot path in [[OrderLineVectors]] casts.
   */
  def readRevenue(allocator: BufferAllocator, source: Path): List[RevenueByCountry] = {
    import ArrowSchemas.RevenueColumns as C
    val collected = List.newBuilder[RevenueByCountry]
    forEachBatch(allocator, source) { root =>
      def text(column: String, row: Int): String = String.valueOf(root.getVector(column).getObject(row))
      def number(column: String, row: Int): Long =
        root.getVector(column).getObject(row) match {
          case value: java.lang.Number => value.longValue()
          case other => throw new IllegalArgumentException(s"column $column holds $other, which is not a number")
        }

      var row = 0
      while (row < root.getRowCount) {
        collected += RevenueByCountry(
          country = text(C.country, row),
          region = text(C.region, row),
          orderCount = number(C.orderCount, row),
          units = number(C.units, row),
          revenueCents = number(C.revenueCents, row)
        )
        row += 1
      }
    }
    collected.result()
  }

  /**
   * Streams every record batch of an IPC file through `handle`.
   *
   * The `VectorSchemaRoot` handed to `handle` is only valid until the next batch is loaded, so the callback must copy
   * out whatever it wants to keep. That contract is what allows a file far larger than memory to be read one batch at a
   * time.
   */
  def forEachBatch(allocator: BufferAllocator, source: Path)(handle: VectorSchemaRoot => Unit): Unit =
    Using.resource(new FileInputStream(source.toFile)) { input =>
      Using.resource(new ArrowFileReader(input.getChannel, allocator)) { reader =>
        val root = reader.getVectorSchemaRoot
        while (reader.loadNextBatch())
          handle(root)
      }
    }

  private def writeBatched[A](
      allocator: BufferAllocator,
      schema: Schema,
      rows: Seq[A],
      target: Path,
      batchSize: Int
  )(fill: (VectorSchemaRoot, Seq[A]) => Unit): Long = {
    require(batchSize > 0, s"batchSize must be positive, was $batchSize")
    Option(target.getParent).foreach(Files.createDirectories(_))
    Using.resource(VectorSchemaRoot.create(schema, allocator)) { root =>
      Using.resource(new FileOutputStream(target.toFile)) { output =>
        Using.resource(new ArrowFileWriter(root, null, Channels.newChannel(output))) { writer =>
          writer.start()
          rows.grouped(batchSize).foreach { batch =>
            root.allocateNew()
            fill(root, batch)
            root.setRowCount(batch.size)
            writer.writeBatch()
            root.clear()
          }
          writer.end()
        }
      }
    }
    rows.size.toLong
  }

  private def fillOrderLines(root: VectorSchemaRoot, batch: Seq[OrderLineRow]): Unit = {
    val columns = OrderLineVectors(root)
    batch.iterator.zipWithIndex.foreach { case (row, index) =>
      columns.orderId.setSafe(index, utf8Bytes(row.orderId))
      columns.customerId.setSafe(index, utf8Bytes(row.customerId))
      columns.country.setSafe(index, utf8Bytes(row.country))
      columns.placedAt.setSafe(index, row.placedAtEpochMillis)
      columns.sku.setSafe(index, utf8Bytes(row.sku))
      columns.quantity.setSafe(index, row.quantity)
      columns.unitPrice.setSafe(index, row.unitPriceCents)
      columns.lineTotal.setSafe(index, row.lineTotalCents)
    }
  }

  private def fillRegions(root: VectorSchemaRoot, batch: Seq[RegionRow]): Unit = {
    val country = root.getVector(ArrowSchemas.RegionColumns.country).asInstanceOf[VarCharVector]
    val region  = root.getVector(ArrowSchemas.RegionColumns.region).asInstanceOf[VarCharVector]
    batch.iterator.zipWithIndex.foreach { case (row, index) =>
      country.setSafe(index, utf8Bytes(row.country))
      region.setSafe(index, utf8Bytes(row.region))
    }
  }
}

/**
 * The eight column buffers of the fact table, looked up once by name.
 *
 * Resolving a column by name costs a map lookup and a cast. Doing that once per batch instead of once per row is the
 * difference between "columnar" and "columnar in name only", and it is what makes the vectorised aggregation in
 * [[RevenueAggregation]] fast.
 */
final case class OrderLineVectors(
    orderId: VarCharVector,
    customerId: VarCharVector,
    country: VarCharVector,
    placedAt: TimeStampMilliVector,
    sku: VarCharVector,
    quantity: IntVector,
    unitPrice: BigIntVector,
    lineTotal: BigIntVector
) {

  /** Materialises a single row as an ordinary Scala object. */
  def rowAt(index: Int): OrderLineRow = {
    def text(vector: VarCharVector): String = new String(vector.get(index), StandardCharsets.UTF_8)
    OrderLineRow(
      orderId = text(orderId),
      customerId = text(customerId),
      country = text(country),
      placedAtEpochMillis = placedAt.get(index),
      sku = text(sku),
      quantity = quantity.get(index),
      unitPriceCents = unitPrice.get(index),
      lineTotalCents = lineTotal.get(index)
    )
  }
}

object OrderLineVectors {
  import ArrowSchemas.OrderLineColumns as C

  def apply(root: VectorSchemaRoot): OrderLineVectors =
    OrderLineVectors(
      orderId = root.getVector(C.orderId).asInstanceOf[VarCharVector],
      customerId = root.getVector(C.customerId).asInstanceOf[VarCharVector],
      country = root.getVector(C.country).asInstanceOf[VarCharVector],
      placedAt = root.getVector(C.placedAt).asInstanceOf[TimeStampMilliVector],
      sku = root.getVector(C.sku).asInstanceOf[VarCharVector],
      quantity = root.getVector(C.quantity).asInstanceOf[IntVector],
      unitPrice = root.getVector(C.unitPriceCents).asInstanceOf[BigIntVector],
      lineTotal = root.getVector(C.lineTotalCents).asInstanceOf[BigIntVector]
    )
}
