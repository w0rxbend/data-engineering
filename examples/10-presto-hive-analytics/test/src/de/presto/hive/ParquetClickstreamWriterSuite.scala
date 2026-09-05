package de.presto.hive

import de.common.domain.{ClickEvent, CustomerId, Sku}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.nio.file.{Files, Path as JPath}
import scala.util.Using

/**
 * Exercises the Parquet writer against a temporary directory.
 *
 * No container is involved: the writer targets the local filesystem, so the layout it produces and the records it
 * writes can both be checked in a plain unit test.
 */
final class ParquetClickstreamWriterSuite extends munit.FunSuite {

  private val stagingDirectory = FunFixture[JPath](
    setup = _ => Files.createTempDirectory("de-10-writer-test"),
    teardown = root => {
      val stream = Files.walk(root)
      try stream.toArray.map(_.asInstanceOf[JPath]).reverse.foreach(Files.deleteIfExists(_): Unit)
      finally stream.close()
    }
  )

  private def click(customer: String, page: String, atMillis: Long, sku: Option[String] = None): ClickEvent =
    ClickEvent(CustomerId(customer), page, sku.map(Sku.apply), atMillis)

  private def readBack(file: JPath): Vector[GenericRecord] =
    Using.resource(
      AvroParquetReader
        .builder[GenericRecord](HadoopInputFile.fromPath(new Path(file.toUri), new Configuration()))
        // Read the file as generic records rather than letting Avro look for a
        // compiled class named after the record.
        .withDataModel(GenericData.get())
        .build()
    ) { reader =>
      Iterator.continually(reader.read()).takeWhile(_ != null).toVector
    }

  stagingDirectory.test("one file is written per partition, in the Hive directory layout") { root =>
    val oneDay = 24L * 60 * 60 * 1000
    val rows   = Clickstream.rowsFrom(
      Seq(
        click("cust-0001", "/home", 1699920000000L),
        click("cust-0001", "/checkout", 1699920000000L + 60000L),
        click("cust-0001", "/home", 1699920000000L + oneDay)
      )
    )

    val written = ParquetClickstreamWriter.write(root, rows)

    assertEquals(written.size, 2, "two calendar days means two partitions")
    assertEquals(written.map(_.rowCount).sum, 3)
    written.foreach { file =>
      assert(Files.isRegularFile(file.file), s"${file.file} should exist")
      val relative = root.relativize(file.file).getParent.toString
      assertEquals(HivePartition.parse(relative), Some(file.partition))
    }
  }

  stagingDirectory.test("the written records carry the event fields and omit the partition columns") { root =>
    val rows = Clickstream.rowsFrom(
      Seq(
        click("cust-0007", "/product", 1699920000000L, Some("SKU-KETTLE")),
        click("cust-0007", "/home", 1699920001000L)
      )
    )

    val file    = ParquetClickstreamWriter.write(root, rows).head.file
    val records = readBack(file)

    assertEquals(records.size, 2)
    assertEquals(records.map(_.get("page").toString).sorted, Vector("/home", "/product"))
    val product = records.find(_.get("page").toString == "/product").get
    assertEquals(product.get("customer_id").toString, "cust-0007")
    assertEquals(product.get("sku").toString, "SKU-KETTLE")
    assertEquals(product.get("occurred_at").asInstanceOf[Long], 1699920000000L)
    assertEquals(Option(records.find(_.get("page").toString == "/home").get.get("sku")), None)
    val fieldNames = records.head.getSchema.getFields.toArray.map(_.toString)
    assert(!fieldNames.exists(_.contains("country")), "country belongs in the directory name, not in the file")
  }

  stagingDirectory.test("relative file paths use forward slashes so they can be used as object keys") { root =>
    val rows = Clickstream.rowsFrom(Seq(click("cust-0002", "/cart", 1699920000000L)))
    ParquetClickstreamWriter.write(root, rows)

    val paths = ParquetClickstreamWriter.relativeFilePaths(root)

    assertEquals(paths.size, 1)
    assert(paths.head.matches("country=[A-Z]{2}/dt=\\d{4}-\\d{2}-\\d{2}/clicks\\.parquet"), paths.head)
  }

  stagingDirectory.test("writing no rows writes no files") { root =>
    assertEquals(ParquetClickstreamWriter.write(root, Vector.empty), Seq.empty)
    assertEquals(ParquetClickstreamWriter.relativeFilePaths(root), Vector.empty)
  }
}
