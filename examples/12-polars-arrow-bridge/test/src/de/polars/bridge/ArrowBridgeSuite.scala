package de.polars.bridge

import de.common.gen.DataGenerator
import munit.FunSuite
import org.apache.arrow.memory.RootAllocator

import java.nio.file.{Files, Path}

/**
 * Tests for everything that happens on the JVM side of the bridge.
 *
 * None of them starts a container: the Arrow writer, the Arrow reader, the Parquet exporter and both aggregations all
 * work on ordinary local files, so `./mill __.test` passes on a machine with no Docker installed at all.
 */
final class ArrowBridgeSuite extends FunSuite {

  /**
   * One Arrow memory allocator per test, checked for leaks when the test ends.
   *
   * Arrow buffers live outside the Java heap, so the garbage collector will not clean them up. Closing a
   * `RootAllocator` that still owns memory throws, which turns "we forgot to close a vector" from a slow leak into a
   * failing test.
   */
  private val allocatorFixture = FunFixture[RootAllocator](
    setup = _ => new RootAllocator(),
    teardown = _.close()
  )

  private val temporaryDirectory = FunFixture[Path](
    setup = test => Files.createTempDirectory(s"arrow-bridge-${test.name.replaceAll("\\W", "-")}"),
    teardown = directory => deleteRecursively(directory)
  )

  private val fixture = FunFixture.map2(allocatorFixture, temporaryDirectory)

  private def deleteRecursively(directory: Path): Unit = {
    val walk = Files.walk(directory)
    try
      walk.sorted(java.util.Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
    finally
      walk.close()
  }

  private def sampleRows(orderCount: Int): List[OrderLineRow] =
    OrderLineRow.fromOrders(new DataGenerator().orders(orderCount))

  test("flattening an order produces one row per order line and repeats the order attributes") {
    val order = new DataGenerator().nextOrder()
    val rows  = OrderLineRow.fromOrder(order)

    assertEquals(rows.size, order.lines.size)
    assertEquals(rows.map(_.orderId).distinct, List(order.id.value))
    assertEquals(rows.map(_.lineTotalCents).sum, order.total.cents)
  }

  fixture.test("an Arrow IPC file round-trips every row unchanged") { case (allocator, directory) =>
    val written = sampleRows(orderCount = 500)
    val target  = directory.resolve("order_lines.arrow")

    val count = ArrowIpc.writeOrderLines(allocator, written, target, batchSize = 64)
    val read  = ArrowIpc.readOrderLines(allocator, target)

    assertEquals(count, written.size.toLong)
    assertEquals(read, written)
  }

  fixture.test("an empty input still produces a readable Arrow file") { case (allocator, directory) =>
    val target = directory.resolve("empty.arrow")

    ArrowIpc.writeOrderLines(allocator, Seq.empty, target)

    assertEquals(ArrowIpc.readOrderLines(allocator, target), Nil)
  }

  fixture.test("the dimension table round-trips") { case (allocator, directory) =>
    val target = directory.resolve("regions.arrow")

    ArrowIpc.writeRegions(allocator, RegionRow.all, target)

    val roots = List.newBuilder[List[String]]
    ArrowIpc.forEachBatch(allocator, target) { root =>
      roots += List.tabulate(root.getRowCount)(row => root.getVector("region").getObject(row).toString)
    }
    assertEquals(roots.result().flatten, RegionRow.all.map(_.region))
  }

  fixture.test("both aggregations agree, whatever the batch boundaries are") { case (allocator, directory) =>
    val rows   = sampleRows(orderCount = 2000)
    val target = directory.resolve("order_lines.arrow")
    ArrowIpc.writeOrderLines(allocator, rows, target, batchSize = 97)

    val fromRows    = RevenueAggregation.fromRows(rows)
    val fromVectors = RevenueAggregation.fromArrowFile(allocator, target)

    assertEquals(fromVectors, fromRows)
  }

  test("the aggregation reproduces totals that can be checked by hand") {
    val rows = List(
      OrderLineRow("order-1", "cust-1", "DE", 1700000000000L, "SKU-MUG", 2, 500L, 1000L),
      OrderLineRow("order-1", "cust-1", "DE", 1700000000000L, "SKU-KETTLE", 1, 2500L, 2500L),
      OrderLineRow("order-2", "cust-2", "DE", 1700000001000L, "SKU-MUG", 3, 500L, 1500L),
      OrderLineRow("order-3", "cust-3", "PL", 1700000002000L, "SKU-FILTER", 1, 300L, 300L)
    )

    val aggregated = RevenueAggregation.fromRows(rows)

    assertEquals(
      aggregated,
      List(
        RevenueByCountry("DE", "DACH", orderCount = 2, units = 6, revenueCents = 5000),
        RevenueByCountry("PL", "CEE", orderCount = 1, units = 1, revenueCents = 300)
      )
    )
  }

  test("a country the dimension table does not list is reported as UNKNOWN rather than dropped") {
    val rows = List(OrderLineRow("order-9", "cust-9", "ZZ", 1700000000000L, "SKU-MUG", 1, 100L, 100L))

    assertEquals(RevenueAggregation.fromRows(rows).map(_.region), List("UNKNOWN"))
  }

  fixture.test("the Parquet export is readable back through Arrow and keeps every row") { case (allocator, directory) =>
    val rows       = sampleRows(orderCount = 300)
    val arrowFile  = directory.resolve("order_lines.arrow")
    val parquetDir = directory.resolve("parquet")
    ArrowIpc.writeOrderLines(allocator, rows, arrowFile)

    val produced = ParquetExport.fromArrowIpc(allocator, arrowFile, parquetDir)

    assert(produced.nonEmpty, "the dataset writer produced no Parquet file")
    assert(produced.forall(file => Files.size(file) > 0L), "a Parquet file is empty")
    assertEquals(ParquetExport.parquetFilesIn(parquetDir), produced)
  }

  allocatorFixture.test("an aggregate file produced by Polars itself is readable from the JVM") { allocator =>
    // `test/resources/polars_revenue.arrow` is a real file written by the container in `docker/`, checked in so that
    // this test can prove cross-language compatibility without starting anything. It matters because Polars encodes
    // text as Arrow's 64-bit-offset "large string" type, which is not the type this module writes.
    val fixtureFile = Path.of(getClass.getResource("/polars_revenue.arrow").toURI)

    val rows = ArrowIpc.readRevenue(allocator, fixtureFile)

    assertEquals(rows.map(_.country), List("DE", "ES", "FR", "PL", "UA"))
    assertEquals(rows.map(_.region), List("DACH", "WEST", "WEST", "CEE", "CEE"))
    assert(rows.forall(_.revenueCents > 0L), rows.toString)
  }

  test("the report states agreement when both sides produced the same rows") {
    val rows = List(RevenueByCountry("DE", "DACH", 2, 6, 5000))

    val message = RevenueReport.agreement("left", rows, "right", rows)

    assert(message.contains("agree on all 1 rows"), message)
  }

  test("the report names the country the two sides disagree on") {
    val left  = List(RevenueByCountry("DE", "DACH", 2, 6, 5000))
    val right = List(RevenueByCountry("DE", "DACH", 2, 6, 4999))

    val message = RevenueReport.agreement("left", left, "right", right)

    assert(message.contains("disagree"), message)
    assert(message.contains("DE"), message)
  }

  test("the timing report marks the fastest measurement as one times itself") {
    val message = RevenueReport.timings(Seq("slow" -> 20.0, "fast" -> 10.0))

    assert(message.contains("1.00x"), message)
    assert(message.contains("2.00x"), message)
  }

  test("the benchmark helper returns the value the work produced") {
    val measured = Stopwatch.measure("addition", warmups = 1, repetitions = 2)(1 + 1)

    assertEquals(measured.value, 2)
    assert(measured.bestMillis >= 0.0)
  }
}
