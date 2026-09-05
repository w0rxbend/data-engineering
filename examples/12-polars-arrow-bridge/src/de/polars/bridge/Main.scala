package de.polars.bridge

import de.common.gen.DataGenerator
import org.apache.arrow.memory.RootAllocator

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.util.{Try, Using}

/**
 * Wires the pieces together: generate orders, write Arrow and Parquet, aggregate twice on the JVM, and - if the Polars
 * container has already run - show what Polars made of the very same bytes.
 *
 * Everything this object does is input/output and printing. The logic worth testing lives in [[ArrowIpc]],
 * [[RevenueAggregation]] and [[RevenueReport]], all of which run without any container.
 *
 * Command line arguments, both optional:
 *   1. the directory to exchange files in (default `examples/12-polars-arrow-bridge/data`)
 *   1. how many orders to generate (default 200000)
 */
object Main {

  private val DefaultDataDirectory = "examples/12-polars-arrow-bridge/data"
  private val DefaultOrderCount    = 200000

  def main(args: Array[String]): Unit = {
    val dataSet    = DataSet(Paths.get(args.headOption.getOrElse(DefaultDataDirectory)).toAbsolutePath)
    val orderCount = args.lift(1).flatMap(argument => Try(argument.toInt).toOption).getOrElse(DefaultOrderCount)

    Files.createDirectories(dataSet.root)
    Using.resource(new RootAllocator()) { allocator =>
      val rows = generateRows(orderCount)
      writeExchangeFiles(allocator, dataSet, rows)
      aggregateOnTheJvm(allocator, dataSet, rows)
      reportPolars(allocator, dataSet)
    }
  }

  private def generateRows(orderCount: Int): List[OrderLineRow] = {
    val generator = new DataGenerator()
    val rows      = OrderLineRow.fromOrders(generator.orders(orderCount))
    println(s"generated $orderCount orders -> ${rows.size} order lines")
    rows
  }

  private def writeExchangeFiles(allocator: RootAllocator, dataSet: DataSet, rows: List[OrderLineRow]): Unit = {
    ArrowIpc.writeOrderLines(allocator, rows, dataSet.orderLinesArrow)
    ArrowIpc.writeRegions(allocator, RegionRow.all, dataSet.regionsArrow)
    val parquetFiles = ParquetExport.fromArrowIpc(allocator, dataSet.orderLinesArrow, dataSet.parquetDirectory)

    println(s"wrote ${dataSet.orderLinesArrow} (${sizeOf(dataSet.orderLinesArrow)})")
    println(s"wrote ${dataSet.regionsArrow} (${sizeOf(dataSet.regionsArrow)})")
    parquetFiles.foreach(file => println(s"wrote $file (${sizeOf(file)})"))
  }

  private def aggregateOnTheJvm(allocator: RootAllocator, dataSet: DataSet, rows: List[OrderLineRow]): Unit = {
    val objectBased = Stopwatch.measure("scala, over case classes")(RevenueAggregation.fromRows(rows))
    val columnar    =
      Stopwatch.measure("scala, over arrow vectors")(
        RevenueAggregation.fromArrowFile(allocator, dataSet.orderLinesArrow)
      )

    println()
    println(RevenueReport.table("revenue per country, computed on the JVM:", columnar.value))
    println()
    println(requireAgreement("case classes", objectBased.value, "arrow vectors", columnar.value))
    println(
      RevenueReport.timings(Seq(objectBased.label -> objectBased.bestMillis, columnar.label -> columnar.bestMillis))
    )
  }

  private def reportPolars(allocator: RootAllocator, dataSet: DataSet): Unit =
    if (!Files.exists(dataSet.polarsRevenueArrow)) {
      println()
      println(s"no Polars result at ${dataSet.polarsRevenueArrow} yet.")
      println("run the Polars container (see README, \"Run it\") and then run this program again.")
    } else {
      ExchangeIntegrity.verifyInputManifest(dataSet).fold(message => throw new IllegalStateException(message), identity)
      val fromPolars = ArrowIpc.readRevenue(allocator, dataSet.polarsRevenueArrow)
      val onTheJvm   = RevenueAggregation.fromArrowFile(allocator, dataSet.orderLinesArrow)
      println()
      println(RevenueReport.table("revenue per country, computed by Polars and read back over Arrow:", fromPolars))
      println()
      println(requireAgreement("arrow vectors", onTheJvm, "polars", fromPolars))
      readDouble(dataSet.polarsTimingMillis).foreach { millis =>
        println(f"polars reported $millis%.2f ms for the same aggregation (see ${dataSet.polarsTimingMillis})")
      }
    }

  private def requireAgreement(
      left: String,
      leftRows: Seq[RevenueByCountry],
      right: String,
      rightRows: Seq[RevenueByCountry]
  ): String =
    RevenueReport
      .verifiedAgreement(left, leftRows, right, rightRows)
      .fold(message => throw new IllegalStateException(message), identity)

  private def readDouble(path: Path): Option[Double] =
    Option
      .when(Files.exists(path)) {
        Try(Files.readString(path, StandardCharsets.UTF_8).trim.toDouble).toOption
      }
      .flatten

  private def sizeOf(path: Path): String = f"${Files.size(path) / 1048576.0}%.1f MiB"
}
