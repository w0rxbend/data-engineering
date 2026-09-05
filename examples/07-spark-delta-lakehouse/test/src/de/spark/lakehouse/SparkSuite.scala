package de.spark.lakehouse

import java.nio.file.{Files, Path}
import java.util.Comparator

import munit.FunSuite
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

/**
 * Base class for the tests that need a real Apache Spark engine.
 *
 * Starting Spark takes a few seconds, so all suites share one session: `SparkSession.builder().getOrCreate()` hands
 * back the session that already exists in this Java Virtual Machine rather than starting a second one. The session runs
 * in `local[*]` mode, which means Spark uses threads inside the test process and no cluster, no container and no
 * network are involved. Delta Lake tables are written to a temporary directory that the operating system cleans up.
 */
abstract class SparkSuite extends FunSuite {

  /** A Spark session configured exactly like the job's, minus anything to do with object storage. */
  protected lazy val spark: SparkSession =
    SparkSession
      .builder()
      .appName("de-07-tests")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "2")
      // A local test does not need the user interface, and binding its port would clash with a parallel build.
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  /** Apache Spark logs a great deal at INFO level; the tests only care about genuine problems. */
  override def beforeAll(): Unit = spark.sparkContext.setLogLevel("WARN")

  /**
   * A fresh empty directory per test, deleted afterwards.
   *
   * Delta Lake tables are directories, so every test that writes one needs its own place to put it; sharing a directory
   * between tests would let one test's committed versions show up in another's time-travel assertions.
   */
  protected val tempDirectory: FunFixture[Path] = FunFixture[Path](
    setup = _ => Files.createTempDirectory("de-07-lakehouse"),
    teardown = deleteRecursively
  )

  /** `Files.delete` refuses to remove a directory that still has contents, so the tree is walked deepest-first. */
  private def deleteRecursively(directory: Path): Unit = {
    val entries = Files.walk(directory).sorted(Comparator.reverseOrder[Path]()).iterator().asScala.toList
    entries.foreach(Files.deleteIfExists)
  }
}
