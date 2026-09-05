package de.spark.streaming

import munit.FunSuite
import org.apache.spark.sql.SparkSession

/**
 * Base class for every test in this example: it owns one Apache Spark session shared by all tests in the suite.
 *
 * Starting a Spark session costs a few seconds, so creating one per test would make the suite unpleasantly slow. It is
 * created before the first test and stopped after the last one.
 *
 * No container is involved. Everything runs inside this JVM against in-memory sources, which is the whole reason the
 * transformations in [[OrderStreams]] were written as plain `DataFrame => DataFrame` functions.
 */
abstract class SparkSuite extends FunSuite {

  /**
   * `lazy val` rather than `var`: `import spark.implicits._` needs a stable identifier, and a `var` is not one. The
   * session is still created on first use, which is inside `beforeAll`.
   */
  protected lazy val spark: SparkSession = SparkSessions.local(getClass.getSimpleName, master = "local[2]")

  override def beforeAll(): Unit = spark.sparkContext.setLogLevel("ERROR")

  override def afterAll(): Unit = spark.stop()

  /** Tests may take longer than MUnit's default limit because a Spark job has to plan and schedule first. */
  override def munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(120, "s")
}
