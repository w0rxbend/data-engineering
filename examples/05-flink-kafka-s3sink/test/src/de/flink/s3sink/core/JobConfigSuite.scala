package de.flink.s3sink.core

final class JobConfigSuite extends munit.FunSuite {

  test("with nothing configured the built-in defaults are used") {
    assertEquals(JobConfig.from(Map.empty, Map.empty), JobConfig.default)
  }

  test("environment variables override the defaults") {
    val config = JobConfig.from(Map.empty, Map("KAFKA_TOPIC" -> "orders-v2", "WINDOW_SIZE_MS" -> "60000"))
    assertEquals(config.topic, "orders-v2")
    assertEquals(config.windowSizeMillis, 60000L)
    assertEquals(config.bootstrapServers, JobConfig.default.bootstrapServers)
  }

  test("command-line arguments override environment variables") {
    val config = JobConfig.from(Map("KAFKA_TOPIC" -> "from-args"), Map("KAFKA_TOPIC" -> "from-env"))
    assertEquals(config.topic, "from-args")
  }

  test("both --flag value and --flag=value name the same setting") {
    assertEquals(
      JobConfig.parseArguments(Seq("--kafka-topic", "orders")),
      JobConfig.parseArguments(Seq("--kafka-topic=orders"))
    )
    assertEquals(JobConfig.parseArguments(Seq("--kafka-topic=orders")), Map("KAFKA_TOPIC" -> "orders"))
  }

  test("a flag without a value is ignored rather than shifting the next flag") {
    assertEquals(
      JobConfig.parseArguments(Seq("--kafka-topic", "--output-uri", "s3://bucket/prefix")),
      Map("OUTPUT_URI" -> "s3://bucket/prefix")
    )
  }

  test("a blank value falls back to the default instead of producing an empty topic") {
    assertEquals(JobConfig.from(Map("KAFKA_TOPIC" -> "   "), Map.empty).topic, JobConfig.default.topic)
  }

  test("a duration that is not a number is reported by name") {
    val failure = intercept[IllegalArgumentException](JobConfig.from(Map("WINDOW_SIZE_MS" -> "soon"), Map.empty))
    assert(failure.getMessage.contains("WINDOW_SIZE_MS"), failure.getMessage)
  }

  test("a non-positive duration is rejected") {
    intercept[IllegalArgumentException](JobConfig.from(Map("CHECKPOINT_INTERVAL_MS" -> "0"), Map.empty))
  }

  test("zero out-of-orderness is valid for a source with monotonic event time") {
    val config = JobConfig.from(Map("MAX_OUT_OF_ORDERNESS_MS" -> "0"), Map.empty)
    assertEquals(config.maxOutOfOrdernessMillis, 0L)
    intercept[IllegalArgumentException](JobConfig.from(Map("MAX_OUT_OF_ORDERNESS_MS" -> "-1"), Map.empty))
  }
}
