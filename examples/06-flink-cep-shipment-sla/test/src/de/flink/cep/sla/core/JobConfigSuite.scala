package de.flink.cep.sla.core

final class JobConfigSuite extends munit.FunSuite {

  test("without any setting the defaults are used") {
    assertEquals(JobConfig.from(Map.empty, Map.empty), JobConfig.default)
  }

  test("both --flag value and --flag=value name the same setting") {
    assertEquals(
      JobConfig.parseArguments(Seq("--shipment-topic", "parcels")),
      JobConfig.parseArguments(Seq("--shipment-topic=parcels"))
    )
    assertEquals(JobConfig.parseArguments(Seq("--shipment-topic=parcels")), Map("SHIPMENT_TOPIC" -> "parcels"))
  }

  test("a command-line argument overrides the environment") {
    val config = JobConfig.from(
      arguments = Map("DELIVER_WITHIN_MS" -> "600000"),
      env = Map("DELIVER_WITHIN_MS" -> "60000", "BREACH_TOPIC" -> "late-parcels")
    )
    assertEquals(config.policy.deliverWithinMillis, 600000L)
    assertEquals(config.breachTopic, "late-parcels")
  }

  test("a window that is not a positive number of milliseconds is refused with a readable message") {
    val notANumber = intercept[IllegalArgumentException](JobConfig.from(Map("DISPATCH_WITHIN_MS" -> "soon"), Map.empty))
    assert(notANumber.getMessage.contains("DISPATCH_WITHIN_MS"), notANumber.getMessage)
    intercept[IllegalArgumentException](JobConfig.from(Map("DISPATCH_WITHIN_MS" -> "0"), Map.empty))
  }

  test("an empty setting falls back to the default instead of blanking the topic") {
    assertEquals(
      JobConfig.from(Map("SHIPMENT_TOPIC" -> "  "), Map.empty).shipmentTopic,
      JobConfig.default.shipmentTopic
    )
  }

  test("zero out-of-orderness is valid for an ordered carrier feed") {
    val config = JobConfig.from(Map("MAX_OUT_OF_ORDERNESS_MS" -> "0"), Map.empty)
    assertEquals(config.maxOutOfOrdernessMillis, 0L)
    intercept[IllegalArgumentException](JobConfig.from(Map("MAX_OUT_OF_ORDERNESS_MS" -> "-1"), Map.empty))
  }
}
