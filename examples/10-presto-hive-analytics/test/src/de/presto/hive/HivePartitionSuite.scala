package de.presto.hive

import java.time.LocalDate

final class HivePartitionSuite extends munit.FunSuite {

  test("a partition renders as the Hive name=value directory layout") {
    val partition = HivePartition("DE", LocalDate.of(2023, 11, 14))
    assertEquals(partition.relativePath, "country=DE/dt=2023-11-14")
  }

  test("the SQL predicate names both partition columns") {
    val partition = HivePartition("PL", LocalDate.of(2023, 11, 15))
    assertEquals(partition.sqlPredicate, "country = 'PL' AND dt = '2023-11-15'")
  }

  test("a partition path parses back into the partition it came from") {
    val partition = HivePartition("UA", LocalDate.of(2024, 1, 31))
    assertEquals(HivePartition.parse(partition.relativePath), Some(partition))
  }

  test("malformed partition paths are rejected rather than guessed at") {
    val rejected = Seq(
      "country=DE",
      "country=DE/dt=2023-11-14/clicks.parquet",
      "dt=2023-11-14/country=DE",
      "country=de/dt=2023-11-14",
      "country=DE/dt=not-a-date",
      "countryDE/dt=2023-11-14"
    )
    rejected.foreach(path => assertEquals(HivePartition.parse(path), None, s"expected '$path' to be rejected"))
  }

  test("the day is derived in UTC, not in the machine's local time zone") {
    // 2023-11-14T23:30:00Z. In any time zone east of UTC this instant already
    // belongs to the next local day, which is exactly the mistake being guarded against.
    val partition = HivePartition.of("FR", 1699997400000L)
    assertEquals(partition.day, LocalDate.of(2023, 11, 14))
  }

  test("a country code that could not become a directory name is refused") {
    intercept[IllegalArgumentException](HivePartition("de", LocalDate.of(2023, 11, 14)))
    intercept[IllegalArgumentException](HivePartition("", LocalDate.of(2023, 11, 14)))
    intercept[IllegalArgumentException](HivePartition("DE/PL", LocalDate.of(2023, 11, 14)))
  }
}
