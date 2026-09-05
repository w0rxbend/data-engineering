package de.flink.s3sink.core

final class EventTimeWindowsSuite extends munit.FunSuite {

  private val oneHour = 3600000L

  test("a timestamp exactly on a boundary starts its own window") {
    assertEquals(EventTimeWindows.windowStart(oneHour * 5, oneHour), oneHour * 5)
    assertEquals(EventTimeWindows.windowEnd(oneHour * 5, oneHour), oneHour * 6)
  }

  test("a timestamp inside a window rounds down to the window start") {
    assertEquals(EventTimeWindows.windowStart(oneHour * 5 + 1, oneHour), oneHour * 5)
    assertEquals(EventTimeWindows.windowStart(oneHour * 6 - 1, oneHour), oneHour * 5)
  }

  test("timestamps before the epoch round down rather than towards zero") {
    assertEquals(EventTimeWindows.windowStart(-1L, oneHour), -oneHour)
  }

  test("a window size of zero is rejected instead of dividing by zero") {
    intercept[IllegalArgumentException](EventTimeWindows.windowStart(1L, 0L))
  }
}
