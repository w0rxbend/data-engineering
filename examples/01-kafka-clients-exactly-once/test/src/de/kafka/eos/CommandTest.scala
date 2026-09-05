package de.kafka.eos

import munit.FunSuite

/** Command line parsing, so a typo produces guidance rather than a stack trace. */
final class CommandTest extends FunSuite {

  test("parses the settlement command") {
    assertEquals(Command.parse(Vector("settle")), Right(Command.Settle))
  }

  test("parses the abort demonstration command") {
    assertEquals(Command.parse(Vector("abort-demo")), Right(Command.ShowAbort))
  }

  test("seeds a default number of orders when no count is given") {
    assertEquals(Command.parse(Vector("seed")), Right(Command.Seed(20)))
  }

  test("seeds the requested number of orders") {
    assertEquals(Command.parse(Vector("seed", "5")), Right(Command.Seed(5)))
  }

  test("rejects a seed count that is not a positive number") {
    assert(Command.parse(Vector("seed", "0")).isLeft)
  }

  test("rejects an unknown command") {
    assert(Command.parse(Vector("charge-everyone-twice")).isLeft)
  }

  test("rejects an empty command line") {
    assertEquals(Command.parse(Vector.empty), Left("no command given"))
  }
}
