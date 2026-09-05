package de.polars.bridge

/** A measurement: what was run, how long the fastest attempt took, and what it produced. */
final case class Timed[A](label: String, bestMillis: Double, value: A)

/**
 * A deliberately small benchmark helper.
 *
 * The Java Virtual Machine compiles hot code as it runs, so the first execution of a method is much slower than the
 * hundredth. Reporting that first execution would make any JVM measurement look terrible next to a native library.
 * [[measure]] therefore runs the work a few times to let the compiler settle ("warm-up"), then keeps the fastest of
 * several timed repetitions.
 *
 * This is not a substitute for a real benchmark harness such as JMH (the Java Microbenchmark Harness): it does not
 * control garbage collection, processor frequency scaling, or dead-code elimination. It is honest enough to show an
 * order-of-magnitude difference, which is all the README claims from it.
 */
object Stopwatch {

  def measure[A](label: String, warmups: Int = 2, repetitions: Int = 5)(work: => A): Timed[A] = {
    require(repetitions > 0, s"repetitions must be positive, was $repetitions")
    var round = 0
    while (round < warmups) {
      work
      round += 1
    }
    var bestNanos = Long.MaxValue
    var last      = work
    round = 0
    while (round < repetitions) {
      val startedAt = System.nanoTime()
      last = work
      val elapsed = System.nanoTime() - startedAt
      if (elapsed < bestNanos) { bestNanos = elapsed }
      round += 1
    }
    Timed(label, bestNanos / 1000000.0, last)
  }
}
