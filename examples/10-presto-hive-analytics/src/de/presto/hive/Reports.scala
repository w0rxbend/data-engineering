package de.presto.hive

/** How many sessions reached each step of the funnel, in funnel order. */
final case class FunnelCounts(
    reachedHome: Long,
    reachedSearch: Long,
    reachedProduct: Long,
    reachedCart: Long,
    reachedCheckout: Long
) {

  /** The steps as `(label, sessions)` pairs, so the renderer does not need to know the step names. */
  def steps: Seq[(String, Long)] = Seq(
    "/home"     -> reachedHome,
    "/search"   -> reachedSearch,
    "/product"  -> reachedProduct,
    "/cart"     -> reachedCart,
    "/checkout" -> reachedCheckout
  )
}

/**
 * One line of the per-country conversion report.
 *
 * Both figures count shopping sessions rather than people, so a shop with a small pool of loyal regulars does not
 * report a hundred percent conversion merely because every one of them bought something eventually.
 */
final case class ConversionRow(country: String, sessions: Long, purchases: Long) {

  /** Share of sessions that reached the checkout page, between 0.0 and 1.0. Zero sessions convert at zero. */
  def conversionRate: Double = if (sessions == 0) 0.0 else purchases.toDouble / sessions.toDouble
}

/** How many bytes PrestoDB had to read for the same count, with and without a partition predicate. */
final case class ScanComparison(bytesWithoutPredicate: Long, bytesWithPredicate: Long) {

  /**
   * How many times less data the pruned query read.
   *
   * Returns `None` when the pruned query read nothing measurable, because dividing by zero would report an infinite
   * speed-up that means nothing.
   */
  def reductionFactor: Option[Double] =
    if (bytesWithPredicate <= 0) None else Some(bytesWithoutPredicate.toDouble / bytesWithPredicate.toDouble)
}

/**
 * Turns query results into the text that lands on the console.
 *
 * Rendering is kept apart from querying so that the exact shape of the output is covered by fast unit tests instead of
 * being discovered by running the whole stack.
 */
object Reports {

  /** Renders a percentage with one decimal, for example `12.5%`. */
  def percentage(fraction: Double): String = f"${fraction * 100}%.1f%%"

  /** Renders a byte count in the largest unit that keeps the number readable, for example `1.4 MiB`. */
  def bytes(count: Long): String = {
    val units = Seq("B", "KiB", "MiB", "GiB", "TiB")
    val index =
      units.indices.takeWhile(i => count >= Math.pow(1024, (i + 1).toDouble)).lastOption.map(_ + 1).getOrElse(0)
    if (index == 0) s"$count B" else f"${count / Math.pow(1024, index.toDouble)}%.2f ${units(index)}"
  }

  /**
   * Renders the funnel, showing for every step how many sessions reached it, what share of the sessions that entered
   * the funnel that is, and what share of the *previous* step survived.
   *
   * The two percentages answer different questions. The overall share tells you how big the opportunity is; the
   * step-to-step share tells you which single step is leaking.
   */
  def funnel(counts: FunnelCounts): String = {
    val entered = counts.reachedHome
    val rows    = counts.steps.zipWithIndex.map { case ((label, reached), index) =>
      val previous   = if (index == 0) reached else counts.steps(index - 1)._2
      val ofEntered  = if (entered == 0) 0.0 else reached.toDouble / entered.toDouble
      val ofPrevious = if (previous == 0) 0.0 else reached.toDouble / previous.toDouble
      Seq(label, reached.toString, percentage(ofEntered), percentage(ofPrevious))
    }
    table(Seq("step", "sessions", "of entered", "of previous"), rows)
  }

  /** Renders the per-country conversion report, best converting country first. */
  def conversion(rows: Seq[ConversionRow]): String =
    table(
      Seq("country", "sessions", "purchases", "conversion"),
      rows
        .sortBy(row => (-row.conversionRate, row.country))
        .map(row => Seq(row.country, row.sessions.toString, row.purchases.toString, percentage(row.conversionRate)))
    )

  /** Renders the bytes-scanned comparison as two lines plus the resulting reduction. */
  def scanComparison(comparison: ScanComparison): String = {
    val factor = comparison.reductionFactor.map(f => f"$f%.1f x less data read").getOrElse("no measurable reduction")
    table(
      Seq("query", "bytes read"),
      Seq(
        Seq("whole table", bytes(comparison.bytesWithoutPredicate)),
        Seq("one partition", bytes(comparison.bytesWithPredicate))
      )
    ) + s"\n$factor"
  }

  /**
   * Renders a fixed-width text table with a header rule.
   *
   * Every column is padded to the widest cell in it, which keeps the output aligned in any terminal without needing a
   * table library.
   */
  def table(header: Seq[String], rows: Seq[Seq[String]]): String = {
    val all    = header +: rows
    val widths = header.indices.map(column => all.map(row => row.lift(column).fold(0)(_.length)).max)
    def render(row: Seq[String]): String =
      row.zip(widths).map { case (cell, width) => cell.padTo(width, ' ') }.mkString("  ").stripTrailing
    val rule = widths.map("-" * _).mkString("  ")
    (render(header) +: rule +: rows.map(render)).mkString("\n")
  }
}
