package de.polars.bridge

/** Renders aggregation results and comparisons as plain text, so the domain code never has to know about printing. */
object RevenueReport {

  private val Header = Seq("country", "region", "orders", "units", "revenue")

  private def euro(cents: Long): String = f"${cents / 100.0}%,.2f EUR"

  private def cells(row: RevenueByCountry): Seq[String] =
    Seq(row.country, row.region, row.orderCount.toString, row.units.toString, euro(row.revenueCents))

  /** A fixed-width table of the aggregate, headed by `title`. */
  def table(title: String, rows: Seq[RevenueByCountry]): String = {
    val body   = rows.map(cells)
    val widths = Header.indices.map(column => (Header +: body).map(_(column).length).max)

    def line(values: Seq[String]): String =
      values.zip(widths).map { case (value, width) => value.padTo(width, ' ') }.mkString("  ").stripTrailing()

    val separator = widths.map("-" * _).mkString("  ")
    (Seq(title, line(Header), separator) ++ body.map(line)).mkString("\n")
  }

  /**
   * States whether two implementations produced the same numbers, and lists the differences when they did not.
   *
   * Agreement is the point of the whole example: if crossing the language boundary changed an answer, the boundary
   * would not be worth crossing.
   */
  def agreement(
      left: String,
      leftRows: Seq[RevenueByCountry],
      right: String,
      rightRows: Seq[RevenueByCountry]
  ): String = {
    val byCountry  = rightRows.map(row => row.country -> row).toMap
    val mismatches = leftRows.filterNot(row => byCountry.get(row.country).contains(row))
    if (mismatches.isEmpty && leftRows.size == rightRows.size) {
      s"$left and $right agree on all ${leftRows.size} rows."
    } else {
      val details =
        mismatches.map(row => s"  $left says $row, $right says ${byCountry.getOrElse(row.country, "nothing")}")
      (s"$left and $right disagree:" +: details).mkString("\n")
    }
  }

  /** One line per measurement, plus how much faster the fastest one was. */
  def timings(measurements: Seq[(String, Double)]): String = {
    if (measurements.isEmpty) { return "no timings recorded" }
    val labelWidth = measurements.map(_._1.length).max
    val fastest    = measurements.map(_._2).min
    val lines      = measurements.map { case (label, millis) =>
      f"  ${label.padTo(labelWidth, ' ')}  $millis%8.2f ms  (${millis / fastest}%.2fx the fastest)"
    }
    ("timings, best of several repetitions:" +: lines).mkString("\n")
  }
}
