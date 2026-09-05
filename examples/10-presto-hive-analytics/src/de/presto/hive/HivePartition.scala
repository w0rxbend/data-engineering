package de.presto.hive

import java.time.{Instant, LocalDate, ZoneOffset}

/**
 * One partition of the clickstream table: all events from one country on one calendar day.
 *
 * Apache Hive stores a partitioned table as a directory tree in which every directory name encodes one partition column
 * as `name=value`, for example `country=DE/dt=2023-11-14`. A query engine can therefore decide which directories it
 * must open by looking at the directory names alone, without opening a single data file. That decision is called
 * *partition pruning* and it is the single largest performance lever in this example.
 *
 * The values are validated on construction because a partition value ends up verbatim in a path and, through the
 * metastore, in a query plan: an empty or slash-containing country code would silently produce a directory that no
 * engine can map back to a partition.
 */
final case class HivePartition(country: String, day: LocalDate) {

  require(HivePartition.isValidCountry(country), s"country must be two uppercase letters, got '$country'")

  /** The relative directory this partition's files live in, for example `country=DE/dt=2023-11-14`. */
  def relativePath: String = s"country=$country/dt=$day"

  /**
   * The predicate that selects exactly this partition in SQL.
   *
   * Both partition columns of this table are `varchar` rather than `date` or `integer`. Hive partition values are
   * strings on disk, and keeping the declared type a string means the value in the SQL text and the value in the
   * directory name are literally the same characters, which makes the `EXPLAIN` output easy to read.
   */
  def sqlPredicate: String = s"country = '$country' AND dt = '$day'"
}

object HivePartition {

  private val CountryPattern = "^[A-Z]{2}$".r

  def isValidCountry(candidate: String): Boolean =
    CountryPattern.findFirstIn(candidate).isDefined

  /**
   * Derives the partition of an event from its country and its timestamp.
   *
   * The day is computed in UTC (Coordinated Universal Time, the zero-offset reference time zone). Partitioning by a
   * local calendar day would make the same event land in different partitions depending on where the job runs, and
   * re-running a backfill on a differently configured machine would then duplicate or lose rows.
   */
  def of(country: String, occurredAtEpochMillis: Long): HivePartition =
    HivePartition(country, Instant.ofEpochMilli(occurredAtEpochMillis).atZone(ZoneOffset.UTC).toLocalDate)

  /**
   * Parses a relative partition path such as `country=DE/dt=2023-11-14` back into a partition.
   *
   * Returns `None` for anything that does not have exactly the two expected `name=value` segments in the expected
   * order. Reading a path back is what lets a test assert on the layout the writer produced.
   */
  def parse(relativePath: String): Option[HivePartition] =
    relativePath.split('/').toList match {
      case countrySegment :: daySegment :: Nil =>
        for {
          country <- valueOf(countrySegment, "country")
          rawDay  <- valueOf(daySegment, "dt")
          day     <- scala.util.Try(LocalDate.parse(rawDay)).toOption
          if isValidCountry(country)
        } yield HivePartition(country, day)
      case _ => None
    }

  private def valueOf(segment: String, expectedName: String): Option[String] =
    segment.split('=') match {
      case Array(name, value) if name == expectedName => Some(value)
      case _                                          => None
    }
}
