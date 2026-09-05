package de.presto.hive

import java.time.LocalDate

/**
 * Every SQL statement this example sends to PrestoDB, built as plain strings.
 *
 * Keeping the statements in one object with no database connection in sight has a practical payoff: the tests can
 * assert on the generated SQL without a running cluster, and a reader can look up any statement the program executes in
 * a single file.
 *
 * @param catalog
 *   the PrestoDB catalog name. A catalog is one configured data source; `hive` is the name given to the Hive connector
 *   in `docker/presto/catalog/hive.properties`.
 * @param schema
 *   the schema (in Hive terminology, the database) that holds the table.
 * @param table
 *   the table name.
 */
final case class HiveSql(catalog: String = "hive", schema: String = "shop", table: String = "clickstream") {

  /** How long a visitor may be idle before the next click counts as a new visit. */
  val DefaultSessionGapMillis: Long = 30 * 60 * 1000L

  /** Fully qualified table name, for example `hive.shop.clickstream`. */
  val qualifiedTable: String = s"$catalog.$schema.$table"

  /**
   * Creates the schema.
   *
   * The location is where the Hive metastore would put *managed* tables of this schema. This example only creates an
   * external table with an explicit location, but a schema without a location makes the metastore fall back to a
   * default that does not exist in this stack.
   */
  def createSchema(schemaLocation: String): String =
    s"CREATE SCHEMA IF NOT EXISTS $catalog.$schema WITH (location = '$schemaLocation')"

  def dropTable: String = s"DROP TABLE IF EXISTS $qualifiedTable"

  /**
   * Registers the Parquet files already sitting in object storage as an external table.
   *
   * Three details matter.
   *
   *   - `external_location` means PrestoDB and the metastore only describe the files; dropping the table deletes no
   *     data. That is the normal shape for a data lake table whose files are produced by another system, which is
   *     exactly the case here (the Scala writer produced them).
   *   - `partitioned_by` names the columns whose values come from the directory names rather than from inside the
   *     files. Hive requires them to be the last columns of the column list, in partition order.
   *   - `format = 'PARQUET'` tells the connector how to read the files. Parquet is columnar, so a query that touches
   *     three of six columns reads roughly half the bytes.
   */
  def createExternalTable(externalLocation: String): String =
    s"""CREATE TABLE $qualifiedTable (
       |  customer_id varchar,
       |  page varchar,
       |  sku varchar,
       |  occurred_at bigint,
       |  country varchar,
       |  dt varchar
       |) WITH (
       |  format = 'PARQUET',
       |  external_location = '$externalLocation',
       |  partitioned_by = ARRAY['country', 'dt']
       |)""".stripMargin

  /**
   * Tells the metastore which partition directories exist.
   *
   * A freshly created external table has zero partitions registered, so it reads as empty no matter how many files are
   * in object storage. `sync_partition_metadata` walks the table location, finds every `country=.../dt=...` directory
   * and adds it. Mode `FULL` also removes registrations whose directory has disappeared.
   */
  def syncPartitionMetadata: String =
    s"CALL $catalog.system.sync_partition_metadata('$schema', '$table', 'FULL')"

  /**
   * Collects table and column statistics (row counts, distinct-value counts, null counts, value ranges).
   *
   * Partition pruning works without statistics, because it only needs the directory names. Statistics matter for the
   * decisions that come after pruning: join order, whether to broadcast one side of a join, and the row-count estimates
   * that `EXPLAIN` prints. Running `ANALYZE` once after loading data is the cheap way to get all of that.
   */
  def analyze: String = s"ANALYZE $qualifiedTable"

  /**
   * Wraps any query in `EXPLAIN`, which returns the query plan as text instead of running the query.
   *
   * The plan is where partition pruning becomes visible: the `TableScan` node lists the partition constraint the
   * connector will apply, so a plan with a partition predicate names the partitions it will open while a plan without
   * one does not restrict them at all.
   */
  def explain(query: String): String = s"EXPLAIN $query"

  /** A deliberately simple counting query, used only to compare how many bytes each variant has to read. */
  def countAll: String = s"SELECT count(*) AS events FROM $qualifiedTable"

  /** The same count restricted to one partition, so the connector can skip every other directory. */
  def countInPartition(partition: HivePartition): String =
    s"SELECT count(*) AS events FROM $qualifiedTable WHERE ${partition.sqlPredicate}"

  /**
   * The sessionization prelude both reports are built on: three common table expressions (the `WITH name AS (...)`
   * form, a named subquery) that turn raw clicks into numbered shopping sessions.
   *
   *   1. `visits` reads the rows for the requested days. This is the only step that touches storage, and the `dt`
   *      predicate here is what triggers partition pruning.
   *   1. `gapped` marks the first click of every session. `LAG(occurred_at) OVER (PARTITION BY customer_id ORDER BY
   *      occurred_at)` is the previous click of the same customer; a gap larger than `sessionGapMillis` starts a new
   *      session. This is the standard sessionization idiom and it cannot be expressed with plain `GROUP BY`.
   *   1. `sessions` turns those markers into a session number with a running sum over the same ordered window. The
   *      frame is written out as `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` on purpose: the SQL default frame
   *      is `RANGE`, which would lump together clicks that share a timestamp.
   *
   * Both reports count *sessions*, not people. A shop with a thousand regulars would otherwise report that every one of
   * them bought something eventually, which is true and useless.
   *
   * @param days
   *   the calendar days to include.
   * @param sessionGapMillis
   *   how long a visitor may be idle before the next click counts as a new visit. Thirty minutes is the convention
   *   inherited from web analytics tools.
   */
  private def sessionize(days: Seq[LocalDate], sessionGapMillis: Long): String = {
    val dayList = days.map(day => s"'$day'").mkString(", ")
    s"""WITH visits AS (
       |  SELECT customer_id, country, page, occurred_at
       |  FROM $qualifiedTable
       |  WHERE dt IN ($dayList)
       |),
       |gapped AS (
       |  SELECT
       |    customer_id,
       |    country,
       |    page,
       |    occurred_at,
       |    CASE
       |      WHEN occurred_at - LAG(occurred_at) OVER (PARTITION BY customer_id ORDER BY occurred_at)
       |             > $sessionGapMillis THEN 1
       |      WHEN LAG(occurred_at) OVER (PARTITION BY customer_id ORDER BY occurred_at) IS NULL THEN 1
       |      ELSE 0
       |    END AS starts_session
       |  FROM visits
       |),
       |sessions AS (
       |  SELECT
       |    customer_id,
       |    country,
       |    page,
       |    occurred_at,
       |    SUM(starts_session) OVER (
       |      PARTITION BY customer_id ORDER BY occurred_at
       |      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       |    ) AS session_number
       |  FROM gapped
       |)""".stripMargin
  }

  /**
   * Counts how many shopping sessions reached each step of the funnel `/home -> /search -> /product -> /cart ->
   * /checkout`, in that order.
   *
   * On top of the sessionization prelude, `stage_times` collapses each session to the first time it touched each page
   * with `MIN(...) OVER (PARTITION BY customer_id, session_number)`. The final `SELECT` then counts sessions per step,
   * requiring each step's first touch to come strictly after the previous step's: a visitor who lands on `/checkout`
   * and only afterwards wanders to `/search` did not walk the funnel. `count_if` is PrestoDB's shorthand for counting
   * the rows that satisfy a condition.
   */
  def funnel(days: Seq[LocalDate], sessionGapMillis: Long = DefaultSessionGapMillis): String =
    s"""${sessionize(days, sessionGapMillis)},
       |stage_times AS (
       |  SELECT DISTINCT
       |    customer_id,
       |    session_number,
       |    MIN(CASE WHEN page = '/home' THEN occurred_at END)
       |      OVER (PARTITION BY customer_id, session_number) AS home_at,
       |    MIN(CASE WHEN page = '/search' THEN occurred_at END)
       |      OVER (PARTITION BY customer_id, session_number) AS search_at,
       |    MIN(CASE WHEN page = '/product' THEN occurred_at END)
       |      OVER (PARTITION BY customer_id, session_number) AS product_at,
       |    MIN(CASE WHEN page = '/cart' THEN occurred_at END)
       |      OVER (PARTITION BY customer_id, session_number) AS cart_at,
       |    MIN(CASE WHEN page = '/checkout' THEN occurred_at END)
       |      OVER (PARTITION BY customer_id, session_number) AS checkout_at
       |  FROM sessions
       |)
       |SELECT
       |  count_if(home_at IS NOT NULL) AS reached_home,
       |  count_if(search_at > home_at) AS reached_search,
       |  count_if(product_at > search_at AND search_at > home_at) AS reached_product,
       |  count_if(cart_at > product_at AND product_at > search_at AND search_at > home_at) AS reached_cart,
       |  count_if(
       |    checkout_at > cart_at AND cart_at > product_at AND product_at > search_at AND search_at > home_at
       |  ) AS reached_checkout
       |FROM stage_times""".stripMargin

  /**
   * Per-country conversion: how many shopping sessions each storefront saw, and how many of them reached `/checkout`.
   *
   * `country` is a partition column, so grouping by it costs nothing extra to read: the connector already knows which
   * directory every row came from, without looking inside a single file.
   */
  def conversionByCountry(days: Seq[LocalDate], sessionGapMillis: Long = DefaultSessionGapMillis): String =
    s"""${sessionize(days, sessionGapMillis)},
       |per_session AS (
       |  SELECT
       |    country,
       |    customer_id,
       |    session_number,
       |    max(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END) AS reached_checkout
       |  FROM sessions
       |  GROUP BY country, customer_id, session_number
       |)
       |SELECT
       |  country,
       |  count(*) AS sessions,
       |  sum(reached_checkout) AS purchases
       |FROM per_session
       |GROUP BY country
       |ORDER BY country""".stripMargin
}
