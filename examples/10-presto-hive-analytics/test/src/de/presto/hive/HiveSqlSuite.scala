package de.presto.hive

import java.time.LocalDate

final class HiveSqlSuite extends munit.FunSuite {

  private val sql  = HiveSql()
  private val days = Seq(LocalDate.of(2023, 11, 14), LocalDate.of(2023, 11, 15))

  test("the table is fully qualified with catalog and schema") {
    assertEquals(sql.qualifiedTable, "hive.shop.clickstream")
  }

  test("the external table declares the partition columns last and in partition order") {
    val statement = sql.createExternalTable("s3a://lake/clickstream")
    assert(statement.contains("external_location = 's3a://lake/clickstream'"))
    assert(statement.contains("partitioned_by = ARRAY['country', 'dt']"))
    val columnOrder = Seq("customer_id", "page", "sku", "occurred_at", "country", "dt").map(statement.indexOf)
    assertEquals(columnOrder.sorted, columnOrder, "columns must be declared in the order Hive expects")
    assert(columnOrder.forall(_ >= 0))
  }

  test("partition metadata is synchronised in FULL mode so stale partitions are dropped") {
    assertEquals(sql.syncPartitionMetadata, "CALL hive.system.sync_partition_metadata('shop', 'clickstream', 'FULL')")
  }

  test("the counting queries differ only by the partition predicate") {
    val partition = HivePartition("DE", LocalDate.of(2023, 11, 15))
    assertEquals(sql.countInPartition(partition), s"${sql.countAll} WHERE ${partition.sqlPredicate}")
  }

  test("EXPLAIN wraps a query without altering it") {
    assertEquals(sql.explain(sql.countAll), s"EXPLAIN ${sql.countAll}")
  }

  test("the funnel query prunes on dt and uses an explicit ROWS window frame") {
    val statement = sql.funnel(days)
    assert(statement.contains("WHERE dt IN ('2023-11-14', '2023-11-15')"))
    assert(statement.contains("LAG(occurred_at) OVER (PARTITION BY customer_id ORDER BY occurred_at)"))
    assert(
      statement.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
      "the default RANGE frame would merge clicks that share a timestamp"
    )
  }

  test("the funnel session gap is configurable and appears in the generated SQL") {
    assert(sql.funnel(days, sessionGapMillis = 60000L).contains("> 60000 THEN 1"))
  }

  test("every funnel stage requires the previous stage to have happened earlier") {
    val statement = sql.funnel(days)
    assert(statement.contains("count_if(search_at > home_at) AS reached_search"))
    assert(statement.contains("count_if(product_at > search_at AND search_at > home_at) AS reached_product"))
    assert(statement.contains("AS reached_checkout"))
  }

  test("the conversion report counts sessions per country, not people") {
    val statement = sql.conversionByCountry(days)
    assert(statement.contains("WHERE dt IN ('2023-11-14', '2023-11-15')"))
    assert(statement.contains("GROUP BY country, customer_id, session_number"))
    assert(statement.contains("count(*) AS sessions"))
    assert(statement.contains("sum(reached_checkout) AS purchases"))
  }

  test("both reports sessionize the clicks the same way") {
    val prelude = "SUM(starts_session) OVER ("
    assert(sql.funnel(days).contains(prelude))
    assert(sql.conversionByCountry(days).contains(prelude))
    assert(sql.conversionByCountry(days, sessionGapMillis = 60000L).contains("> 60000 THEN 1"))
  }

  test("a different catalog or schema is carried through every statement") {
    val other = HiveSql(catalog = "lake", schema = "web", table = "clicks")
    assertEquals(other.qualifiedTable, "lake.web.clicks")
    assert(other.analyze.contains("lake.web.clicks"))
    assert(other.syncPartitionMetadata.startsWith("CALL lake.system.sync_partition_metadata('web', 'clicks'"))
    assert(other.createSchema("s3a://lake/web").startsWith("CREATE SCHEMA IF NOT EXISTS lake.web"))
  }
}
