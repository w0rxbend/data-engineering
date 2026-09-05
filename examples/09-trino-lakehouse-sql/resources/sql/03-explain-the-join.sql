-- EXPLAIN prints the plan Trino would run without running it.
-- Look for the two sources at the bottom of the plan: a TableScan on the Delta table
-- and a TableScan on the PostgreSQL table, feeding one join operator.
EXPLAIN
SELECT c.tier, count(*)
FROM delta.shop.orders AS o
JOIN postgresql.public.customers AS c
  ON c.customer_id = o.customer_id
GROUP BY c.tier;

-- EXPLAIN (TYPE IO) answers a different question: which tables and columns would this
-- query actually touch? It is the quickest way to see what a connector pushed down.
EXPLAIN (TYPE IO, FORMAT TEXT)
SELECT count(*)
FROM postgresql.public.customers
WHERE tier = 'gold';
