-- The query that explains why Trino exists: one statement, two storage systems.
-- delta.shop.orders is Parquet in object storage, postgresql.public.customers is
-- rows in a PostgreSQL database. Neither side is copied into the other; Trino reads
-- both and performs the join in memory.
SELECT
    c.tier,
    o.country,
    count(*)                     AS order_count,
    sum(o.total_cents) / 100.0   AS revenue_eur
FROM delta.shop.orders AS o
JOIN postgresql.public.customers AS c
  ON c.customer_id = o.customer_id
GROUP BY c.tier, o.country
ORDER BY revenue_eur DESC
LIMIT 10;

-- The same join narrowed to one customer tier. The filter on a PostgreSQL column is
-- pushed down into PostgreSQL, so the database sends back only the matching rows.
SELECT
    c.full_name,
    count(*)                   AS order_count,
    sum(o.total_cents) / 100.0 AS revenue_eur
FROM delta.shop.orders AS o
JOIN postgresql.public.customers AS c
  ON c.customer_id = o.customer_id
WHERE c.tier = 'gold'
GROUP BY c.full_name
ORDER BY revenue_eur DESC
LIMIT 5;
