-- Session properties are per-connection knobs. They change how the engine executes
-- the next queries without changing anything on disk or in the server configuration.
SHOW SESSION LIKE 'join_distribution_type';

-- The default is AUTOMATIC: Trino decides per query, and for tables this small it picks
-- BROADCAST, which sends the whole right-hand table to every worker. PARTITIONED is the
-- other strategy: both sides are redistributed by the join key, which is what you want
-- when neither side fits in the memory of one worker. Forcing it here makes the effect
-- of a session property visible in the plan.
SET SESSION join_distribution_type = 'PARTITIONED';

-- The join in the plan below now says `distribution = PARTITIONED` and both inputs
-- arrive through their own remote exchange, where the previous plan replicated one side.
EXPLAIN
SELECT c.tier, count(*)
FROM delta.shop.orders AS o
JOIN postgresql.public.customers AS c
  ON c.customer_id = o.customer_id
GROUP BY c.tier;

-- Back to the engine default for the rest of the session.
RESET SESSION join_distribution_type;

-- Trino keeps a table of everything it has run, including the elapsed time and how many
-- bytes each query read. This is the same data the web interface shows.
SELECT query_id, state, source, substr(query, 1, 40) AS query_start
FROM system.runtime.queries
ORDER BY created DESC
LIMIT 5;
