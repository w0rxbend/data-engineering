-- Step 4: a pull query.
--
-- A pull query has no EMIT CHANGES. It reads the current state of a table once,
-- answers, and closes the connection - the behaviour a dashboard needs when a
-- user refreshes a page. Because the table is windowed, each answer also
-- carries the start and end of the one-minute bucket it belongs to.
SELECT country, WINDOWSTART, WINDOWEND, revenueCents, orderCount
FROM revenue_per_country_per_minute
WHERE country = 'DE';
