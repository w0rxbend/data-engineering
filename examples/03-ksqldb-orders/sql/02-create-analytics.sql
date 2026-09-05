-- Step 2: the operations dashboard, expressed as three derived collections.
--
-- Every statement below is "persistent": ksqlDB starts a long-running query
-- that keeps the result up to date as new records arrive, and writes that
-- result into a new Kafka topic. CSAS is short for CREATE STREAM AS SELECT and
-- CTAS for CREATE TABLE AS SELECT. A STREAM is an unbounded sequence of facts;
-- a TABLE is the current value per key, which is what a dashboard tile shows.

-- 2a. Flatten each order into one row carrying its total in cents.
--     REDUCE walks the ARRAY of order lines with a running accumulator, the
--     same way `foldLeft` does in Scala. `->` reads a field out of a STRUCT.
CREATE STREAM orders_enriched AS
  SELECT
    id,
    customerId,
    country,
    REDUCE(lines, CAST(0 AS BIGINT), (total, line) => total + line->quantity * line->unitPrice->cents)
      AS totalCents
  FROM orders_raw
  EMIT CHANGES;

-- 2b. Revenue per country per minute.
--     WINDOW TUMBLING cuts event time into fixed, non-overlapping one-minute
--     buckets, so the key of this table is the pair (country, window).
CREATE TABLE revenue_per_country_per_minute AS
  SELECT
    country,
    SUM(totalCents) AS revenueCents,
    COUNT(*) AS orderCount
  FROM orders_enriched
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY country
  EMIT CHANGES;

-- 2c. Declined payments, joined back to the order that was being paid for.
--     A stream-stream join needs a time bound: WITHIN 1 HOURS means "match a
--     payment with an order whose event time is at most one hour away".
CREATE STREAM declined_payments AS
  SELECT
    p.orderId AS orderId,
    o.customerId AS customerId,
    o.country AS country,
    p.amount->cents AS amountCents
  FROM payments_raw p
  JOIN orders_enriched o WITHIN 1 HOURS ON p.orderId = o.id
  WHERE p.status = 'Declined'
  EMIT CHANGES;

-- 2d. One row per customer who has had at least one payment declined.
CREATE TABLE declined_payments_per_customer AS
  SELECT
    customerId,
    COUNT(*) AS declinedCount,
    SUM(amountCents) AS declinedCents
  FROM declined_payments
  GROUP BY customerId
  EMIT CHANGES;
