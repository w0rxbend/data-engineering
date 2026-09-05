-- The operational side of the online shop: the customer master data.
--
-- These rows never leave PostgreSQL. Example 09 joins them against order facts that
-- live as Delta Lake files in object storage, and Trino performs that join without
-- either data set being copied anywhere.
CREATE TABLE customers (
    customer_id text PRIMARY KEY,
    full_name   text NOT NULL,
    tier        text NOT NULL,
    signed_up   date NOT NULL
);

-- One thousand customers with the identifiers the shared data generator produces
-- (cust-0000 .. cust-0999), so every generated order finds its customer.
INSERT INTO customers (customer_id, full_name, tier, signed_up)
SELECT
    'cust-' || to_char(n, 'FM0000'),
    'Customer ' || n,
    (ARRAY['bronze', 'silver', 'gold'])[1 + (n % 3)],
    DATE '2023-01-01' + (n % 365)
FROM generate_series(0, 999) AS n;

CREATE INDEX customers_tier_idx ON customers (tier);
