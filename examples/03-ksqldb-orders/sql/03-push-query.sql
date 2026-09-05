-- Step 3: a push query.
--
-- EMIT CHANGES turns the SELECT into a subscription: the HTTP response never
-- ends on its own, and ksqlDB pushes a new row down the same connection every
-- time the result changes. LIMIT is what makes this example terminate; drop it
-- and the client would keep printing rows until you stop it.
SELECT customerId, declinedCount, declinedCents
FROM declined_payments_per_customer
EMIT CHANGES
LIMIT 5;
