-- Step 1: describe the raw Apache Kafka topics as ksqlDB streams.
--
-- A ksqlDB STREAM is a schema placed over a Kafka topic. Creating one moves no
-- data and copies nothing: it only tells ksqlDB how to read the bytes that are
-- already in the topic (or that will arrive later).
--
-- KEY marks the column stored in the Kafka record key rather than in the JSON
-- value. TIMESTAMP tells ksqlDB which column carries event time, so that the
-- windowed aggregation in 02-create-analytics.sql groups orders by when they
-- were placed rather than by when ksqlDB happened to read them.

CREATE STREAM orders_raw (
  id VARCHAR KEY,
  customerId VARCHAR,
  lines ARRAY<STRUCT<sku VARCHAR, quantity INT, unitPrice STRUCT<cents BIGINT, currency VARCHAR>>>,
  placedAt BIGINT,
  country VARCHAR
) WITH (
  KAFKA_TOPIC = 'orders',
  PARTITIONS = 1,
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'placedAt'
);

CREATE STREAM payments_raw (
  orderId VARCHAR KEY,
  amount STRUCT<cents BIGINT, currency VARCHAR>,
  status VARCHAR,
  occurredAt BIGINT
) WITH (
  KAFKA_TOPIC = 'payments',
  PARTITIONS = 1,
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'occurredAt'
);
