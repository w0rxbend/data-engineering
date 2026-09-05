-- Trino addresses every table as catalog.schema.table.
-- A catalog is one configured connection to a data source, so the list below is
-- literally the list of systems this coordinator can read.
SHOW CATALOGS;

-- The lakehouse catalog: Delta Lake tables whose files live in the MinIO object store.
SHOW TABLES FROM delta.shop;

-- The operational catalog: tables that stay inside PostgreSQL and are never copied.
SHOW TABLES FROM postgresql.public;

-- Column types of the Delta table, read out of the Delta transaction log.
DESCRIBE delta.shop.orders;
