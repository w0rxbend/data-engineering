# Local script helpers

This folder contains small local utilities for running the example modules consistently.

## `run-example.sh`

Wrapper for compose-driven workflow on a single example module:

```bash
scripts/run-example.sh up 09-trino-lakehouse-sql    # docker compose up --wait
scripts/run-example.sh test 09-trino-lakehouse-sql   # ./mill examples.09-trino-lakehouse-sql.test
scripts/run-example.sh logs 09-trino-lakehouse-sql   # compose logs -f
scripts/run-example.sh down 09-trino-lakehouse-sql   # compose down -v
```

The `module` value is the directory name under `examples/` (for example
`02-kafka-streams-fraud`).

