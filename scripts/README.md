# Local script helpers

This folder contains small local utilities for running the example modules consistently.

## `run-example.sh`

Wrapper for a Compose workflow on a single example module:

```bash
scripts/run-example.sh up 09-trino-lakehouse-sql   # docker compose up --wait
scripts/run-example.sh up 09                      # docker compose up --wait (by example number)
scripts/run-example.sh test 09-trino-lakehouse-sql # ./mill examples.09-trino-lakehouse-sql.test
scripts/run-example.sh logs 09                    # compose logs -f (by example number)
scripts/run-example.sh down 09-trino-lakehouse-sql # stop containers, retain volumes
scripts/run-example.sh reset 09                   # delete this stack's volumes
```

The `module` value is the directory name under `examples/` (for example
`02-kafka-streams-fraud`) or the two-digit example number (`02`).

## `mill-docker.sh`

Run the same Mill tasks in a container, without installing a host JDK:

```bash
scripts/mill-docker.sh resolve __
scripts/mill-docker.sh __.test
scripts/mill-docker.sh examples.11-parquet-arrow-toolkit.run
```

This uses the root [Compose file](../compose.yml), runs as your user and shares
the checkout and `out/` directory. Dependencies and downloaded JDKs are cached in
`.docker-cache/`. The wrapper mounts the checkout at its original absolute path,
so an assembly produced inside the container can be mounted by an example stack.
Avoid simultaneous host and container builds in the same checkout.

The runner uses host networking to reach the examples' `localhost` endpoints,
including addresses returned in Kafka broker metadata. This works on Linux;
on Docker Desktop 4.34+, enable **Settings → Resources → Network → Enable host
networking** ([Docker documentation](https://docs.docker.com/engine/network/drivers/host/)).
The first invocation downloads Mill and the module JDKs and dependencies.

For environment overrides, use Compose directly. Set `DE_WORKSPACE` to the absolute
checkout path when mixing this with the wrapper or host builds:

```bash
DE_WORKSPACE="$PWD" DOCKER_UID="$(id -u)" DOCKER_GID="$(id -g)" \
  docker compose run --rm -e ORDER_COUNT=1000 mill examples.11-parquet-arrow-toolkit.run
```

Host environment variables are not implicitly passed to the container. Use `-e`
for each setting from the example README. Docker remains on the host; its socket
is not mounted into the build container.
