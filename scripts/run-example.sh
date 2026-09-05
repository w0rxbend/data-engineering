#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/run-example.sh up <module-dir|number>
  scripts/run-example.sh down <module-dir|number>
  scripts/run-example.sh reset <module-dir|number>
  scripts/run-example.sh logs <module-dir|number>
  scripts/run-example.sh status <module-dir|number>
  scripts/run-example.sh config <module-dir|number>
  scripts/run-example.sh test <module-dir|number>
  scripts/run-example.sh run <module-dir|number> [program arguments...]
  scripts/run-example.sh list

Examples:
  scripts/run-example.sh up 09-trino-lakehouse-sql
  scripts/run-example.sh test 14-hugegraph-fraud-ring
  scripts/run-example.sh up 09

The module value can be the exact folder name under examples/
or just its two-digit number.

module-dir is the folder under examples/, for example 09-trino-lakehouse-sql.
down preserves volumes; reset deletes this example's volumes.
Set MILL_DOCKER=1 to build or run Scala/Java through the root Compose runner.
EOF
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -eq 1 && $1 == list ]]; then
  for module in "$ROOT_DIR"/examples/[0-9][0-9]-*/; do
    basename "$module"
  done
  exit 0
fi

if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

ACTION=$1
MODULE_OR_NUMBER=$2
shift 2
MODULE_DIR=""

if [[ "$ACTION" != run && $# -ne 0 ]]; then
  usage
  exit 1
fi

if [[ "${MODULE_OR_NUMBER}" =~ ^[0-9]{2}-[a-z0-9-]+$ && -d "${ROOT_DIR}/examples/${MODULE_OR_NUMBER}" ]]; then
  MODULE_DIR="${ROOT_DIR}/examples/${MODULE_OR_NUMBER}"
elif [[ "${MODULE_OR_NUMBER}" =~ ^[0-9]{2}$ ]]; then
  shopt -s nullglob
  MATCHES=("${ROOT_DIR}/examples/${MODULE_OR_NUMBER}-"*/)
  shopt -u nullglob

  if [[ ${#MATCHES[@]} -eq 1 ]]; then
    MODULE_DIR="${MATCHES[0]%/}"
  elif [[ ${#MATCHES[@]} -gt 1 ]]; then
    echo "error: example number ${MODULE_OR_NUMBER} is ambiguous." >&2
    echo "Matches:"
    for match in "${MATCHES[@]}"; do
      echo "  - $(basename "${match}")"
    done
    exit 1
  fi
elif [[ "${MODULE_OR_NUMBER}" =~ ^[0-9]+$ ]]; then
  echo "error: expected a two-digit module number (01-16), got ${MODULE_OR_NUMBER}." >&2
  exit 1
fi

if [[ -z "${MODULE_DIR}" ]]; then
  echo "error: module not found: ${MODULE_OR_NUMBER}" >&2
  echo "Try using either the directory name (09-trino-lakehouse-sql) or two-digit number (09)." >&2
  exit 1
fi

MODULE="$(basename "${MODULE_DIR}")"
COMPOSE_FILE="${MODULE_DIR}/docker/docker-compose.yml"
MILL_MODULE="examples.${MODULE}"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "error: compose file not found for ${MODULE}: ${COMPOSE_FILE}" >&2
  exit 1
fi

run_mill() {
  if [[ ${MILL_DOCKER:-0} == 1 ]]; then
    "$ROOT_DIR/scripts/mill-docker.sh" "$@"
  else
    (cd "$ROOT_DIR" && ./mill "$@")
  fi
}

case "${ACTION}" in
  up)
    if [[ "$MODULE" == 12-* ]]; then
      echo "Example 12 is a batch job. Follow its README: generate Arrow data, run the Polars container, verify results." >&2
      exit 1
    fi
    # Connect bind-mounts the plugin jar; otherwise Docker creates a directory there.
    if [[ "$MODULE" == 04-* ]]; then
      run_mill "${MILL_MODULE}.assembly"
    fi
    echo "Starting ${MODULE} stack..."
    docker compose -f "${COMPOSE_FILE}" up -d --wait --wait-timeout 300
    case "$MODULE" in
      11-*) docker compose -f "$COMPOSE_FILE" run --rm minio-init ;;
      13-*) docker compose -f "$COMPOSE_FILE" run --rm cmak-register ;;
    esac
    ;;
  down)
    echo "Stopping ${MODULE} stack..."
    docker compose -f "${COMPOSE_FILE}" down
    ;;
  reset)
    echo "Deleting ${MODULE} containers and volumes..."
    docker compose -f "${COMPOSE_FILE}" down -v
    ;;
  logs)
    docker compose -f "${COMPOSE_FILE}" logs -f
    ;;
  status)
    docker compose -f "${COMPOSE_FILE}" ps --all
    ;;
  config)
    docker compose -f "${COMPOSE_FILE}" config --quiet
    ;;
  test)
    echo "Running Mill test task for ${MODULE}..."
    run_mill "${MILL_MODULE}.test"
    ;;
  run)
    if [[ "$MODULE" == 04-* ]]; then
      echo "Example 04 is a Java Connect plugin. Use up to build and mount it, then follow its README." >&2
      exit 1
    fi
    run_mill "${MILL_MODULE}.run" "$@"
    ;;
  *)
    usage
    exit 1
    ;;
esac
