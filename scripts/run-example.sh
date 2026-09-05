#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/run-example.sh up <module-dir>
  scripts/run-example.sh down <module-dir>
  scripts/run-example.sh logs <module-dir>
  scripts/run-example.sh test <module-dir>

Examples:
  scripts/run-example.sh up 09-trino-lakehouse-sql
  scripts/run-example.sh test 14-hugegraph-fraud-ring

module-dir is the folder under examples/, for example 09-trino-lakehouse-sql.
EOF
}

if [[ $# -ne 2 ]]; then
  usage
  exit 1
fi

ACTION=$1
MODULE=$2
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULE_DIR="${ROOT_DIR}/examples/${MODULE}"
COMPOSE_FILE="${MODULE_DIR}/docker/docker-compose.yml"
MILL_MODULE="examples.${MODULE}"

if [[ ! -d "${MODULE_DIR}" ]]; then
  echo "error: module directory not found: ${MODULE_DIR}" >&2
  exit 1
fi

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "error: compose file not found for ${MODULE}: ${COMPOSE_FILE}" >&2
  exit 1
fi

case "${ACTION}" in
  up)
    echo "Starting ${MODULE} stack..."
    docker compose -f "${COMPOSE_FILE}" up -d --wait
    ;;
  down)
    echo "Stopping ${MODULE} stack..."
    docker compose -f "${COMPOSE_FILE}" down -v
    ;;
  logs)
    docker compose -f "${COMPOSE_FILE}" logs -f
    ;;
  test)
    echo "Running Mill test task for ${MODULE}..."
    (cd "${ROOT_DIR}" && ./mill "${MILL_MODULE}.test")
    ;;
  *)
    usage
    exit 1
    ;;
esac
