#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/run-example.sh up <module-dir|number>
  scripts/run-example.sh down <module-dir|number>
  scripts/run-example.sh logs <module-dir|number>
  scripts/run-example.sh test <module-dir|number>

Examples:
  scripts/run-example.sh up 09-trino-lakehouse-sql
  scripts/run-example.sh test 14-hugegraph-fraud-ring
  scripts/run-example.sh up 09

The module value can be the exact folder name under examples/
or just its two-digit number.

module-dir is the folder under examples/, for example 09-trino-lakehouse-sql.
EOF
}

if [[ $# -ne 2 ]]; then
  usage
  exit 1
fi

ACTION=$1
MODULE_OR_NUMBER=$2
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULE_DIR=""

if [[ -d "${ROOT_DIR}/examples/${MODULE_OR_NUMBER}" ]]; then
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

if [[ ! -d "${MODULE_DIR}" ]]; then
  echo "error: module directory not found: ${MODULE_DIR}" >&2
  exit 1
fi

case "${ACTION}" in
  up)
    if [[ ! -f "${COMPOSE_FILE}" ]]; then
      echo "error: compose file not found for ${MODULE}: ${COMPOSE_FILE}" >&2
      exit 1
    fi
    echo "Starting ${MODULE} stack..."
    docker compose -f "${COMPOSE_FILE}" up -d --wait
    ;;
  down)
    if [[ ! -f "${COMPOSE_FILE}" ]]; then
      echo "error: compose file not found for ${MODULE}: ${COMPOSE_FILE}" >&2
      exit 1
    fi
    echo "Stopping ${MODULE} stack..."
    docker compose -f "${COMPOSE_FILE}" down -v
    ;;
  logs)
    if [[ ! -f "${COMPOSE_FILE}" ]]; then
      echo "error: compose file not found for ${MODULE}: ${COMPOSE_FILE}" >&2
      exit 1
    fi
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
