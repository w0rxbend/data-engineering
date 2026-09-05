#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage: scripts/check-repo.sh [static|build|all]

  static  Check shell syntax, runner regressions, and every Compose model.
  build   Check Scalafmt, compile all modules, and run all tests.
  all     Run static and build checks (default).

Set MILL_DOCKER=1 to run the build layer through scripts/mill-docker.sh.
Set MILL_JOBS to a positive integer to change native Mill concurrency (default: 2).
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

run_static_checks() {
  require_command bash
  require_command docker
  require_command find
  require_command python3
  if ! docker compose version >/dev/null 2>&1; then
    echo "error: Docker Compose plugin is not available" >&2
    exit 1
  fi

  echo "Checking shell syntax..."
  while IFS= read -r -d '' script; do
    bash -n "$script"
  done < <(find "$ROOT_DIR/scripts" -maxdepth 1 -type f -name '*.sh' -print0)

  echo "Running shell-runner regression tests..."
  (cd "$ROOT_DIR" && PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*.py')

  local compose_files=()
  while IFS= read -r -d '' compose_file; do
    compose_files+=("$compose_file")
  done < <(
    find "$ROOT_DIR/examples" -mindepth 3 -maxdepth 3 -type f -path '*/docker/docker-compose.yml' -print0
  )
  if [[ ${#compose_files[@]} -ne 16 ]]; then
    echo "error: expected 16 example Compose files, found ${#compose_files[@]}" >&2
    exit 1
  fi

  echo "Checking root build-runner Compose model..."
  docker compose -f "$ROOT_DIR/compose.yml" config --quiet

  echo "Checking 16 example Compose models..."
  for compose_file in "${compose_files[@]}"; do
    docker compose -f "$compose_file" config --quiet
  done
}

run_mill() {
  if [[ ${MILL_DOCKER:-0} == 1 ]]; then
    "$ROOT_DIR/scripts/mill-docker.sh" "$@"
  else
    "$ROOT_DIR/mill" --no-server -j "$MILL_JOBS" "$@"
  fi
}

run_build_checks() {
  MILL_JOBS="${MILL_JOBS:-2}"
  if [[ ! $MILL_JOBS =~ ^[1-9][0-9]*$ ]]; then
    echo "error: MILL_JOBS must be a positive integer, got: $MILL_JOBS" >&2
    exit 1
  fi

  if [[ ${MILL_DOCKER:-0} == 1 ]]; then
    require_command docker
    local effective_jobs=2
    if [[ $MILL_JOBS -ne 2 ]]; then
      echo "Note: the Docker Mill runner fixes concurrency at 2; MILL_JOBS only affects native Mill."
    fi
  else
    require_command java
    local effective_jobs="$MILL_JOBS"
  fi

  echo "Checking Scala formatting..."
  run_mill mill.scalalib.scalafmt/checkFormatAll

  echo "Compiling all modules with at most $effective_jobs concurrent Mill tasks..."
  run_mill __.compile

  echo "Running all tests with at most $effective_jobs concurrent Mill tasks..."
  run_mill __.test
}

ACTION="${1:-all}"
if [[ $# -gt 1 ]]; then
  usage
  exit 1
fi

cd "$ROOT_DIR"

case "$ACTION" in
  static)
    run_static_checks
    ;;
  build)
    run_build_checks
    ;;
  all)
    run_static_checks
    run_build_checks
    ;;
  -h|--help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
