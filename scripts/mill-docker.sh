#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Preserve the checkout's absolute path so Mill artifacts work on the host too.
export DE_WORKSPACE="$ROOT_DIR"
export DOCKER_UID="${DOCKER_UID:-$(id -u)}"
export DOCKER_GID="${DOCKER_GID:-$(id -g)}"
mkdir -p "$ROOT_DIR/.docker-cache"

exec docker compose -f "$ROOT_DIR/compose.yml" run --rm mill "$@"
