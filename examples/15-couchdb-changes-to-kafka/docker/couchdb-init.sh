#!/bin/sh
# Prepares a freshly started Apache CouchDB for this example.
#
# Two things have to happen before anything can connect:
#
#   1. A single-node CouchDB starts without its own system databases. Until `_users`, `_replicator` and
#      `_global_changes` exist, CouchDB reports itself as unconfigured and logs a warning on every request.
#   2. The `catalogue` database this example follows has to exist, and it is seeded with the product documents that
#      were "already there" before the connector was written - so the first run of the connector replays real history
#      from sequence 0 rather than starting on an empty feed.
#
# Re-running the container is safe: creating a database that already exists answers 412, and a `_bulk_docs` write of
# documents that already exist is reported per document as a conflict, which leaves the stored catalogue untouched.

set -eu

COUCH="http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couchdb:5984"

create_database() {
  status=$(curl --silent --output /dev/null --write-out '%{http_code}' -X PUT "${COUCH}/$1")
  case "${status}" in
    201 | 202 | 412) echo "database $1 ready (HTTP ${status})" ;;
    *)
      echo "could not create database $1 (HTTP ${status})" >&2
      exit 1
      ;;
  esac
}

for database in _users _replicator _global_changes catalogue; do
  create_database "${database}"
done

echo "seeding the catalogue"
curl --silent --fail -X POST "${COUCH}/catalogue/_bulk_docs" \
  -H 'Content-Type: application/json' \
  --data @/seed/catalogue-seed.json > /dev/null

echo "CouchDB is ready"
