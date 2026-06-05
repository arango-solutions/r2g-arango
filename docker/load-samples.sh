#!/bin/bash
# Postgres init hook: loads the extra demo databases (Chinook, Pagila) into the
# same instance as Northwind. Runs only on a fresh data volume. Sample SQL is
# mounted read-only at /samples. Errors are non-fatal (e.g. OWNER TO postgres)
# so a missing role doesn't abort the demo seed.
set -u

PSQL=(psql -v ON_ERROR_STOP=0 -U "$POSTGRES_USER")

# Pagila assigns object ownership to a "postgres" role; create it so the
# ALTER ... OWNER TO postgres statements succeed cleanly.
"${PSQL[@]}" -d postgres -c "CREATE ROLE postgres LOGIN SUPERUSER" 2>/dev/null || true

# Chinook self-creates its database (CREATE DATABASE chinook; \c chinook).
if [ -f /samples/chinook/chinook.sql ]; then
  echo "[load-samples] loading Chinook"
  "${PSQL[@]}" -d postgres -f /samples/chinook/chinook.sql
fi

# Pagila ships schema + data separately and expects the DB to exist.
if [ -f /samples/pagila/pagila-schema.sql ]; then
  echo "[load-samples] creating + loading Pagila"
  "${PSQL[@]}" -d postgres -c "CREATE DATABASE pagila" || true
  "${PSQL[@]}" -d pagila -f /samples/pagila/pagila-schema.sql
  "${PSQL[@]}" -d pagila -f /samples/pagila/pagila-data.sql
fi
