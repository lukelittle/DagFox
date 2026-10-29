#!/bin/bash
# Runs once, on first start of the postgres container: a separate database
# (and role) for DagFox data, kept apart from Airflow's metadata database.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
    CREATE ROLE dagfox LOGIN PASSWORD '${DAGFOX_DB_PASSWORD}';
    CREATE DATABASE dagfox OWNER dagfox;
SQL
