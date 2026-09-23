#!/usr/bin/env bash
# Creates the Airflow metadata database alongside the smartgrid database.
# This script runs BEFORE 01_init.sql (files execute in filename order).
# The postgres container only knows POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB.
set -e

AIRFLOW_DB="${POSTGRES_AIRFLOW_DB:-airflow}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE "${AIRFLOW_DB}"'
    WHERE NOT EXISTS (
        SELECT FROM pg_database WHERE datname = '${AIRFLOW_DB}'
    )\gexec
EOSQL

echo "Airflow database '${AIRFLOW_DB}' ready."
