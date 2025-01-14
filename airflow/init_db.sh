
set -e


psql -U "$POSTGRES_USER" -d postgres -c "SELECT 1 FROM pg_database WHERE datname = 'energy_db'" | grep -q 1 || \
psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE energy_db"

psql -U "$POSTGRES_USER" -d postgres -c "SELECT 1 FROM pg_database WHERE datname = 'staging_db'" | grep -q 1 || \
psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE staging_db"


psql -U "$POSTGRES_USER" -d energy_db -f /docker-entrypoint-initdb.d/init_energy_db.sql

psql -U "$POSTGRES_USER" -d staging_db -f /docker-entrypoint-initdb.d/init_staging_db.sql
