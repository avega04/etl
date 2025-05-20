#!/bin/bash

# Default values
DB_NAME=${DB_NAME:-"qqcatalyst_etl"}
DB_USER=${DB_USER:-"postgres"}
DB_PASSWORD=${DB_PASSWORD:-"postgres"}
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5432"}

# Install dblink extension if not exists (requires superuser privileges)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "CREATE EXTENSION IF NOT EXISTS dblink;" 2>/dev/null

# Create database if it doesn't exist
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1 || \
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "CREATE DATABASE $DB_NAME"

# Run the schema creation script
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f "$(dirname "$0")/init_db.sql"

echo "Database initialization completed." 