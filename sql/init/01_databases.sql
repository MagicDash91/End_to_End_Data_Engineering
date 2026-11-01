-- Idempotent: safe to run on every start, locally and against RDS.
-- Run with: psql -v ON_ERROR_STOP=1 -d postgres -v shop_db=shop -v warehouse_db=warehouse -f 01_databases.sql

SELECT format('CREATE DATABASE %I', :'shop_db')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'shop_db')\gexec

SELECT format('CREATE DATABASE %I', :'warehouse_db')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'warehouse_db')\gexec
