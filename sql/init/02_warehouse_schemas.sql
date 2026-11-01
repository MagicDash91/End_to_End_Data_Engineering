-- Run against the warehouse database. One schema per medallion layer:
--   bronze : raw copies of the source tables, append-only, written by Airbyte (or the python loader)
--   silver : cleaned, typed, de-duplicated entities, written by Spark
--   gold   : dimensional model + business marts, built by dbt
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA bronze IS 'Raw, append-only copies of source tables (Airbyte incremental append)';
COMMENT ON SCHEMA silver IS 'Cleaned, de-duplicated, validated entities (Spark)';
COMMENT ON SCHEMA gold   IS 'Dimensions, facts and marts for analytics (dbt)';
