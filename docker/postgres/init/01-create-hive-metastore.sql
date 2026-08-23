-- ==========================================================
-- Hive Metastore
-- ==========================================================

CREATE USER hiveuser WITH PASSWORD 'hivepassword';

CREATE DATABASE metastore OWNER hiveuser;

GRANT ALL PRIVILEGES ON DATABASE metastore TO hiveuser;


-- ==========================================================
-- Hue
-- ==========================================================

CREATE USER hueuser WITH PASSWORD 'huepassword';

CREATE DATABASE hue OWNER hueuser;

GRANT ALL PRIVILEGES ON DATABASE hue TO hueuser;