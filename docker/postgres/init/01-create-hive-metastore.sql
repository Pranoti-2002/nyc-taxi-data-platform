\set ON_ERROR_STOP on

\getenv hive_user HIVE_METASTORE_USER
\getenv hive_password HIVE_METASTORE_PASSWORD
\getenv hive_db HIVE_METASTORE_DB
\getenv hue_user HUE_DB_USER
\getenv hue_password HUE_DB_PASSWORD
\getenv hue_db HUE_DB_NAME

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'hive_user', :'hive_password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'hive_db', :'hive_user') \gexec
SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', :'hive_db', :'hive_user') \gexec

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'hue_user', :'hue_password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'hue_db', :'hue_user') \gexec
SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', :'hue_db', :'hue_user') \gexec