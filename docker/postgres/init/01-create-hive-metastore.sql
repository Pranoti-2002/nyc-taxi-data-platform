CREATE USER hiveuser WITH PASSWORD 'hivepassword';
CREATE DATABASE metastore OWNER hiveuser;
GRANT ALL PRIVILEGES ON DATABASE metastore TO hiveuser;