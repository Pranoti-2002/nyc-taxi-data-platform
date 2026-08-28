# NYC Taxi Data Engineering Platform

An end-to-end data engineering platform built using NYC Yellow Taxi
data.

## Architecture

NYC Taxi Data
     |
     v
Bronze
     |
     v
Silver
     |
     v
Gold

## Technology Stack

- Python
- PySpark
- Apache Airflow 3
- PostgreSQL 15
- Docker
- Git
- SQL
- MinIo
- DuckDB
- Gemini API (AI component)

## Local Setup

Create the local environment file before starting Compose:

```bash
cp .env.example .env
```

Replace every `change_me` value and add the AWS credentials required for S3 access. Keep `.env` local and never commit it.

Start the platform with:

```bash
docker compose up --build
```

## Data Layers

### Bronze
Raw source data with minimal transformation.

### Silver
Cleaned, standardized, validated data.

### Gold
Analytics-ready and business-oriented datasets.

## Project Status

Currently setting up the project infrastructure.