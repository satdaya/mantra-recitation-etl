# Iceberg Integration Setup Guide

This guide will help you connect both Dagster and SQLMesh to Apache Iceberg tables in Snowflake.

## Prerequisites

1. **Snowflake Account** with:
   - Iceberg table support enabled
   - External volume configured for Iceberg
   - Appropriate warehouse, database, and schema
   - User with necessary permissions

2. **Flask API** endpoint accessible with:
   - API URL
   - Authentication token/API key

3. **Python 3.9+** installed

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Copy the environment template:
```bash
cp .env.example .env
```

3. Edit `.env` with your credentials:
   - Flask API configuration
   - Snowflake credentials
   - Iceberg catalog settings

## Snowflake Iceberg Setup

### 1. Create External Volume (if not exists)

```sql
CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS = (
      (
         NAME = 'my-s3-location'
         STORAGE_PROVIDER = 'S3'
         STORAGE_BASE_URL = 's3://your-bucket/path/'
         STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/YourRole'
      )
   );
```

### 2. Create Iceberg Catalog

```sql
CREATE OR REPLACE ICEBERG CATALOG my_iceberg_catalog
   EXTERNAL_VOLUME = 'iceberg_external_volume'
   TABLE_FORMAT = 'ICEBERG';
```

### 3. Create Database and Schemas

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS mantra_recitation;

-- Create schemas for each layer
CREATE SCHEMA IF NOT EXISTS mantra_recitation.raw;
CREATE SCHEMA IF NOT EXISTS mantra_recitation.staging;
CREATE SCHEMA IF NOT EXISTS mantra_recitation.marts;
```

### 4. Create Raw Iceberg Table

```sql
CREATE OR REPLACE ICEBERG TABLE mantra_recitation.raw.mantra_recitations (
    recitation_id VARCHAR,
    user_id VARCHAR,
    mantra_name VARCHAR,
    repetitions INTEGER,
    duration_seconds INTEGER,
    event_timestamp TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    device_type VARCHAR,
    app_version VARCHAR,
    extracted_at TIMESTAMP
)
EXTERNAL_VOLUME = 'iceberg_external_volume'
CATALOG = 'my_iceberg_catalog'
BASE_LOCATION = 'mantra_recitations/';
```

## SQLMesh Configuration

### 1. Update SQLMesh config

Edit `sqlmesh/config.yaml` and replace placeholders:
- `YOUR_ACCOUNT` → Your Snowflake account identifier
- `YOUR_USER` → Your Snowflake username
- `YOUR_PASSWORD` → Your Snowflake password
- `YOUR_WAREHOUSE` → Your Snowflake warehouse
- `YOUR_DATABASE` → `mantra_recitation`
- `YOUR_SCHEMA` → `raw` (or your target schema)
- `YOUR_EXTERNAL_VOLUME` → Your external volume name
- `YOUR_CATALOG` → Your Iceberg catalog name

### 2. Initialize SQLMesh

```bash
cd sqlmesh
sqlmesh init
```

### 3. Run SQLMesh Models

```bash
# Plan the changes
sqlmesh plan

# Apply the plan
sqlmesh apply

# Run transformations
sqlmesh run
```

## Dagster Configuration

### 1. Environment Variables

Ensure your `.env` file is properly configured with all required variables.

### 2. Start Dagster

```bash
# Development mode with auto-reload
dagster dev

# Or using dagster-webserver
dagster-webserver -f dagster/__init__.py
```

### 3. Access Dagster UI

Open your browser to `http://localhost:3000`

### 4. Materialize Assets

In the Dagster UI:
1. Navigate to **Assets**
2. Select the assets you want to materialize:
   - `raw_mantra_recitations` - Extracts from Flask API
   - `mantra_recitations_raw_iceberg` - Loads to Iceberg raw layer
   - `validate_staging_data` - Validates SQLMesh transformations
3. Click **Materialize**

## Architecture Overview

### Data Flow

```
Flask API → Dagster (Extraction) → Iceberg Raw Layer → SQLMesh (Transformations) → Iceberg Staging/Marts → Dagster (Validation)
```

### Layers

1. **Raw Layer** (`raw` schema)
   - Raw data from source systems
   - Managed by Dagster ingestion jobs
   - Iceberg tables for efficient storage

2. **Staging Layer** (`staging` schema)
   - Cleaned and validated data
   - Managed by SQLMesh models
   - Incremental models with time-based partitioning

3. **Marts Layer** (`marts` schema)
   - Business-level aggregations
   - Managed by SQLMesh models
   - Ready for analytics and reporting

## SQLMesh Models

### Available Models

1. **stg_mantra_recitations** (`sqlmesh/models/staging/stg_mantra_recitations.sql`)
   - Staging model for raw recitation data
   - Incremental by timestamp
   - Includes data quality audits

2. **mantra_recitations_daily** (`sqlmesh/models/marts/mantra_recitations_daily.sql`)
   - Daily aggregations by user and mantra
   - Incremental by date
   - Metrics: counts, repetitions, duration

3. **user_mantra_summary** (`sqlmesh/models/marts/user_mantra_summary.sql`)
   - Lifetime user summary by mantra
   - Full refresh model
   - Activity tracking and engagement metrics

## Dagster Assets

### Ingestion Assets

- **raw_mantra_recitations**: Extracts data from Flask API
- **mantra_recitations_raw_iceberg**: Loads to Iceberg raw layer

### Validation Assets

- **validate_staging_data**: Validates SQLMesh transformations

## Jobs and Schedules

### Jobs

1. **ingest_raw_data**: Extract and load raw data
2. **validate_data**: Validate staging data

### Schedules

- **daily_ingestion**: Runs at 2 AM daily

## Monitoring and Observability

### Dagster UI

- Asset lineage visualization
- Run history and logs
- Asset metadata and previews

### SQLMesh

```bash
# View model lineage
sqlmesh dag

# Check model status
sqlmesh info

# View audit results
sqlmesh audit
```

## Iceberg Benefits

1. **ACID Transactions**: Full transactional support
2. **Time Travel**: Query historical data snapshots
3. **Schema Evolution**: Add/remove columns without rewriting data
4. **Partitioning**: Automatic partition pruning for performance
5. **Metadata Management**: Efficient metadata operations

### Time Travel Example

```sql
-- Query data as of a specific timestamp
SELECT * FROM mantra_recitation.staging.stg_mantra_recitations
AT(TIMESTAMP => '2024-01-01 00:00:00'::TIMESTAMP);

-- Query data as of a specific snapshot
SELECT * FROM mantra_recitation.staging.stg_mantra_recitations
AT(SNAPSHOT => '1234567890');
```

## Troubleshooting

### Connection Issues

- Verify Snowflake credentials in `.env`
- Check network connectivity to Snowflake
- Ensure warehouse is running

### Iceberg Table Issues

- Verify external volume permissions
- Check catalog configuration
- Ensure base location is accessible

### SQLMesh Issues

- Run `sqlmesh audit` to check data quality
- Review model definitions for syntax errors
- Check gateway configuration in `config.yaml`

### Dagster Issues

- Check logs in Dagster UI
- Verify resource configurations
- Test connections independently

## Next Steps

1. Customize SQLMesh models for your specific use case
2. Add more Dagster assets for additional data sources
3. Configure alerts and monitoring
4. Set up CI/CD for deployments
5. Add data quality checks and tests

## Resources

- [Snowflake Iceberg Tables Documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [SQLMesh Documentation](https://sqlmesh.readthedocs.io/)
- [Dagster Documentation](https://docs.dagster.io/)
