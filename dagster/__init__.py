"""Dagster pipelines for mantra recitation ETL."""

import os
from dagster import (
    Definitions,
    load_assets_from_modules,
    EnvVar,
    define_asset_job,
    ScheduleDefinition
)

from . import assets
from .resources import FlaskAPIResource, SnowflakeIcebergResource

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Define jobs
ingest_raw_data_job = define_asset_job(
    name="ingest_raw_data",
    selection=["raw_mantra_recitations", "mantra_recitations_raw_iceberg"],
    description="Extract data from Flask API and load to Iceberg raw layer"
)

validate_data_job = define_asset_job(
    name="validate_data",
    selection=["validate_staging_data"],
    description="Validate data in Iceberg staging layer after SQLMesh transformations"
)

# Define schedules
daily_ingestion_schedule = ScheduleDefinition(
    name="daily_ingestion",
    job=ingest_raw_data_job,
    cron_schedule="0 2 * * *",  # Run at 2 AM daily
    description="Daily data ingestion from Flask API to Iceberg"
)

# Define resources with environment variables
resources = {
    "flask_api": FlaskAPIResource(
        api_url=EnvVar("FLASK_API_URL"),
        api_key=EnvVar("FLASK_API_KEY"),
        timeout=int(os.getenv("FLASK_API_TIMEOUT", "30"))
    ),
    "iceberg": SnowflakeIcebergResource(
        account=EnvVar("SNOWFLAKE_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_USER"),
        password=EnvVar("SNOWFLAKE_PASSWORD"),
        warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
        database=EnvVar("SNOWFLAKE_DATABASE"),
        schema=EnvVar("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),  # Optional
        catalog_name=os.getenv("ICEBERG_CATALOG_NAME", "snowflake")
    )
}

# Main definitions object
defs = Definitions(
    assets=all_assets,
    jobs=[ingest_raw_data_job, validate_data_job],
    schedules=[daily_ingestion_schedule],
    resources=resources,
)
