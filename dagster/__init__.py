"""dagster pipelines for mantra recitation etl."""

import os
from dagster import (
    Definitions,
    load_assets_from_modules,
    EnvVar,
    define_asset_job,
    ScheduleDefinition
)

from . import assets
from .resources import SnowflakeIcebergResource

# load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# define jobs
run_transformations_job = define_asset_job(
    name="run_transformations",
    selection=["raw_data_freshness_check", "sqlmesh_run_transformations"],
    description="check raw data freshness and run sqlmesh transformations"
)

validate_all_job = define_asset_job(
    name="validate_all",
    selection=["validate_cleansed_data", "validate_marts_data"],
    description="validate data quality in cleansed and marts layers"
)

full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection="*",  # run all assets in order
    description="run full pipeline: freshness check -> transformations -> validation"
)

# define schedules
daily_transformations_schedule = ScheduleDefinition(
    name="daily_transformations",
    job=full_pipeline_job,
    cron_schedule="0 3 * * *",  # run at 3 am daily
    description="daily sqlmesh transformations and validation"
)

# define resources with environment variables
resources = {
    "iceberg": SnowflakeIcebergResource(
        account=EnvVar("SNOWFLAKE_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_USER"),
        password=EnvVar("SNOWFLAKE_PASSWORD"),
        warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
        database=EnvVar("SNOWFLAKE_DATABASE"),
        schema=EnvVar("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),  # optional
        catalog_name=os.getenv("ICEBERG_CATALOG_NAME", "snowflake")
    )
}

# main definitions object
defs = Definitions(
    assets=all_assets,
    jobs=[run_transformations_job, validate_all_job, full_pipeline_job],
    schedules=[daily_transformations_schedule],
    resources=resources,
)
