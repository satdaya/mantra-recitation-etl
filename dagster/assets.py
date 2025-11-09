"""Dagster assets for mantra recitation data pipeline."""

from datetime import datetime, timedelta
from typing import Any
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import pandas as pd

from .resources import FlaskAPIResource, SnowflakeIcebergResource


@asset(
    description="Extract raw mantra recitation data from Flask API",
    group_name="ingestion"
)
def raw_mantra_recitations(
    context: AssetExecutionContext,
    flask_api: FlaskAPIResource
) -> pd.DataFrame:
    """
    Fetch raw mantra recitation data from the Flask backend API.

    This asset extracts data from the Flask API and returns it as a DataFrame
    for downstream processing.
    """
    context.log.info("Extracting mantra recitation data from API")

    # Calculate date range for incremental extraction (last 7 days by default)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    # Fetch data from API
    df = flask_api.get_recitations(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat()
    )

    # Add extraction metadata
    df['extracted_at'] = datetime.now()

    context.log.info(f"Extracted {len(df)} records from Flask API")

    return Output(
        df,
        metadata={
            "num_records": len(df),
            "columns": MetadataValue.md(", ".join(df.columns.tolist())),
            "date_range": f"{start_date.date()} to {end_date.date()}",
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )


@asset(
    description="Load raw mantra recitation data to Iceberg raw layer",
    group_name="ingestion"
)
def mantra_recitations_raw_iceberg(
    context: AssetExecutionContext,
    raw_mantra_recitations: pd.DataFrame,
    iceberg: SnowflakeIcebergResource
) -> None:
    """
    Load raw mantra recitation data to Snowflake Iceberg table (raw layer).

    This asset writes the raw API data directly to an Iceberg table in the raw schema,
    which serves as the source for SQLMesh transformations.
    """
    context.log.info("Loading raw data to Snowflake Iceberg table")

    table_name = "mantra_recitations"

    # Write to Iceberg table in append mode
    iceberg.write_to_iceberg(
        table_name=table_name,
        data=raw_mantra_recitations,
        mode="append"
    )

    context.log.info(f"Successfully loaded {len(raw_mantra_recitations)} records to Iceberg")

    return Output(
        None,
        metadata={
            "num_records_written": len(raw_mantra_recitations),
            "table_name": table_name,
            "write_mode": "append",
            "timestamp": datetime.now().isoformat()
        }
    )


@asset(
    description="Read and validate data from Iceberg staging layer",
    group_name="validation"
)
def validate_staging_data(
    context: AssetExecutionContext,
    iceberg: SnowflakeIcebergResource
) -> pd.DataFrame:
    """
    Read data from the staging Iceberg table to validate SQLMesh transformations.

    This asset demonstrates reading from Iceberg tables created by SQLMesh.
    """
    context.log.info("Reading data from staging Iceberg table for validation")

    # Read recent data from staging table (created by SQLMesh)
    # Note: This assumes SQLMesh has already run the staging model
    df = iceberg.read_from_iceberg(
        table_name="stg_mantra_recitations",
        limit=1000
    )

    # Basic validation checks
    assert len(df) > 0, "Staging table is empty"
    assert "recitation_id" in df.columns, "Missing recitation_id column"
    assert df["recitation_id"].notna().all(), "Found null recitation_ids"

    context.log.info(f"Validation passed for {len(df)} records")

    return Output(
        df,
        metadata={
            "num_records_validated": len(df),
            "validation_timestamp": datetime.now().isoformat(),
            "columns": MetadataValue.md(", ".join(df.columns.tolist())),
            "sample": MetadataValue.md(df.head(10).to_markdown())
        }
    )
