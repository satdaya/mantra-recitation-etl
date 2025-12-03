"""dagster assets for mantra recitation data pipeline."""

from datetime import datetime, timedelta
import subprocess
import polars as pl
from dagster import asset, AssetExecutionContext, Output, MetadataValue
from .resources import SnowflakeIcebergResource


@asset(
    description="monitor raw iceberg table for data freshness",
    group_name="monitoring"
)
def raw_data_freshness_check(
    context: AssetExecutionContext,
    iceberg: SnowflakeIcebergResource
) -> dict:
    """
    check the freshness of data in the raw iceberg table.

    this asset monitors when data was last written to the raw layer
    to ensure the flask auto-push is working correctly.
    """
    context.log.info("checking raw data freshness")

    # read latest records from raw table
    df = iceberg.read_from_iceberg(
        table_name="mantra_recitations",
        limit=10
    )

    if len(df) == 0:
        context.log.warning("raw table is empty!")
        return Output(
            {"status": "empty", "record_count": 0},
            metadata={
                "status": "empty",
                "record_count": 0
            }
        )

    # get most recent created_at timestamp
    latest_record = df.select(pl.col("created_at").max()).item()
    hours_since_last_record = (datetime.now() - latest_record).total_seconds() / 3600

    context.log.info(f"latest record was {hours_since_last_record:.2f} hours ago")

    # alert if data is stale (>24 hours)
    status = "fresh" if hours_since_last_record < 24 else "stale"

    return Output(
        {
            "status": status,
            "latest_record_time": latest_record.isoformat(),
            "hours_since_last": hours_since_last_record,
            "record_count": len(df)
        },
        metadata={
            "status": status,
            "latest_record": latest_record.isoformat(),
            "hours_since_last": f"{hours_since_last_record:.2f}",
            "total_records": len(df)
        }
    )


@asset(
    description="run sqlmesh plan and apply transformations",
    group_name="transformation"
)
def sqlmesh_run_transformations(
    context: AssetExecutionContext,
    raw_data_freshness_check: dict
) -> dict:
    """
    execute sqlmesh to run all transformation models.

    this orchestrates the sqlmesh plan/apply process to transform
    raw data into cleansed and marts layers.
    """
    context.log.info("running sqlmesh transformations")

    # only run if raw data is fresh
    if raw_data_freshness_check.get("status") == "empty":
        context.log.warning("skipping sqlmesh run - raw table is empty")
        return Output(
            {"status": "skipped", "reason": "empty raw table"},
            metadata={"status": "skipped"}
        )

    try:
        # run sqlmesh plan with auto-apply
        result = subprocess.run(
            ["sqlmesh", "plan", "--auto-apply"],
            cwd="sqlmesh",
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        context.log.info(f"sqlmesh output:\n{result.stdout}")

        if result.returncode != 0:
            context.log.error(f"sqlmesh failed:\n{result.stderr}")
            raise Exception(f"sqlmesh plan failed: {result.stderr}")

        return Output(
            {
                "status": "success",
                "output": result.stdout,
                "timestamp": datetime.now().isoformat()
            },
            metadata={
                "status": "success",
                "timestamp": datetime.now().isoformat()
            }
        )

    except subprocess.TimeoutExpired:
        context.log.error("sqlmesh run timed out after 5 minutes")
        raise
    except Exception as e:
        context.log.error(f"sqlmesh run failed: {str(e)}")
        raise


@asset(
    description="validate cleansed layer data quality",
    group_name="validation"
)
def validate_cleansed_data(
    context: AssetExecutionContext,
    iceberg: SnowflakeIcebergResource,
    sqlmesh_run_transformations: dict
) -> pl.DataFrame:
    """
    validate data quality in the cleansed iceberg table.

    performs data quality checks on the cleansed layer after sqlmesh
    transformations have completed.
    """
    context.log.info("validating cleansed data quality")

    # read recent data from cleansed table
    df = iceberg.read_from_iceberg(
        table_name="cln_mantra_recitation",
        limit=1000
    )

    # data quality checks
    assert len(df) > 0, "cleansed table is empty"
    assert "id" in df.columns, "missing id column"

    # check for nulls in required columns
    null_checks = {
        "id": df.filter(pl.col("id").is_null()).height,
        "user_id": df.filter(pl.col("user_id").is_null()).height,
        "mantra_name": df.filter(pl.col("mantra_name").is_null()).height,
        "recitation_timestamp": df.filter(pl.col("recitation_timestamp").is_null()).height
    }

    for col, null_count in null_checks.items():
        if null_count > 0:
            context.log.warning(f"found {null_count} null values in {col}")
        assert null_count == 0, f"found {null_count} null values in {col}"

    # check for duplicates
    duplicate_count = df.group_by("id").len().filter(pl.col("len") > 1).height
    assert duplicate_count == 0, f"found {duplicate_count} duplicate ids"

    context.log.info(f"validation passed for {len(df)} records")

    return Output(
        df,
        metadata={
            "num_records_validated": len(df),
            "validation_timestamp": datetime.now().isoformat(),
            "columns": MetadataValue.md(", ".join(df.columns)),
            "null_checks_passed": str(null_checks),
            "duplicate_count": duplicate_count
        }
    )


@asset(
    description="validate marts layer aggregations",
    group_name="validation"
)
def validate_marts_data(
    context: AssetExecutionContext,
    iceberg: SnowflakeIcebergResource,
    validate_cleansed_data: pl.DataFrame
) -> pl.DataFrame:
    """
    validate marts layer aggregations.

    checks that daily aggregations are computed correctly.
    """
    context.log.info("validating marts data quality")

    # read from daily aggregation table
    df = iceberg.read_from_iceberg(
        table_name="mantra_recitations_daily",
        limit=100
    )

    if len(df) == 0:
        context.log.warning("marts table is empty")
        return Output(
            df,
            metadata={"status": "empty", "num_records": 0}
        )

    # validate aggregation columns exist
    required_cols = ["user_id", "mantra_name", "event_date", "recitation_count"]
    for col in required_cols:
        assert col in df.columns, f"missing required column: {col}"

    # validate counts are positive
    negative_counts = df.filter(pl.col("recitation_count") < 0).height
    assert negative_counts == 0, f"found {negative_counts} negative recitation counts"

    context.log.info(f"marts validation passed for {len(df)} records")

    return Output(
        df,
        metadata={
            "num_records_validated": len(df),
            "validation_timestamp": datetime.now().isoformat(),
            "columns": MetadataValue.md(", ".join(df.columns))
        }
    )
