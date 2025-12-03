"""Dagster resources for connecting to external systems."""

from typing import Any, Optional
import polars as pl
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import pyarrow as pa


class SnowflakeIcebergResource(ConfigurableResource):
    """Resource for connecting to Snowflake Iceberg tables via PyIceberg."""

    account: str = Field(description="Snowflake account identifier")
    user: str = Field(description="Snowflake user")
    password: str = Field(description="Snowflake password")
    warehouse: str = Field(description="Snowflake warehouse")
    database: str = Field(description="Snowflake database")
    schema_name: str = Field(description="Snowflake schema", alias="schema")
    role: Optional[str] = Field(default=None, description="Snowflake role")
    catalog_name: str = Field(default="snowflake", description="Iceberg catalog name")

    def _get_catalog(self):
        """Initialize and return Iceberg catalog connection to Snowflake."""
        catalog_config = {
            "type": "snowflake",
            "uri": f"https://{self.account}.snowflakecomputing.com",
            "warehouse": self.warehouse,
            "user": self.user,
            "password": self.password,
        }

        if self.role:
            catalog_config["role"] = self.role

        return load_catalog(self.catalog_name, **catalog_config)

    def get_table(self, table_name: str) -> Table:
        """
        Get an Iceberg table reference.

        Args:
            table_name: Fully qualified table name (database.schema.table)

        Returns:
            PyIceberg Table object
        """
        catalog = self._get_catalog()
        full_table_name = f"{self.database}.{self.schema_name}.{table_name}"
        return catalog.load_table(full_table_name)

    def write_to_iceberg(
        self,
        table_name: str,
        data: pl.DataFrame,
        mode: str = "append"
    ) -> None:
        """
        Write data to an Iceberg table.

        Args:
            table_name: Table name (without database/schema prefix)
            data: Polars DataFrame to write
            mode: Write mode - 'append' or 'overwrite'
        """
        logger = get_dagster_logger()

        try:
            # Get table reference
            table = self.get_table(table_name)

            # Convert Polars DataFrame to PyArrow table
            arrow_table = data.to_arrow()

            # Write to Iceberg
            if mode == "overwrite":
                table.overwrite(arrow_table)
                logger.info(f"overwrote {len(data)} records to {table_name}")
            else:  # append
                table.append(arrow_table)
                logger.info(f"appended {len(data)} records to {table_name}")

        except Exception as e:
            logger.error(f"failed to write to iceberg table {table_name}: {str(e)}")
            raise

    def read_from_iceberg(
        self,
        table_name: str,
        filters: Optional[Any] = None,
        limit: Optional[int] = None
    ) -> pl.DataFrame:
        """
        Read data from an Iceberg table.

        Args:
            table_name: Table name (without database/schema prefix)
            filters: PyIceberg filters to apply
            limit: Maximum number of rows to return

        Returns:
            Polars DataFrame containing table data
        """
        logger = get_dagster_logger()

        try:
            table = self.get_table(table_name)
            scan = table.scan()

            if filters:
                scan = scan.filter(filters)

            if limit:
                scan = scan.limit(limit)

            arrow_table = scan.to_arrow()
            df = pl.from_arrow(arrow_table)

            logger.info(f"read {len(df)} records from {table_name}")
            return df

        except Exception as e:
            logger.error(f"failed to read from iceberg table {table_name}: {str(e)}")
            raise
