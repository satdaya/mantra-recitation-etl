"""Dagster resources for connecting to external systems."""

from typing import Any, Dict, Optional
import requests
import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import pyarrow as pa


class FlaskAPIResource(ConfigurableResource):
    """Resource for connecting to Flask backend API."""

    api_url: str = Field(description="Base URL for Flask API")
    api_key: str = Field(description="API key for authentication")
    timeout: int = Field(default=30, description="Request timeout in seconds")

    def get_recitations(self, endpoint: str = "/api/recitations", **params) -> pd.DataFrame:
        """
        Fetch recitation data from API.

        Args:
            endpoint: API endpoint path
            **params: Query parameters to pass to the API

        Returns:
            DataFrame containing recitation data
        """
        logger = get_dagster_logger()
        url = f"{self.api_url}{endpoint}"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            # Convert to DataFrame
            df = pd.DataFrame(data)
            logger.info(f"Fetched {len(df)} records from {url}")
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data from API: {str(e)}")
            raise


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
        data: pd.DataFrame,
        mode: str = "append"
    ) -> None:
        """
        Write data to an Iceberg table.

        Args:
            table_name: Table name (without database/schema prefix)
            data: DataFrame to write
            mode: Write mode - 'append' or 'overwrite'
        """
        logger = get_dagster_logger()

        try:
            # Get table reference
            table = self.get_table(table_name)

            # Convert DataFrame to PyArrow table
            arrow_table = pa.Table.from_pandas(data)

            # Write to Iceberg
            if mode == "overwrite":
                table.overwrite(arrow_table)
                logger.info(f"Overwrote {len(data)} records to {table_name}")
            else:  # append
                table.append(arrow_table)
                logger.info(f"Appended {len(data)} records to {table_name}")

        except Exception as e:
            logger.error(f"Failed to write to Iceberg table {table_name}: {str(e)}")
            raise

    def read_from_iceberg(
        self,
        table_name: str,
        filters: Optional[Any] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read data from an Iceberg table.

        Args:
            table_name: Table name (without database/schema prefix)
            filters: PyIceberg filters to apply
            limit: Maximum number of rows to return

        Returns:
            DataFrame containing table data
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
            df = arrow_table.to_pandas()

            logger.info(f"Read {len(df)} records from {table_name}")
            return df

        except Exception as e:
            logger.error(f"Failed to read from Iceberg table {table_name}: {str(e)}")
            raise
