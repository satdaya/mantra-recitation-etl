"""Dagster resources for connecting to external systems."""

from dagster import ConfigurableResource
from pydantic import Field


class FlaskAPIResource(ConfigurableResource):
    """Resource for connecting to Flask backend API."""

    api_url: str = Field(description="Base URL for Flask API")
    api_key: str = Field(description="API key for authentication")

    def get_recitations(self, **kwargs):
        """Fetch recitation data from API."""
        # TODO: Implement API client
        pass


class SnowflakeIcebergResource(ConfigurableResource):
    """Resource for connecting to Snowflake Iceberg tables."""

    account: str = Field(description="Snowflake account")
    user: str = Field(description="Snowflake user")
    password: str = Field(description="Snowflake password")
    warehouse: str = Field(description="Snowflake warehouse")
    database: str = Field(description="Snowflake database")
    schema_name: str = Field(description="Snowflake schema", alias="schema")

    def write_to_iceberg(self, table_name: str, data):
        """Write data to Iceberg table."""
        # TODO: Implement Iceberg write logic
        pass
