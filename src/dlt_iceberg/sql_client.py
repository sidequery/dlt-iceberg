"""
DuckDB SQL client for Iceberg tables.

Provides queryable access to Iceberg tables via DuckDB views using iceberg_scan().
This enables pipeline.dataset() to work with the Iceberg destination.
"""

import logging
from typing import TYPE_CHECKING, Any, List, Tuple

import duckdb
from packaging.version import Version

from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.destination.typing import PreparedTableSchema
from dlt.destinations.impl.duckdb.sql_client import WithTableScanners
from dlt.destinations.impl.duckdb.factory import DuckDbCredentials
from dlt.destinations.sql_client import raise_database_error

if TYPE_CHECKING:
    from dlt_iceberg.destination_client import IcebergRestClient
else:
    IcebergRestClient = Any

logger = logging.getLogger(__name__)


class IcebergSqlClient(WithTableScanners):
    """SQL client that maps Iceberg tables as DuckDB views.

    Creates DuckDB views using iceberg_scan() that point to the Iceberg
    table metadata, enabling SQL queries over Iceberg tables.
    """

    def __init__(
        self,
        remote_client: "IcebergRestClient",
        dataset_name: str,
        cache_db: DuckDbCredentials = None,
        persist_secrets: bool = False,
    ) -> None:
        super().__init__(remote_client, dataset_name, cache_db, persist_secrets=persist_secrets)
        self.remote_client: "IcebergRestClient" = remote_client
        self.iceberg_initialized = False

    def can_create_view(self, table_schema: PreparedTableSchema) -> bool:
        """Check if a view can be created for this table."""
        # All Iceberg tables can have views created
        return True

    def should_replace_view(self, view_name: str, table_schema: PreparedTableSchema) -> bool:
        """Determine if view should be replaced to get fresh data."""
        # Always replace to get latest snapshot
        # TODO: Could optimize with configuration option
        return True

    @raise_database_error
    def open_connection(self) -> duckdb.DuckDBPyConnection:
        """Open DuckDB connection and set up for Iceberg access."""
        with self.credentials.conn_pool._conn_lock:
            first_connection = self.credentials.conn_pool.never_borrowed
            super().open_connection()

        if first_connection:
            # Set up storage credentials if available
            self._setup_storage_credentials()

        return self._conn

    def _setup_storage_credentials(self) -> None:
        """Set up DuckDB secrets for storage access."""
        config = self.remote_client.config

        # S3 credentials
        if config.s3_access_key_id and config.s3_secret_access_key:
            secret_sql = f"""
                CREATE SECRET IF NOT EXISTS iceberg_s3_secret (
                    TYPE S3,
                    KEY_ID '{config.s3_access_key_id}',
                    SECRET '{config.s3_secret_access_key}'
            """
            if config.s3_region:
                secret_sql += f", REGION '{config.s3_region}'"
            if config.s3_endpoint:
                # Handle endpoint URL
                endpoint = config.s3_endpoint
                if endpoint.startswith("http://"):
                    secret_sql += f", ENDPOINT '{endpoint[7:]}', USE_SSL false"
                elif endpoint.startswith("https://"):
                    secret_sql += f", ENDPOINT '{endpoint[8:]}'"
                else:
                    secret_sql += f", ENDPOINT '{endpoint}'"
            secret_sql += ")"

            try:
                self._conn.execute(secret_sql)
                logger.info("Created DuckDB S3 secret for Iceberg access")
            except Exception as e:
                logger.warning(f"Failed to create S3 secret: {e}")

    @raise_database_error
    def create_view(self, view_name: str, table_schema: PreparedTableSchema) -> None:
        """Create a DuckDB view for an Iceberg table using iceberg_scan()."""
        table_name = table_schema["name"]

        # Get table location from catalog
        try:
            table_location = self.remote_client.get_open_table_location("iceberg", table_name)
        except Exception as e:
            raise DestinationUndefinedEntity(table_name) from e

        if not table_location:
            raise DestinationUndefinedEntity(table_name)

        # Ensure iceberg extension is loaded
        if not self.iceberg_initialized:
            self._setup_iceberg(self._conn)
            self.iceberg_initialized = True

        # Get metadata file path
        metadata_file = self._get_metadata_file(table_location)
        if not metadata_file:
            raise DestinationUndefinedEntity(table_name)

        # Check for gzip compression
        compression = ""
        if ".gz." in metadata_file:
            compression = ", metadata_compression_codec = 'gzip'"

        # Scanner options based on DuckDB version
        if Version(duckdb.__version__) > Version("1.3.0"):
            scanner_options = "union_by_name=true"
        else:
            scanner_options = "skip_schema_inference=false"

        # Build column selection from schema
        columns = list(self.schema.get_table_columns(table_name).keys())
        escaped_columns = [self.escape_column_name(c) for c in columns]
        columns_sql = ", ".join(escaped_columns) if columns else "*"

        # Create the view
        view_sql = f"""
            CREATE OR REPLACE VIEW {self.escape_column_name(view_name)} AS
            SELECT {columns_sql}
            FROM iceberg_scan('{metadata_file}'{compression}, {scanner_options})
        """

        logger.info(f"Creating view {view_name} for Iceberg table {table_name}")
        self._conn.execute(view_sql)

    def _get_metadata_file(self, table_location: str) -> str:
        """Get the latest metadata file path for an Iceberg table.

        Args:
            table_location: Base location of the Iceberg table

        Returns:
            Path to the latest metadata JSON file
        """
        # Try to get metadata from the catalog directly
        try:
            # Load the table through PyIceberg to get metadata location
            catalog = self.remote_client._get_catalog()
            namespace = self.remote_client.config.namespace

            # Extract table name from location
            table_name = table_location.rstrip("/").split("/")[-1]
            identifier = f"{namespace}.{table_name}"

            iceberg_table = catalog.load_table(identifier)
            metadata_location = iceberg_table.metadata_location

            if metadata_location:
                return metadata_location
        except Exception as e:
            logger.debug(f"Could not get metadata from catalog: {e}")

        # Fallback: scan metadata directory
        metadata_path = f"{table_location.rstrip('/')}/metadata"
        return self._find_latest_metadata(metadata_path)

    def _find_latest_metadata(self, metadata_path: str) -> str:
        """Find the latest metadata file in a metadata directory.

        Args:
            metadata_path: Path to the metadata directory

        Returns:
            Path to the latest metadata file or empty string
        """
        import os
        from urllib.parse import urlparse

        parsed = urlparse(metadata_path)

        # Only support local filesystem for fallback scan
        if parsed.scheme and parsed.scheme not in ("file", ""):
            logger.warning(
                f"Cannot scan metadata directory for {parsed.scheme} storage, "
                "use catalog for metadata location"
            )
            return ""

        local_path = parsed.path if parsed.scheme == "file" else metadata_path

        if not os.path.exists(local_path):
            return ""

        # Find latest metadata file
        metadata_files = [
            f for f in os.listdir(local_path)
            if f.endswith(".metadata.json")
        ]

        if not metadata_files:
            return ""

        # Sort by version number (format: v1.metadata.json, v2.metadata.json, etc.)
        # or by timestamp for UUID-based names
        metadata_files.sort(reverse=True)

        return f"{metadata_path}/{metadata_files[0]}"
