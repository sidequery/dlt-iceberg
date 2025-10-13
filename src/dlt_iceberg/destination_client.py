"""
Class-based Iceberg REST destination with atomic multi-file commits.

This implementation uses dlt's JobClientBase interface to provide full lifecycle
hooks, enabling atomic commits of multiple files per table.
"""

import logging
import time
import threading
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Iterable, Tuple, Type
from types import TracebackType

import pyarrow as pa
import pyarrow.parquet as pq
from dlt.common.configuration import configspec
from dlt.common.destination import DestinationCapabilitiesContext, Destination
from dlt.common.destination.client import (
    JobClientBase,
    LoadJob,
    RunnableLoadJob,
    DestinationClientConfiguration,
)
from dlt.common.schema import Schema, TTableSchema
from dlt.common.schema.typing import TTableSchema as PreparedTableSchema
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    NoSuchTableError,
    NoSuchNamespaceError,
)

from .schema_converter import convert_dlt_to_iceberg_schema
from .partition_builder import build_partition_spec
from .schema_evolution import evolve_schema_if_needed, SchemaEvolutionError
from .schema_casting import (
    cast_table_safe,
    CastingError,
    ensure_iceberg_compatible_arrow_data,
)
from .error_handling import (
    is_retryable_error,
    log_error_with_context,
    get_user_friendly_error_message,
)
from pyiceberg.io.pyarrow import schema_to_pyarrow

logger = logging.getLogger(__name__)


# MODULE-LEVEL STATE: Accumulate files across client instances
# Key: load_id, Value: {table_name: [(table_schema, file_path, arrow_table), ...]}
# This works because dlt creates multiple client instances but they're all in the same process
_PENDING_FILES: Dict[str, Dict[str, List[Tuple[TTableSchema, str, pa.Table]]]] = {}
_PENDING_FILES_LOCK = threading.Lock()


@configspec
class IcebergRestConfiguration(DestinationClientConfiguration):
    """Configuration for Iceberg REST catalog destination."""

    destination_type: str = "iceberg_rest"

    # Catalog configuration
    catalog_uri: Optional[str] = None
    warehouse: Optional[str] = None
    namespace: str = "default"

    # Authentication (OAuth2)
    credential: Optional[str] = None
    oauth2_server_uri: Optional[str] = None
    scope: Optional[str] = "PRINCIPAL_ROLE:ALL"

    # Or Bearer token
    token: Optional[str] = None

    # AWS SigV4 (for Glue)
    sigv4_enabled: bool = False
    signing_region: Optional[str] = None
    signing_name: str = "execute-api"

    # S3 configuration
    s3_endpoint: Optional[str] = None
    s3_access_key_id: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    s3_region: Optional[str] = None

    # Retry configuration
    max_retries: int = 5
    retry_backoff_base: float = 2.0

    # Schema casting configuration
    strict_casting: bool = False

    # Merge batch size (for upsert operations to avoid memory issues)
    merge_batch_size: int = 100000


class IcebergRestLoadJob(RunnableLoadJob):
    """
    Load job that processes a single parquet file.

    This job validates the file and accumulates it for atomic commit in complete_load().
    """

    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._client: Optional["IcebergRestClient"] = None

    def run(self) -> None:
        """
        Process the file: read, validate, and register for batch commit.

        This does NOT commit to Iceberg - that happens in complete_load().
        """
        try:
            # Read parquet file
            file_path = Path(self._file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"Parquet file not found: {file_path}")

            arrow_table = pq.read_table(str(file_path))
            logger.info(f"Read {len(arrow_table)} rows from {file_path.name}")

            # Get table info from load context
            table_name = self._load_table["name"]

            # Register file for batch commit (using global state)
            self._client.register_pending_file(
                load_id=self._load_id,
                table_schema=self._load_table,
                table_name=table_name,
                file_path=str(file_path),
                arrow_table=arrow_table,
            )

            logger.info(
                f"Registered file for batch commit: {table_name}/{file_path.name}"
            )

        except Exception as e:
            logger.error(f"Failed to process file {self._file_path}: {e}")
            raise


class IcebergRestClient(JobClientBase):
    """
    Class-based Iceberg REST destination with atomic multi-file commits.

    Accumulates files during load and commits them atomically in complete_load().
    """

    def __init__(
        self,
        schema: Schema,
        config: IcebergRestConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: IcebergRestConfiguration = config

        # Catalog instance (created lazily)
        self._catalog = None

    def _get_catalog(self):
        """Get or create catalog connection."""
        if self._catalog is not None:
            return self._catalog

        # Build catalog configuration
        if self.config.catalog_uri.startswith("sqlite://") or self.config.catalog_uri.startswith("postgresql://"):
            catalog_type = "sql"
        else:
            catalog_type = "rest"

        catalog_config = {
            "type": catalog_type,
            "uri": self.config.catalog_uri,
        }

        if self.config.warehouse:
            catalog_config["warehouse"] = self.config.warehouse

        # Add authentication
        if self.config.credential and self.config.oauth2_server_uri:
            catalog_config["credential"] = self.config.credential
            catalog_config["oauth2-server-uri"] = self.config.oauth2_server_uri
            if self.config.scope:
                catalog_config["scope"] = self.config.scope
        elif self.config.token:
            catalog_config["token"] = self.config.token

        # AWS SigV4
        if self.config.sigv4_enabled:
            catalog_config["rest.sigv4-enabled"] = "true"
            if self.config.signing_region:
                catalog_config["rest.signing-region"] = self.config.signing_region
            catalog_config["rest.signing-name"] = self.config.signing_name

        # S3 configuration
        if self.config.s3_endpoint:
            catalog_config["s3.endpoint"] = self.config.s3_endpoint
        if self.config.s3_access_key_id:
            catalog_config["s3.access-key-id"] = self.config.s3_access_key_id
        if self.config.s3_secret_access_key:
            catalog_config["s3.secret-access-key"] = self.config.s3_secret_access_key
        if self.config.s3_region:
            catalog_config["s3.region"] = self.config.s3_region

        logger.info(
            f"Creating catalog connection (type={catalog_type}, uri={self.config.catalog_uri})"
        )
        self._catalog = load_catalog("dlt_catalog", **catalog_config)
        return self._catalog

    def initialize_storage(self, truncate_tables: Optional[Iterable[str]] = None) -> None:
        """Create Iceberg namespace if it doesn't exist."""
        catalog = self._get_catalog()
        namespace = self.config.namespace

        try:
            namespaces = catalog.list_namespaces()
            if (namespace,) not in namespaces:
                logger.info(f"Creating namespace {namespace}")
                catalog.create_namespace(namespace)
            else:
                logger.info(f"Namespace {namespace} already exists")
        except Exception as e:
            logger.error(f"Failed to initialize storage: {e}")
            raise

        # Handle truncation if requested
        if truncate_tables:
            for table_name in truncate_tables:
                identifier = f"{namespace}.{table_name}"
                try:
                    catalog.drop_table(identifier)
                    logger.info(f"Truncated table {identifier}")
                except NoSuchTableError:
                    pass  # Table doesn't exist, nothing to truncate

    def is_storage_initialized(self) -> bool:
        """Check if namespace exists."""
        try:
            catalog = self._get_catalog()
            namespace = self.config.namespace
            namespaces = catalog.list_namespaces()
            return (namespace,) in namespaces
        except Exception:
            return False

    def drop_storage(self) -> None:
        """Drop all tables in the namespace."""
        catalog = self._get_catalog()
        namespace = self.config.namespace

        try:
            for table_identifier in catalog.list_tables(namespace):
                catalog.drop_table(table_identifier)
                logger.info(f"Dropped table {table_identifier}")
        except NoSuchNamespaceError:
            pass  # Namespace doesn't exist

    def create_load_job(
        self,
        table: PreparedTableSchema,
        file_path: str,
        load_id: str,
        restore: bool = False,
    ) -> LoadJob:
        """
        Create a load job for a single file.

        The job will register the file for batch commit but not commit yet.
        """
        job = IcebergRestLoadJob(file_path)
        # Set reference to client so job can register files
        job._client = self
        return job

    def register_pending_file(
        self,
        load_id: str,
        table_schema: TTableSchema,
        table_name: str,
        file_path: str,
        arrow_table: pa.Table,
    ) -> None:
        """
        Register a file for batch commit in complete_load().

        Uses module-level global state since dlt creates multiple client instances.
        """
        with _PENDING_FILES_LOCK:
            if load_id not in _PENDING_FILES:
                _PENDING_FILES[load_id] = defaultdict(list)

            _PENDING_FILES[load_id][table_name].append(
                (table_schema, file_path, arrow_table)
            )

            file_count = len(_PENDING_FILES[load_id][table_name])
            logger.info(
                f"Registered file for {table_name} in load {load_id}: "
                f"{file_count} files pending"
            )

    def complete_load(self, load_id: str) -> None:
        """
        ATOMIC COMMIT: Process all accumulated files in a single transaction per table.

        Called once by dlt after all individual file jobs complete successfully.
        Reads from module-level global state since dlt creates multiple client instances.
        """
        with _PENDING_FILES_LOCK:
            if load_id not in _PENDING_FILES or not _PENDING_FILES[load_id]:
                logger.info(f"No files to commit for load {load_id}")
                return

            # Copy data and clear immediately (under lock)
            pending_files = dict(_PENDING_FILES[load_id])
            del _PENDING_FILES[load_id]

        catalog = self._get_catalog()
        namespace = self.config.namespace

        total_files = sum(len(files) for files in pending_files.values())
        logger.info(
            f"Committing {total_files} files across "
            f"{len(pending_files)} tables for load {load_id}"
        )

        # Process each table
        for table_name, file_data in pending_files.items():
            identifier = f"{namespace}.{table_name}"

            try:
                self._commit_table_files(
                    catalog=catalog,
                    identifier=identifier,
                    table_name=table_name,
                    file_data=file_data,
                )
            except Exception as e:
                logger.error(
                    f"Failed to commit files for table {identifier}: {e}",
                    exc_info=True,
                )
                raise

        logger.info(f"Load {load_id} completed successfully")

    def _commit_table_files(
        self,
        catalog,
        identifier: str,
        table_name: str,
        file_data: List[Tuple[TTableSchema, str, pa.Table]],
    ) -> None:
        """
        Commit all files for a single table atomically.

        This is where the atomic magic happens - all files go into one Iceberg snapshot.
        """
        # Get table schema and write disposition from first file
        table_schema, _, _ = file_data[0]
        write_disposition = table_schema.get("write_disposition", "append")

        logger.info(
            f"Processing {len(file_data)} files for {identifier} "
            f"with disposition {write_disposition}"
        )

        # Retry loop for transient failures
        for attempt in range(self.config.max_retries):
            try:
                # Check if table exists
                table_exists = False
                try:
                    iceberg_table = catalog.load_table(identifier)
                    table_exists = True
                    logger.info(f"Loaded existing table {identifier}")
                except NoSuchTableError:
                    logger.info(f"Table {identifier} does not exist, will create")

                # Create table if needed
                if not table_exists:
                    # Use first file's Arrow table to generate schema
                    # Apply Iceberg compatibility first so schema uses compatible types
                    first_arrow_table = ensure_iceberg_compatible_arrow_data(file_data[0][2])
                    iceberg_schema = convert_dlt_to_iceberg_schema(
                        table_schema, first_arrow_table
                    )

                    # Build partition spec
                    partition_spec = build_partition_spec(table_schema, iceberg_schema)
                    if partition_spec is None:
                        from pyiceberg.partitioning import PartitionSpec
                        partition_spec = PartitionSpec()

                    # Create table
                    logger.info(f"Creating table {identifier}")
                    iceberg_table = catalog.create_table(
                        identifier=identifier,
                        schema=iceberg_schema,
                        partition_spec=partition_spec,
                    )
                    logger.info(f"Created table {identifier} at {iceberg_table.location()}")
                else:
                    # Table exists - check if schema evolution is needed
                    first_arrow_table = ensure_iceberg_compatible_arrow_data(file_data[0][2])
                    incoming_schema = convert_dlt_to_iceberg_schema(
                        table_schema, first_arrow_table
                    )

                    schema_evolved = evolve_schema_if_needed(
                        iceberg_table,
                        incoming_schema,
                        allow_column_drops=False,
                    )
                    if schema_evolved:
                        logger.info(f"Schema evolved for table {identifier}")
                        iceberg_table = catalog.load_table(identifier)

                # Get expected schema (already has Iceberg-compatible types from creation)
                expected_schema = schema_to_pyarrow(iceberg_table.schema())

                # Combine all Arrow tables and cast to match Iceberg schema
                combined_tables = []

                for _, file_path, arrow_table in file_data:
                    # Cast to match Iceberg schema
                    # (compatibility conversions already applied when schema was created)
                    casted_table = cast_table_safe(
                        arrow_table,
                        expected_schema,
                        strict=self.config.strict_casting,
                    )
                    combined_tables.append(casted_table)

                # Concatenate all tables
                combined_table = pa.concat_tables(combined_tables)
                total_rows = len(combined_table)

                logger.info(
                    f"Combined {len(file_data)} files into {total_rows} rows "
                    f"for table {identifier}"
                )

                # ATOMIC COMMIT: Write all data in one transaction
                if write_disposition == "replace":
                    logger.info(f"Overwriting table {identifier}")
                    iceberg_table.overwrite(combined_table)
                elif write_disposition == "append":
                    logger.info(f"Appending to table {identifier}")
                    iceberg_table.append(combined_table)
                elif write_disposition == "merge":
                    # Get primary keys
                    primary_keys = table_schema.get("primary_key") or table_schema.get("x-merge-keys")

                    if not primary_keys:
                        columns = table_schema.get("columns", {})
                        primary_keys = [
                            col_name
                            for col_name, col_def in columns.items()
                            if col_def.get("primary_key") or col_def.get("x-primary-key")
                        ]

                    if not primary_keys:
                        logger.warning(
                            f"Merge disposition requires primary_key, falling back to append"
                        )
                        iceberg_table.append(combined_table)
                    else:
                        logger.info(f"Merging into table {identifier} on keys {primary_keys}")

                        # Batch upserts to avoid memory issues on large datasets
                        batch_size = self.config.merge_batch_size
                        total_updated = 0
                        total_inserted = 0

                        for batch_start in range(0, len(combined_table), batch_size):
                            batch_end = min(batch_start + batch_size, len(combined_table))
                            batch = combined_table.slice(batch_start, batch_end - batch_start)

                            logger.info(
                                f"Upserting batch {batch_start//batch_size + 1}: "
                                f"rows {batch_start} to {batch_end} ({len(batch)} rows)"
                            )

                            upsert_result = iceberg_table.upsert(
                                df=batch,
                                join_cols=primary_keys,
                                when_matched_update_all=True,
                                when_not_matched_insert_all=True,
                            )

                            total_updated += upsert_result.rows_updated
                            total_inserted += upsert_result.rows_inserted

                        logger.info(
                            f"Upsert completed: {total_updated} updated, "
                            f"{total_inserted} inserted across {(total_rows + batch_size - 1) // batch_size} batches"
                        )
                else:
                    raise ValueError(f"Unknown write disposition: {write_disposition}")

                logger.info(
                    f"Successfully committed {len(file_data)} files "
                    f"({total_rows} rows) to {identifier}"
                )
                return  # Success

            except (CastingError, SchemaEvolutionError) as e:
                # Non-retryable errors
                log_error_with_context(
                    e,
                    operation=f"commit files to {identifier}",
                    table_name=table_name,
                    include_traceback=True,
                )
                raise

            except Exception as e:
                # Check if retryable
                retryable = is_retryable_error(e)

                log_error_with_context(
                    e,
                    operation=f"commit files to {identifier}",
                    table_name=table_name,
                    attempt=attempt + 1,
                    max_attempts=self.config.max_retries,
                    include_traceback=not retryable,
                )

                if not retryable:
                    error_msg = get_user_friendly_error_message(
                        e, f"commit files to {identifier}"
                    )
                    raise RuntimeError(error_msg) from e

                if attempt >= self.config.max_retries - 1:
                    error_msg = get_user_friendly_error_message(
                        e,
                        f"commit files to {identifier} after {self.config.max_retries} attempts",
                    )
                    raise RuntimeError(error_msg) from e

                # Retry with exponential backoff
                sleep_time = self.config.retry_backoff_base ** attempt
                logger.info(
                    f"Retrying after {sleep_time}s (attempt {attempt + 2}/{self.config.max_retries})"
                )
                time.sleep(sleep_time)

    def __enter__(self) -> "IcebergRestClient":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        # Cleanup catalog connection if needed
        if self._catalog is not None:
            # PyIceberg doesn't have explicit close, but clear reference
            self._catalog = None


class iceberg_rest_class_based(Destination[IcebergRestConfiguration, "IcebergRestClient"]):
    """
    Iceberg REST destination with atomic multi-file commits.

    This uses the class-based destination API to provide full lifecycle hooks,
    enabling atomic commits of multiple files per table.

    Usage:
        pipeline = dlt.pipeline(
            destination=iceberg_rest(
                catalog_uri="http://localhost:19120/iceberg/v1/main",
                warehouse="s3://bucket/warehouse",
                namespace="default",
            )
        )
    """

    spec = IcebergRestConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        """Define capabilities for Iceberg REST destination."""
        caps = DestinationCapabilitiesContext()

        # File formats
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []

        # Merge strategies (we handle upsert ourselves)
        caps.supported_merge_strategies = ["upsert"]

        # Replace strategies
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]

        # Identifiers
        caps.escape_identifier = lambda x: f"`{x}`"
        caps.escape_literal = lambda x: f"'{x}'"
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = True

        # Precision
        caps.decimal_precision = (38, 9)
        caps.wei_precision = (38, 0)
        caps.timestamp_precision = 6

        # Limits
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.max_query_length = 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True

        # Transactions (Iceberg handles its own)
        caps.supports_ddl_transactions = False
        caps.supports_transactions = False
        caps.supports_multiple_statements = False

        return caps

    @property
    def client_class(self) -> Type["IcebergRestClient"]:
        return IcebergRestClient
