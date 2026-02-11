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
from typing import Any, Dict, List, Optional, Iterable, Tuple, Type
from types import TracebackType

import pyarrow as pa
import pyarrow.parquet as pq
from dlt.common import pendulum
from dlt.common.configuration import configspec
from dlt.common.destination import DestinationCapabilitiesContext, Destination
from dlt.common.destination.client import (
    JobClientBase,
    LoadJob,
    RunnableLoadJob,
    DestinationClientConfiguration,
    SupportsOpenTables,
)
from dlt.common.schema.typing import TTableFormat
from dlt.destinations.sql_client import WithSqlClient, SqlClientBase
from dlt.common.schema import Schema, TTableSchema
from dlt.common.schema.typing import TTableSchema as PreparedTableSchema
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    NoSuchTableError,
    NoSuchNamespaceError,
)
from pyiceberg.expressions import EqualTo

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
    merge_batch_size: int = 500000

    # Table location layout - controls directory structure for table files
    # Supports patterns: {namespace}, {dataset_name}, {table_name}
    # Example: "{namespace}/{table_name}" or "warehouse/{dataset_name}/{table_name}"
    table_location_layout: Optional[str] = None

    # Register tables found in storage but missing from catalog (backward compatibility)
    register_new_tables: bool = False

    # Hard delete column name - rows with this column set will be deleted during merge
    # Set to None to disable hard delete
    hard_delete_column: Optional[str] = "_dlt_deleted_at"


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


class IcebergRestClient(JobClientBase, WithSqlClient, SupportsOpenTables):
    """
    Class-based Iceberg REST destination with atomic multi-file commits.

    Accumulates files during load and commits them atomically in complete_load().
    Implements WithSqlClient and SupportsOpenTables for pipeline.dataset() support.
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
        # SQL client instance (created lazily)
        self._sql_client = None

    # ---- WithSqlClient interface ----

    @property
    def sql_client(self) -> SqlClientBase:
        """Get or create the DuckDB SQL client for dataset access."""
        if self._sql_client is None:
            from .sql_client import IcebergSqlClient
            self._sql_client = IcebergSqlClient(
                remote_client=self,
                dataset_name=self.config.namespace,
            )
        return self._sql_client

    @property
    def sql_client_class(self) -> Type[SqlClientBase]:
        """Return the SQL client class."""
        from .sql_client import IcebergSqlClient
        return IcebergSqlClient

    # ---- SupportsOpenTables interface ----

    def get_open_table_catalog(self, table_format: TTableFormat, catalog_name: str = None) -> Any:
        """Get the PyIceberg catalog for accessing table metadata."""
        if table_format != "iceberg":
            raise ValueError(f"Unsupported table format: {table_format}")
        return self._get_catalog()

    def get_open_table_location(self, table_format: TTableFormat, table_name: str) -> str:
        """Get the storage location for an Iceberg table."""
        if table_format != "iceberg":
            raise ValueError(f"Unsupported table format: {table_format}")

        # Try to get location from catalog
        try:
            catalog = self._get_catalog()
            identifier = f"{self.config.namespace}.{table_name}"
            iceberg_table = catalog.load_table(identifier)
            return iceberg_table.location()
        except NoSuchTableError:
            # Table doesn't exist yet, compute expected location
            location = self._get_table_location(table_name)
            if location:
                return location
            # Fallback to default warehouse location
            warehouse = self.config.warehouse or ""
            if warehouse and not warehouse.endswith("/"):
                warehouse += "/"
            return f"{warehouse}{self.config.namespace}/{table_name}"

    def load_open_table(self, table_format: TTableFormat, table_name: str, **kwargs: Any) -> Any:
        """Load and return a PyIceberg Table object."""
        if table_format != "iceberg":
            raise ValueError(f"Unsupported table format: {table_format}")

        from dlt.common.destination.exceptions import DestinationUndefinedEntity

        catalog = self._get_catalog()
        identifier = f"{self.config.namespace}.{table_name}"

        try:
            return catalog.load_table(identifier)
        except NoSuchTableError as e:
            raise DestinationUndefinedEntity(table_name) from e

    def is_open_table(self, table_format: TTableFormat, table_name: str) -> bool:
        """Check if a table uses the specified open table format."""
        # All tables in this destination are Iceberg tables
        return table_format == "iceberg"

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

    def _get_table_location(self, table_name: str) -> Optional[str]:
        """
        Get the table location based on table_location_layout configuration.

        Args:
            table_name: Name of the table

        Returns:
            Table location string or None to use catalog default
        """
        if not self.config.table_location_layout:
            return None

        # Get warehouse base from config
        warehouse = self.config.warehouse or ""

        # Build location from layout pattern
        location = self.config.table_location_layout.format(
            namespace=self.config.namespace,
            dataset_name=self.config.namespace,  # In dlt, dataset_name maps to namespace
            table_name=table_name,
        )

        # If layout is relative (doesn't start with protocol), prepend warehouse
        if not location.startswith(("s3://", "gs://", "az://", "file://", "hdfs://")):
            # Ensure warehouse ends with / for proper joining
            if warehouse and not warehouse.endswith("/"):
                warehouse += "/"
            location = f"{warehouse}{location}"

        return location

    def _register_tables_from_storage(self, catalog, namespace: str) -> None:
        """
        Register tables found in storage but missing from catalog.

        Scans the warehouse directory for Iceberg metadata files and registers
        any tables not already in the catalog. This provides backward compatibility
        when tables exist in storage but haven't been registered.
        """
        if not self.config.register_new_tables:
            return

        if not self.config.warehouse:
            logger.warning("Cannot register tables: no warehouse configured")
            return

        import os
        from urllib.parse import urlparse

        warehouse = self.config.warehouse

        # Only support local filesystem for now
        parsed = urlparse(warehouse)
        if parsed.scheme and parsed.scheme != "file":
            logger.info(
                f"register_new_tables only supported for local filesystem, "
                f"skipping for {parsed.scheme}"
            )
            return

        # Get local path
        local_path = parsed.path if parsed.scheme == "file" else warehouse
        namespace_path = os.path.join(local_path, namespace)

        if not os.path.exists(namespace_path):
            logger.info(f"Namespace path {namespace_path} doesn't exist, nothing to register")
            return

        # Get existing tables in catalog
        try:
            existing_tables = {t[1] for t in catalog.list_tables(namespace)}
        except NoSuchNamespaceError:
            existing_tables = set()

        # Scan for table directories with metadata
        registered_count = 0
        for item in os.listdir(namespace_path):
            table_path = os.path.join(namespace_path, item)
            if not os.path.isdir(table_path):
                continue

            # Check if it's an Iceberg table (has metadata directory)
            metadata_path = os.path.join(table_path, "metadata")
            if not os.path.exists(metadata_path):
                continue

            table_name = item
            if table_name in existing_tables:
                continue

            # Find latest metadata file
            metadata_files = [
                f for f in os.listdir(metadata_path)
                if f.endswith(".metadata.json")
            ]
            if not metadata_files:
                continue

            # Sort to get latest (by version number in filename)
            metadata_files.sort(reverse=True)
            latest_metadata = os.path.join(metadata_path, metadata_files[0])

            try:
                identifier = f"{namespace}.{table_name}"
                catalog.register_table(
                    identifier=identifier,
                    metadata_location=f"file://{latest_metadata}",
                )
                logger.info(f"Registered table {identifier} from storage")
                registered_count += 1
            except Exception as e:
                logger.warning(f"Failed to register table {table_name}: {e}")

        if registered_count > 0:
            logger.info(f"Registered {registered_count} tables from storage")

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

        # Register tables from storage if enabled
        self._register_tables_from_storage(catalog, namespace)

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
        pending_files: Dict[str, List[Tuple[TTableSchema, str, pa.Table]]] = {}
        with _PENDING_FILES_LOCK:
            if load_id in _PENDING_FILES and _PENDING_FILES[load_id]:
                # Copy data and clear immediately (under lock)
                pending_files = dict(_PENDING_FILES[load_id])
                del _PENDING_FILES[load_id]

        catalog = self._get_catalog()
        namespace = self.config.namespace

        if pending_files:
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
        else:
            logger.info(f"No files to commit for load {load_id}")

        # dlt expects complete_load to persist a record in _dlt_loads.
        # Without this, _dlt_loads is never materialized for this destination.
        self._store_completed_load(catalog, load_id)
        logger.info(f"Load {load_id} completed successfully")

    def _store_completed_load(self, catalog, load_id: str) -> None:
        """Persist a load completion row in the internal _dlt_loads table."""
        loads_table_name = self.schema.loads_table_name
        identifier = f"{self.config.namespace}.{loads_table_name}"

        load_row = pa.table(
            {
                "load_id": [load_id],
                "schema_name": [self.schema.name],
                "status": [0],
                "inserted_at": [pendulum.now()],
                "schema_version_hash": [self.schema.version_hash],
            }
        )

        for attempt in range(self.config.max_retries):
            try:
                loads_table = self._get_or_create_loads_table(
                    catalog, identifier, loads_table_name, load_row
                )

                # Idempotency: if this load_id was already recorded, do nothing.
                if self._load_record_exists(loads_table, load_id):
                    logger.info(f"Load {load_id} already recorded in {identifier}")
                    return

                expected_schema = schema_to_pyarrow(loads_table.schema())
                casted_row = cast_table_safe(load_row, expected_schema, strict=False)
                loads_table.append(casted_row)
                return

            except Exception as e:
                retryable = is_retryable_error(e)

                # Ambiguous write handling: append may have succeeded but client saw an error.
                # Read-after-error: if record exists now, treat as success.
                if self._load_record_exists_in_catalog(catalog, identifier, load_id):
                    logger.warning(
                        f"Encountered error while recording load {load_id} but record exists; "
                        f"treating as success: {e}"
                    )
                    return

                log_error_with_context(
                    e,
                    operation=f"record load in {identifier}",
                    table_name=loads_table_name,
                    attempt=attempt + 1,
                    max_attempts=self.config.max_retries,
                    include_traceback=not retryable,
                )

                if not retryable:
                    error_msg = get_user_friendly_error_message(
                        e, f"record load metadata in table {identifier}"
                    )
                    raise RuntimeError(error_msg) from e

                if attempt >= self.config.max_retries - 1:
                    error_msg = get_user_friendly_error_message(
                        e,
                        f"record load metadata in table {identifier} after "
                        f"{self.config.max_retries} attempts",
                    )
                    raise RuntimeError(error_msg) from e

                sleep_time = self.config.retry_backoff_base ** attempt
                logger.info(
                    f"Retrying load metadata write after {sleep_time}s "
                    f"(attempt {attempt + 2}/{self.config.max_retries})"
                )
                time.sleep(sleep_time)

    def _get_or_create_loads_table(
        self,
        catalog,
        identifier: str,
        loads_table_name: str,
        load_row: pa.Table,
    ):
        """Load the internal _dlt_loads table, creating it if needed."""
        try:
            return catalog.load_table(identifier)
        except NoSuchTableError:
            from pyiceberg.partitioning import PartitionSpec

            loads_schema = convert_dlt_to_iceberg_schema(
                self.schema.get_table(loads_table_name),
                load_row,
            )
            create_kwargs = {
                "identifier": identifier,
                "schema": loads_schema,
                "partition_spec": PartitionSpec(),
            }
            table_location = self._get_table_location(loads_table_name)
            if table_location:
                create_kwargs["location"] = table_location

            catalog.create_table(
                **create_kwargs
            )
            return catalog.load_table(identifier)

    def _load_record_exists(self, loads_table, load_id: str) -> bool:
        """Check whether a load_id already exists in _dlt_loads."""
        existing_rows = loads_table.scan(row_filter=EqualTo("load_id", load_id)).to_arrow()
        return len(existing_rows) > 0

    def _load_record_exists_in_catalog(self, catalog, identifier: str, load_id: str) -> bool:
        """Best-effort catalog-level existence check for ambiguous write outcomes."""
        try:
            loads_table = catalog.load_table(identifier)
            return self._load_record_exists(loads_table, load_id)
        except Exception:
            return False

    def _get_merge_strategy(self, table_schema: TTableSchema) -> str:
        """Extract merge strategy from table schema.

        write_disposition can be:
        - "merge" (string) -> use upsert (backward compatible)
        - {"disposition": "merge", "strategy": "delete-insert"} -> explicit strategy

        Returns:
            Merge strategy: "upsert" or "delete-insert"
        """
        write_disposition = table_schema.get("write_disposition", "append")

        if isinstance(write_disposition, dict):
            return write_disposition.get("strategy", "delete-insert")

        # String "merge" - use upsert as our default (backward compatible)
        return "upsert"

    def _execute_delete_insert(
        self,
        iceberg_table,
        combined_table: pa.Table,
        primary_keys: List[str],
        identifier: str,
        hard_delete_filter=None,
    ) -> Tuple[int, int, int]:
        """Execute delete-insert merge strategy with optional hard deletes.

        Deletes rows matching primary keys in incoming data, then appends new data.
        Uses PyIceberg transaction for atomic hard-delete + delete + append.

        Args:
            iceberg_table: PyIceberg table object
            combined_table: Arrow table with data to merge
            primary_keys: List of primary key column names
            identifier: Table identifier for logging
            hard_delete_filter: Optional filter for hard deletes (rows to permanently remove)

        Returns:
            Tuple of (rows_deleted_estimate, rows_inserted, hard_deleted)
        """
        from pyiceberg.expressions import In, And, EqualTo, Or

        # Build delete filter from primary key values in incoming data
        if len(primary_keys) == 1:
            pk_col = primary_keys[0]
            pk_values = combined_table.column(pk_col).to_pylist()
            # Deduplicate values
            unique_pk_values = list(set(pk_values))
            delete_filter = In(pk_col, unique_pk_values)
            deleted_estimate = len(unique_pk_values)
        else:
            # Composite primary key - build OR of AND conditions
            pk_tuples = set()
            for i in range(len(combined_table)):
                pk_tuple = tuple(
                    combined_table.column(pk).to_pylist()[i] for pk in primary_keys
                )
                pk_tuples.add(pk_tuple)

            conditions = []
            for pk_tuple in pk_tuples:
                and_conditions = [
                    EqualTo(pk, val) for pk, val in zip(primary_keys, pk_tuple)
                ]
                if len(and_conditions) == 1:
                    conditions.append(and_conditions[0])
                else:
                    conditions.append(And(*and_conditions))

            if len(conditions) == 1:
                delete_filter = conditions[0]
            else:
                delete_filter = Or(*conditions)
            deleted_estimate = len(pk_tuples)

        logger.info(
            f"Delete-insert for {identifier}: deleting up to {deleted_estimate} "
            f"matching rows, inserting {len(combined_table)} rows"
        )

        # Execute atomic hard-delete + delete + append using single transaction
        with iceberg_table.transaction() as txn:
            # Hard deletes first (permanent removal)
            if hard_delete_filter is not None:
                txn.delete(hard_delete_filter)
            # Then delete-insert for merge
            txn.delete(delete_filter)
            txn.append(combined_table)

        return (deleted_estimate, len(combined_table), 1 if hard_delete_filter else 0)

    def _prepare_hard_deletes(
        self,
        combined_table: pa.Table,
        primary_keys: List[str],
    ) -> Tuple[pa.Table, Optional[Any], int]:
        """
        Prepare hard deletes from incoming data (does not execute).

        Rows with the hard_delete_column set (non-null) will be deleted.
        Returns the filter expression to use in a transaction.

        Args:
            combined_table: Arrow table with data including possible delete markers
            primary_keys: List of primary key column names

        Returns:
            Tuple of (remaining_rows, delete_filter_or_none, num_to_delete)
        """
        hard_delete_col = self.config.hard_delete_column

        # Check if hard delete column exists in data
        if not hard_delete_col or hard_delete_col not in combined_table.column_names:
            return combined_table, None, 0

        from pyiceberg.expressions import In, And, EqualTo, Or
        import pyarrow.compute as pc

        # Get the delete marker column
        delete_col = combined_table.column(hard_delete_col)

        # Find rows marked for deletion (non-null values)
        delete_mask = pc.is_valid(delete_col)
        rows_to_delete = combined_table.filter(delete_mask)
        rows_to_keep = combined_table.filter(pc.invert(delete_mask))

        if len(rows_to_delete) == 0:
            return rows_to_keep, None, 0

        # Build delete filter from primary keys of rows to delete
        if len(primary_keys) == 1:
            pk_col = primary_keys[0]
            pk_values = rows_to_delete.column(pk_col).to_pylist()
            unique_pk_values = list(set(pk_values))
            delete_filter = In(pk_col, unique_pk_values)
        else:
            # Composite primary key
            pk_tuples = set()
            for i in range(len(rows_to_delete)):
                pk_tuple = tuple(
                    rows_to_delete.column(pk).to_pylist()[i] for pk in primary_keys
                )
                pk_tuples.add(pk_tuple)

            conditions = []
            for pk_tuple in pk_tuples:
                and_conditions = [
                    EqualTo(pk, val) for pk, val in zip(primary_keys, pk_tuple)
                ]
                if len(and_conditions) == 1:
                    conditions.append(and_conditions[0])
                else:
                    conditions.append(And(*and_conditions))

            if len(conditions) == 1:
                delete_filter = conditions[0]
            else:
                delete_filter = Or(*conditions)

        return rows_to_keep, delete_filter, len(rows_to_delete)

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

                    # Get custom location if configured
                    table_location = self._get_table_location(table_name)
                    create_kwargs = {
                        "identifier": identifier,
                        "schema": iceberg_schema,
                        "partition_spec": partition_spec,
                    }
                    if table_location:
                        create_kwargs["location"] = table_location
                        logger.info(f"Using custom location: {table_location}")

                    iceberg_table = catalog.create_table(**create_kwargs)
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
                # Handle both string and dict write_disposition
                disposition_type = write_disposition
                if isinstance(write_disposition, dict):
                    disposition_type = write_disposition.get("disposition", "append")

                if disposition_type == "replace":
                    logger.info(f"Overwriting table {identifier}")
                    iceberg_table.overwrite(combined_table)
                elif disposition_type == "append":
                    logger.info(f"Appending to table {identifier}")
                    iceberg_table.append(combined_table)
                elif disposition_type == "merge":
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
                        # Prepare hard deletes (rows marked for deletion)
                        remaining_rows, hard_delete_filter, num_hard_deletes = self._prepare_hard_deletes(
                            combined_table, primary_keys
                        )
                        if num_hard_deletes > 0:
                            logger.info(f"Prepared {num_hard_deletes} rows for hard delete")

                        # If all rows were hard deletes, just execute the delete
                        if len(remaining_rows) == 0:
                            if hard_delete_filter is not None:
                                iceberg_table.delete(hard_delete_filter)
                                logger.info(f"Executed {num_hard_deletes} hard deletes (no merge needed)")
                            return

                        # Get merge strategy
                        merge_strategy = self._get_merge_strategy(table_schema)
                        logger.info(
                            f"Merging into table {identifier} on keys {primary_keys} "
                            f"using strategy: {merge_strategy}"
                        )

                        if merge_strategy == "delete-insert":
                            # Atomic hard-delete + delete + insert in single transaction
                            deleted, inserted, _ = self._execute_delete_insert(
                                iceberg_table, remaining_rows, primary_keys, identifier,
                                hard_delete_filter=hard_delete_filter
                            )
                            logger.info(
                                f"Delete-insert completed: ~{deleted} deleted, "
                                f"{inserted} inserted"
                            )
                        else:
                            # Default: upsert strategy
                            # Execute hard deletes first (separate transaction since upsert is atomic)
                            if hard_delete_filter is not None:
                                iceberg_table.delete(hard_delete_filter)
                                logger.info(f"Executed {num_hard_deletes} hard deletes before upsert")

                            batch_size = self.config.merge_batch_size
                            total_updated = 0
                            total_inserted = 0

                            for batch_start in range(0, len(remaining_rows), batch_size):
                                batch_end = min(batch_start + batch_size, len(remaining_rows))
                                batch = remaining_rows.slice(batch_start, batch_end - batch_start)

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
                                f"{total_inserted} inserted across {(len(remaining_rows) + batch_size - 1) // batch_size} batches"
                            )
                else:
                    raise ValueError(f"Unknown write disposition: {disposition_type}")

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
        caps.supported_merge_strategies = ["delete-insert", "upsert"]

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
