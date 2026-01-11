"""
Iceberg REST Catalog destination for dlt.

This custom destination writes data to Apache Iceberg tables using a REST catalog
(Polaris, Unity Catalog, AWS Glue, Nessie, etc.).
"""

import time
import logging
from typing import Optional, Dict, Any
from pathlib import Path

import dlt
import pyarrow.parquet as pq
from dlt.common.schema import TTableSchema
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    NoSuchTableError,
    CommitFailedException,
    NoSuchNamespaceError,
    ValidationError,
)

from .schema_converter import convert_dlt_to_iceberg_schema
from .partition_builder import build_partition_spec
from .schema_evolution import evolve_schema_if_needed, SchemaEvolutionError
from .schema_casting import cast_table_safe, CastingError
from .error_handling import (
    is_retryable_error,
    log_error_with_context,
    get_user_friendly_error_message,
)
from pyiceberg.io.pyarrow import schema_to_pyarrow

logger = logging.getLogger(__name__)


def _get_merge_strategy(table_schema: TTableSchema) -> str:
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


def _execute_delete_insert(iceberg_table, arrow_table, primary_keys: list, identifier: str):
    """Execute delete-insert merge strategy.

    Deletes rows matching primary keys in incoming data, then appends new data.
    Uses PyIceberg transaction for atomic delete + append.

    Args:
        iceberg_table: PyIceberg table object
        arrow_table: Arrow table with data to merge
        primary_keys: List of primary key column names
        identifier: Table identifier for logging

    Returns:
        Tuple of (rows_deleted_estimate, rows_inserted)
    """
    from pyiceberg.expressions import In, And, EqualTo, Or

    # Build delete filter from primary key values in incoming data
    if len(primary_keys) == 1:
        pk_col = primary_keys[0]
        pk_values = arrow_table.column(pk_col).to_pylist()
        unique_pk_values = list(set(pk_values))
        delete_filter = In(pk_col, unique_pk_values)
        deleted_estimate = len(unique_pk_values)
    else:
        # Composite primary key - build OR of AND conditions
        pk_tuples = set()
        for i in range(len(arrow_table)):
            pk_tuple = tuple(
                arrow_table.column(pk).to_pylist()[i] for pk in primary_keys
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
        f"matching rows, inserting {len(arrow_table)} rows"
    )

    # Execute atomic delete + append using transaction
    with iceberg_table.transaction() as txn:
        txn.delete(delete_filter)
        txn.append(arrow_table)

    return (deleted_estimate, len(arrow_table))


def _iceberg_rest_handler(
    items: str,  # File path when batch_size=0
    table: TTableSchema,
    # Catalog configuration
    catalog_uri: str = dlt.secrets.value,
    warehouse: Optional[str] = dlt.secrets.value,
    namespace: str = "default",
    # Authentication (OAuth2)
    credential: Optional[str] = dlt.secrets.value,
    oauth2_server_uri: Optional[str] = dlt.secrets.value,
    scope: Optional[str] = "PRINCIPAL_ROLE:ALL",
    # Or Bearer token
    token: Optional[str] = dlt.secrets.value,
    # AWS SigV4 (for Glue)
    sigv4_enabled: bool = False,
    signing_region: Optional[str] = None,
    signing_name: str = "execute-api",
    # S3 configuration (for MinIO or other S3-compatible storage)
    s3_endpoint: Optional[str] = None,
    s3_access_key_id: Optional[str] = None,
    s3_secret_access_key: Optional[str] = None,
    s3_region: Optional[str] = None,
    # Retry configuration
    max_retries: int = 5,
    retry_backoff_base: float = 2.0,
    # Schema casting configuration
    strict_casting: bool = False,
) -> None:
    """
    Custom dlt destination for Iceberg tables with REST catalog.

    Args:
        items: Path to parquet file generated by dlt
        table: dlt table schema with column information and hints
        catalog_uri: REST catalog endpoint URL
        warehouse: Warehouse location (S3/GCS/Azure path)
        namespace: Iceberg namespace (database)
        credential: OAuth2 credentials in format "client_id:client_secret"
        oauth2_server_uri: OAuth2 token endpoint
        token: Bearer token (alternative to OAuth2)
        sigv4_enabled: Enable AWS SigV4 signing
        signing_region: AWS region for SigV4
        signing_name: AWS service name for SigV4
        max_retries: Maximum retry attempts for commit failures
        retry_backoff_base: Base for exponential backoff
        strict_casting: If True, fail when schema cast would lose data (timezone info,
                       precision, etc.). If False, proceed with aggressive casting and
                       log warnings. Default is False for backward compatibility.
    """

    # Build catalog configuration
    # Auto-detect catalog type from URI
    if catalog_uri.startswith("sqlite://") or catalog_uri.startswith("postgresql://"):
        catalog_type = "sql"
    else:
        catalog_type = "rest"

    catalog_config = {
        "type": catalog_type,
        "uri": catalog_uri,
    }

    # Add warehouse if provided (some catalogs configure it globally)
    if warehouse:
        catalog_config["warehouse"] = warehouse

    # Add authentication
    if credential and oauth2_server_uri:
        catalog_config["credential"] = credential
        catalog_config["oauth2-server-uri"] = oauth2_server_uri
        if scope:
            catalog_config["scope"] = scope
    elif token:
        catalog_config["token"] = token

    # AWS SigV4
    if sigv4_enabled:
        catalog_config["rest.sigv4-enabled"] = "true"
        if signing_region:
            catalog_config["rest.signing-region"] = signing_region
        catalog_config["rest.signing-name"] = signing_name

    # S3 configuration
    if s3_endpoint:
        catalog_config["s3.endpoint"] = s3_endpoint
    if s3_access_key_id:
        catalog_config["s3.access-key-id"] = s3_access_key_id
    if s3_secret_access_key:
        catalog_config["s3.secret-access-key"] = s3_secret_access_key
    if s3_region:
        catalog_config["s3.region"] = s3_region

    # Create fresh catalog connection for each file
    catalog_type = catalog_config.get("type", "rest")
    catalog_uri = catalog_config.get("uri", "unknown")
    logger.info(
        f"Creating catalog connection (type={catalog_type}, uri={catalog_uri})"
    )
    catalog = load_catalog("dlt_catalog", **catalog_config)

    # Get table information
    table_name = table["name"]
    identifier = f"{namespace}.{table_name}"
    write_disposition = table.get("write_disposition", "append")

    logger.info(f"Processing table {identifier} with disposition {write_disposition}")

    # Ensure namespace exists
    try:
        namespaces = catalog.list_namespaces()
        if (namespace,) not in namespaces:
            logger.info(f"Creating namespace {namespace}")
            catalog.create_namespace(namespace)
        else:
            logger.info(f"Namespace {namespace} exists")
    except Exception as e:
        # Non-retryable namespace errors should fail fast
        if not is_retryable_error(e):
            log_error_with_context(
                e,
                operation=f"ensure namespace {namespace} exists",
                table_name=table_name,
                include_traceback=True,
            )
            raise RuntimeError(get_user_friendly_error_message(e, "ensure namespace exists")) from e
        # If retryable, let it propagate to the retry loop
        raise

    # Read parquet file generated by dlt
    file_path = Path(items)
    if not file_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {file_path}")

    arrow_table = pq.read_table(str(file_path))
    logger.info(f"Read {len(arrow_table)} rows from {file_path}")

    # Retry loop for transient failures
    for attempt in range(max_retries):
        try:
            # Check if table exists
            table_exists = False
            try:
                iceberg_table = catalog.load_table(identifier)
                table_exists = True
                logger.info(f"Loaded existing table {identifier}")
            except NoSuchTableError:
                logger.info(f"Table {identifier} does not exist, will create")
            except Exception as e:
                # Non-retryable errors during table load should fail fast
                if not is_retryable_error(e):
                    log_error_with_context(
                        e,
                        operation="load table",
                        table_name=identifier,
                        attempt=attempt + 1,
                        max_attempts=max_retries,
                        include_traceback=True,
                    )
                    raise RuntimeError(get_user_friendly_error_message(e, "load table")) from e
                # Retryable error - propagate to retry loop
                raise

            # Create table if it doesn't exist
            if not table_exists:
                # Convert dlt schema to Iceberg schema
                iceberg_schema = convert_dlt_to_iceberg_schema(table, arrow_table)

                # Build partition spec from table hints
                partition_spec = build_partition_spec(table, iceberg_schema)

                # If no partitioning, use empty spec (PyIceberg doesn't handle None well)
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
                incoming_schema = convert_dlt_to_iceberg_schema(table, arrow_table)

                try:
                    schema_evolved = evolve_schema_if_needed(
                        iceberg_table,
                        incoming_schema,
                        allow_column_drops=False
                    )
                    if schema_evolved:
                        logger.info(f"Schema evolved for table {identifier}")
                        # Refresh table to get updated schema
                        iceberg_table = catalog.load_table(identifier)
                except SchemaEvolutionError as e:
                    # Schema evolution errors are non-retryable (data model issues)
                    log_error_with_context(
                        e,
                        operation="evolve schema",
                        table_name=identifier,
                        include_traceback=True,
                    )
                    raise

            # Cast Arrow table to match Iceberg schema with defensive validation
            # This handles timezone differences and other schema mismatches
            # In strict mode (default), casting fails if data would be lost
            # In non-strict mode, casting proceeds with warnings
            expected_schema = schema_to_pyarrow(iceberg_table.schema())
            try:
                arrow_table = cast_table_safe(
                    arrow_table,
                    expected_schema,
                    strict=strict_casting
                )
            except CastingError as e:
                # Casting errors are non-retryable (schema mismatch)
                log_error_with_context(
                    e,
                    operation="cast schema",
                    table_name=identifier,
                    include_traceback=True,
                )
                logger.error(
                    f"Schema casting failed for {identifier}. "
                    f"Set strict_casting=False to allow aggressive casting."
                )
                raise

            # Write data based on disposition
            # Handle both string and dict write_disposition
            disposition_type = write_disposition
            if isinstance(write_disposition, dict):
                disposition_type = write_disposition.get("disposition", "append")

            if disposition_type == "replace":
                logger.info(f"Overwriting table {identifier}")
                iceberg_table.overwrite(arrow_table)
            elif disposition_type == "append":
                logger.info(f"Appending to table {identifier}")
                iceberg_table.append(arrow_table)
            elif disposition_type == "merge":
                # For merge, we need primary keys
                # Try multiple ways to get primary keys from dlt table schema
                primary_keys = table.get("primary_key") or table.get("x-merge-keys")

                # If not found, check columns for primary_key hints
                if not primary_keys:
                    columns = table.get("columns", {})
                    primary_keys = [
                        col_name
                        for col_name, col_def in columns.items()
                        if col_def.get("primary_key") or col_def.get("x-primary-key")
                    ]

                if not primary_keys:
                    logger.warning(
                        f"Merge disposition requires primary_key, falling back to append"
                    )
                    iceberg_table.append(arrow_table)
                else:
                    # Get merge strategy
                    merge_strategy = _get_merge_strategy(table)
                    logger.info(
                        f"Merging into table {identifier} on keys {primary_keys} "
                        f"using strategy: {merge_strategy}"
                    )

                    if merge_strategy == "delete-insert":
                        # Atomic delete + insert
                        deleted, inserted = _execute_delete_insert(
                            iceberg_table, arrow_table, primary_keys, identifier
                        )
                        logger.info(
                            f"Delete-insert completed: ~{deleted} deleted, "
                            f"{inserted} inserted"
                        )
                    else:
                        # Default: upsert strategy
                        upsert_result = iceberg_table.upsert(
                            df=arrow_table,
                            join_cols=primary_keys,
                            when_matched_update_all=True,
                            when_not_matched_insert_all=True,
                        )
                        logger.info(
                            f"Upsert completed: {upsert_result.rows_updated} updated, "
                            f"{upsert_result.rows_inserted} inserted"
                        )
            else:
                raise ValueError(f"Unknown write disposition: {disposition_type}")

            logger.info(f"Successfully wrote {len(arrow_table)} rows to {identifier}")
            return  # Success

        except Exception as e:
            # Classify the error
            retryable = is_retryable_error(e)

            # Log error with context
            log_error_with_context(
                e,
                operation=f"write data (disposition={write_disposition})",
                table_name=identifier,
                attempt=attempt + 1,
                max_attempts=max_retries,
                include_traceback=not retryable,  # Full trace for non-retryable errors
            )

            # Non-retryable errors should fail immediately
            if not retryable:
                error_msg = get_user_friendly_error_message(
                    e, f"write data to table {identifier}"
                )
                raise RuntimeError(error_msg) from e

            # Retryable error - check if we should retry
            if attempt >= max_retries - 1:
                # Max retries exhausted
                error_msg = get_user_friendly_error_message(
                    e, f"write data to table {identifier} after {max_retries} attempts"
                )
                raise RuntimeError(error_msg) from e

            # Retry with exponential backoff
            sleep_time = retry_backoff_base ** attempt
            logger.info(
                f"Retrying after {sleep_time}s (attempt {attempt + 2}/{max_retries})"
            )
            time.sleep(sleep_time)

            # Refresh table state for next attempt
            if table_exists:
                try:
                    iceberg_table.refresh()
                    logger.debug(f"Refreshed table state for {identifier}")
                except Exception as refresh_error:
                    logger.warning(
                        f"Failed to refresh table (will retry anyway): {refresh_error}"
                    )


# Create the base destination factory
_iceberg_rest_base = dlt.destination(
    _iceberg_rest_handler,
    batch_size=0,
    loader_file_format="parquet",
    name="iceberg_rest",
    naming_convention="snake_case",
    skip_dlt_columns_and_tables=True,
    max_parallel_load_jobs=5,
    loader_parallelism_strategy="table-sequential",
)


# Wrap the factory to add merge support to capabilities
def iceberg_rest(**kwargs):
    """
    Iceberg REST destination factory with merge support.

    Returns a destination instance configured for Iceberg with merge capabilities.
    """
    # Get the destination instance
    dest = _iceberg_rest_base(**kwargs)

    # Override the _raw_capabilities method to include merge support
    original_raw_capabilities = dest._raw_capabilities

    def _raw_capabilities_with_merge():
        """Add merge support to the destination capabilities."""
        caps = original_raw_capabilities()
        caps.supported_merge_strategies = ["delete-insert", "upsert"]
        return caps

    # Bind the new method to the instance
    dest._raw_capabilities = _raw_capabilities_with_merge

    return dest
