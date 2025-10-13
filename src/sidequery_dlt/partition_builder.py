"""
Partition spec builder for Iceberg tables.
"""

import logging
from typing import Optional
from dlt.common.schema import TTableSchema
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import (
    IdentityTransform,
    YearTransform,
    MonthTransform,
    DayTransform,
    HourTransform,
)
from pyiceberg.types import (
    TimestampType,
    DateType,
    StringType,
    IntegerType,
    LongType,
)

logger = logging.getLogger(__name__)


def build_partition_spec(
    dlt_table: TTableSchema, iceberg_schema: Schema
) -> Optional[PartitionSpec]:
    """
    Build Iceberg partition spec from dlt table hints.

    Looks for columns marked with partition=True in dlt table schema
    and creates appropriate partition transforms based on data types.

    Args:
        dlt_table: dlt table schema with column hints
        iceberg_schema: Iceberg schema with field information

    Returns:
        PartitionSpec or None if no partitioning specified
    """
    partition_columns = []

    # Extract partition columns from dlt hints
    dlt_columns = dlt_table.get("columns", {})
    for col_name, col_info in dlt_columns.items():
        if col_info.get("partition"):
            partition_columns.append(col_name)

    if not partition_columns:
        logger.info("No partition columns specified")
        return None

    logger.info(f"Building partition spec for columns: {partition_columns}")

    # Build partition fields
    partition_fields = []

    for col_name in partition_columns:
        # Find field in Iceberg schema
        iceberg_field = None
        for field in iceberg_schema.fields:
            if field.name == col_name:
                iceberg_field = field
                break

        if not iceberg_field:
            logger.warning(
                f"Partition column {col_name} not found in schema, skipping"
            )
            continue

        # Choose transform based on data type
        transform = choose_partition_transform(
            iceberg_field.field_type, col_name, dlt_columns.get(col_name, {})
        )

        # Create partition field
        partition_field = PartitionField(
            source_id=iceberg_field.field_id,
            field_id=1000 + len(partition_fields),  # Start partition IDs at 1000
            transform=transform,
            name=f"{col_name}_{get_transform_name(transform)}",
        )
        partition_fields.append(partition_field)

    if not partition_fields:
        logger.warning("No valid partition fields created")
        return None

    spec = PartitionSpec(*partition_fields)
    logger.info(f"Created partition spec with {len(partition_fields)} fields")
    return spec


def choose_partition_transform(field_type, col_name: str, col_hints: dict):
    """
    Choose appropriate Iceberg transform based on field type and hints.

    Args:
        field_type: Iceberg field type
        col_name: Column name
        col_hints: dlt column hints (may contain partition transform info)

    Returns:
        Iceberg transform
    """
    # Check if user specified a transform in hints
    partition_transform = col_hints.get("partition_transform")

    # Timestamp/Date types: use temporal transforms
    if isinstance(field_type, (TimestampType, DateType)):
        if partition_transform == "year":
            return YearTransform()
        elif partition_transform == "month":
            return MonthTransform()
        elif partition_transform == "day":
            return DayTransform()
        elif partition_transform == "hour":
            return HourTransform()
        else:
            # Default to month for timestamps/dates
            return MonthTransform()

    # String, Integer, Long: use identity transform
    elif isinstance(field_type, (StringType, IntegerType, LongType)):
        return IdentityTransform()

    # Default: identity transform
    else:
        logger.warning(
            f"Using identity transform for {col_name} with type {field_type}"
        )
        return IdentityTransform()


def get_transform_name(transform) -> str:
    """Get human-readable name for transform."""
    if isinstance(transform, IdentityTransform):
        return "identity"
    elif isinstance(transform, YearTransform):
        return "year"
    elif isinstance(transform, MonthTransform):
        return "month"
    elif isinstance(transform, DayTransform):
        return "day"
    elif isinstance(transform, HourTransform):
        return "hour"
    else:
        return "transform"
