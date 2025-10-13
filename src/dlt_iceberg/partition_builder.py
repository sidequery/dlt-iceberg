"""
Partition spec builder for Iceberg tables.

Supports advanced Iceberg partitioning transforms:
- Temporal: year, month, day, hour
- Identity: no transformation
- Bucket: hash-based partitioning with bucket[N] syntax
- Truncate: truncate to width with truncate[N] syntax

Examples:
    # Bucket partitioning on user_id into 10 buckets
    partition_transform="bucket[10]"

    # Truncate string to 4 characters
    partition_transform="truncate[4]"

    # Temporal partitioning
    partition_transform="month"
"""

import logging
import re
from typing import Optional, Tuple
from dlt.common.schema import TTableSchema
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import (
    IdentityTransform,
    YearTransform,
    MonthTransform,
    DayTransform,
    HourTransform,
    BucketTransform,
    TruncateTransform,
)
from pyiceberg.types import (
    TimestampType,
    DateType,
    StringType,
    IntegerType,
    LongType,
    BinaryType,
    DecimalType,
    DoubleType,
    FloatType,
)

logger = logging.getLogger(__name__)


def parse_transform_hint(hint: str) -> Tuple[str, Optional[int]]:
    """
    Parse partition transform hint into transform type and parameter.

    Supports:
    - Simple transforms: "year", "month", "day", "hour", "identity"
    - Parameterized transforms: "bucket[10]", "truncate[5]"

    Args:
        hint: Transform hint string

    Returns:
        Tuple of (transform_type, parameter)
        Example: ("bucket", 10) or ("month", None)

    Raises:
        ValueError: If hint format is invalid
    """
    # Check for parameterized transform: bucket[N] or truncate[N]
    match = re.match(r"^(\w+)\[(\d+)\]$", hint)
    if match:
        transform_type = match.group(1)
        param = int(match.group(2))
        return (transform_type, param)

    # Simple transform
    return (hint, None)


def validate_transform_for_type(
    transform_type: str, param: Optional[int], field_type, col_name: str
) -> None:
    """
    Validate that a transform is appropriate for the field type.

    Args:
        transform_type: Transform type (e.g., "bucket", "truncate", "month")
        param: Transform parameter (e.g., bucket count, truncate width)
        field_type: Iceberg field type
        col_name: Column name for error messages

    Raises:
        ValueError: If transform is invalid for the field type
    """
    # Temporal transforms only for timestamp/date
    temporal_transforms = {"year", "month", "day", "hour"}
    if transform_type in temporal_transforms:
        if not isinstance(field_type, (TimestampType, DateType)):
            raise ValueError(
                f"Temporal transform '{transform_type}' cannot be applied to "
                f"column '{col_name}' with type {field_type}. "
                f"Use timestamp or date types for temporal transforms."
            )

    # Bucket transform validation
    if transform_type == "bucket":
        if param is None or param <= 0:
            raise ValueError(
                f"Bucket transform requires a positive integer parameter, "
                f"got: {param} for column '{col_name}'"
            )
        # Bucket can be applied to most types except binary
        if isinstance(field_type, BinaryType):
            raise ValueError(
                f"Bucket transform cannot be applied to binary column '{col_name}'"
            )

    # Truncate transform validation
    if transform_type == "truncate":
        if param is None or param <= 0:
            raise ValueError(
                f"Truncate transform requires a positive integer parameter, "
                f"got: {param} for column '{col_name}'"
            )
        # Truncate works on strings, integers, longs, decimals, binary
        valid_types = (StringType, IntegerType, LongType, DecimalType, BinaryType)
        if not isinstance(field_type, valid_types):
            raise ValueError(
                f"Truncate transform cannot be applied to column '{col_name}' "
                f"with type {field_type}. "
                f"Use string, integer, long, decimal, or binary types."
            )


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
    # Support both 'partition' and 'x-partition' (custom hint)
    dlt_columns = dlt_table.get("columns", {})
    for col_name, col_info in dlt_columns.items():
        if col_info.get("partition") or col_info.get("x-partition"):
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

    Supports:
    - Temporal transforms: year, month, day, hour (for timestamps/dates)
    - Bucket transforms: bucket[N] (hash-based partitioning)
    - Truncate transforms: truncate[N] (truncate to width)
    - Identity transform: no transformation

    Args:
        field_type: Iceberg field type
        col_name: Column name
        col_hints: dlt column hints (may contain partition_transform)

    Returns:
        Iceberg transform

    Raises:
        ValueError: If transform is invalid for the field type

    Examples:
        # Bucket partitioning
        col_hints = {"partition_transform": "bucket[10]"}

        # Truncate partitioning
        col_hints = {"partition_transform": "truncate[4]"}

        # Temporal partitioning
        col_hints = {"partition_transform": "month"}
    """
    # Check if user specified a transform in hints
    # Support both 'partition_transform' and 'x-partition-transform' (custom hint)
    partition_transform = col_hints.get("partition_transform") or col_hints.get("x-partition-transform")

    if partition_transform:
        # Parse the transform hint
        transform_type, param = parse_transform_hint(partition_transform)

        # Validate transform is appropriate for field type
        validate_transform_for_type(transform_type, param, field_type, col_name)

        # Create the appropriate transform
        if transform_type == "bucket":
            return BucketTransform(num_buckets=param)
        elif transform_type == "truncate":
            return TruncateTransform(width=param)
        elif transform_type == "year":
            return YearTransform()
        elif transform_type == "month":
            return MonthTransform()
        elif transform_type == "day":
            return DayTransform()
        elif transform_type == "hour":
            return HourTransform()
        elif transform_type == "identity":
            return IdentityTransform()
        else:
            raise ValueError(
                f"Unknown transform type '{transform_type}' for column '{col_name}'"
            )

    # No hint specified - use defaults based on type
    if isinstance(field_type, (TimestampType, DateType)):
        # Default to month for timestamps/dates
        return MonthTransform()
    elif isinstance(field_type, (StringType, IntegerType, LongType)):
        # Default to identity for discrete types
        return IdentityTransform()
    else:
        # Fallback to identity
        logger.warning(
            f"Using identity transform for {col_name} with type {field_type}"
        )
        return IdentityTransform()


def get_transform_name(transform) -> str:
    """
    Get human-readable name for transform.

    Args:
        transform: Iceberg transform instance

    Returns:
        String representation of the transform
    """
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
    elif isinstance(transform, BucketTransform):
        return f"bucket_{transform.num_buckets}"
    elif isinstance(transform, TruncateTransform):
        return f"truncate_{transform.width}"
    else:
        return "transform"
