"""
Iceberg adapter for dlt resources.

Provides a way to add Iceberg-specific hints to dlt resources, following
the adapter pattern used by BigQuery, Databricks, and other destinations.

Usage:
    from dlt_iceberg import iceberg_adapter, iceberg_partition

    @dlt.resource(name="events")
    def my_events():
        yield {"event_date": "2024-01-01", "user_id": 123}

    # Partition by month on event_date and bucket user_id
    adapted = iceberg_adapter(
        my_events,
        partition=[
            iceberg_partition.month("event_date"),
            iceberg_partition.bucket("user_id", 10),
        ]
    )
"""

import logging
from typing import Any, List, Optional, Union, cast
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PartitionTransform:
    """Represents an Iceberg partition transform for a column.

    Attributes:
        column: Column name to partition on
        transform: Transform type (identity, year, month, day, hour, bucket, truncate)
        param: Optional parameter for bucket[N] or truncate[N]
    """

    column: str
    transform: str
    param: Optional[int] = None

    def to_hint_value(self) -> str:
        """Convert to partition_transform hint value."""
        if self.param is not None:
            return f"{self.transform}[{self.param}]"
        return self.transform


class iceberg_partition:
    """Factory for Iceberg partition transforms.

    Provides static methods to create partition specifications:

    - identity(column): No transformation, use value as-is
    - year(column): Extract year from timestamp/date
    - month(column): Extract year-month from timestamp/date
    - day(column): Extract date from timestamp/date
    - hour(column): Extract date-hour from timestamp
    - bucket(column, n): Hash partition into n buckets
    - truncate(column, width): Truncate string/number to width

    Examples:
        iceberg_partition.month("created_at")
        iceberg_partition.bucket("user_id", 10)
        iceberg_partition.truncate("email", 4)
    """

    @staticmethod
    def identity(column: str) -> PartitionTransform:
        """Identity transform - use column value as-is for partitioning."""
        return PartitionTransform(column=column, transform="identity")

    @staticmethod
    def year(column: str) -> PartitionTransform:
        """Year transform - partition by year extracted from timestamp/date."""
        return PartitionTransform(column=column, transform="year")

    @staticmethod
    def month(column: str) -> PartitionTransform:
        """Month transform - partition by year-month extracted from timestamp/date."""
        return PartitionTransform(column=column, transform="month")

    @staticmethod
    def day(column: str) -> PartitionTransform:
        """Day transform - partition by date extracted from timestamp/date."""
        return PartitionTransform(column=column, transform="day")

    @staticmethod
    def hour(column: str) -> PartitionTransform:
        """Hour transform - partition by date-hour extracted from timestamp."""
        return PartitionTransform(column=column, transform="hour")

    @staticmethod
    def bucket(column: str, num_buckets: int) -> PartitionTransform:
        """Bucket transform - hash partition into n buckets.

        Args:
            column: Column name to partition on
            num_buckets: Number of buckets (must be positive)

        Raises:
            ValueError: If num_buckets is not positive
        """
        if num_buckets <= 0:
            raise ValueError(f"num_buckets must be positive, got {num_buckets}")
        return PartitionTransform(column=column, transform="bucket", param=num_buckets)

    @staticmethod
    def truncate(column: str, width: int) -> PartitionTransform:
        """Truncate transform - truncate string/number to width.

        Args:
            column: Column name to partition on
            width: Truncation width (must be positive)

        Raises:
            ValueError: If width is not positive
        """
        if width <= 0:
            raise ValueError(f"width must be positive, got {width}")
        return PartitionTransform(column=column, transform="truncate", param=width)


def _get_resource_for_adapter(data: Any):
    """Get or create a DltResource from data.

    Follows the pattern from dlt.destinations.utils.get_resource_for_adapter.
    """
    import dlt
    from dlt.extract.resource import DltResource
    from dlt.extract.source import DltSource

    if isinstance(data, DltResource):
        return data

    if isinstance(data, DltSource):
        if len(data.selected_resources.keys()) == 1:
            return list(data.selected_resources.values())[0]
        else:
            raise ValueError(
                "You are trying to use iceberg_adapter on a DltSource with "
                "multiple resources. You can only use adapters on: pure data, "
                "a DltResource, or a DltSource with a single DltResource."
            )

    resource_name = None
    if not hasattr(data, "__name__"):
        logger.info("Setting default resource name to 'content' for adapted resource.")
        resource_name = "content"

    return cast("DltResource", dlt.resource(data, name=resource_name))


def iceberg_adapter(
    data: Any,
    partition: Optional[Union[PartitionTransform, List[PartitionTransform]]] = None,
):
    """
    Apply Iceberg-specific hints to a dlt resource.

    This adapter prepares data for loading into Iceberg tables by setting
    partition specifications using Iceberg's native transforms.

    Args:
        data: A dlt resource, source (with single resource), or raw data
        partition: Partition specification(s). Can be:
            - A single PartitionTransform
            - A list of PartitionTransform objects
            Use iceberg_partition helpers to create transforms.

    Returns:
        DltResource with Iceberg-specific hints applied

    Examples:
        # Single partition column with month transform
        iceberg_adapter(my_resource, partition=iceberg_partition.month("created_at"))

        # Multiple partition columns
        iceberg_adapter(
            my_resource,
            partition=[
                iceberg_partition.day("event_date"),
                iceberg_partition.bucket("user_id", 10),
            ]
        )

        # Works with raw data too
        data = [{"id": 1, "ts": "2024-01-01"}]
        iceberg_adapter(data, partition=iceberg_partition.month("ts"))
    """
    resource = _get_resource_for_adapter(data)

    if partition is None:
        return resource

    # Normalize to list
    partitions = [partition] if isinstance(partition, PartitionTransform) else partition

    if not partitions:
        return resource

    # Build column hints for partitioning
    column_hints = {}

    for p in partitions:
        if p.column not in column_hints:
            column_hints[p.column] = {"name": p.column}

        # Set partition flag using x-partition (custom hint prefix)
        column_hints[p.column]["x-partition"] = True

        # Set transform (identity is handled as default in partition_builder)
        if p.transform != "identity":
            column_hints[p.column]["x-partition-transform"] = p.to_hint_value()

    # Apply hints to resource
    resource.apply_hints(columns=column_hints)

    logger.info(f"Applied Iceberg partition hints: {[p.column for p in partitions]}")

    return resource
