"""
Tests for partition spec builder.
"""

import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
    TimestampType,
)
from pyiceberg.transforms import (
    MonthTransform,
    DayTransform,
    IdentityTransform,
)

from sidequery_dlt.partition_builder import (
    build_partition_spec,
    choose_partition_transform,
)


def test_build_partition_spec_no_partitions():
    """Test when no partition columns are specified."""

    dlt_table = {
        "name": "test_table",
        "columns": {
            "id": {"data_type": "bigint", "nullable": False},
            "name": {"data_type": "text", "nullable": True},
        },
    }

    iceberg_schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=False),
    )

    spec = build_partition_spec(dlt_table, iceberg_schema)
    assert spec is None


def test_build_partition_spec_with_timestamp():
    """Test partition spec with timestamp column."""

    dlt_table = {
        "name": "test_table",
        "columns": {
            "id": {"data_type": "bigint", "nullable": False},
            "created_at": {
                "data_type": "timestamp",
                "nullable": False,
                "partition": True,
            },
        },
    }

    iceberg_schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "created_at", TimestampType(), required=True),
    )

    spec = build_partition_spec(dlt_table, iceberg_schema)

    assert spec is not None
    assert len(spec.fields) == 1

    partition_field = spec.fields[0]
    assert partition_field.source_id == 2  # created_at field
    assert isinstance(partition_field.transform, MonthTransform)


def test_build_partition_spec_with_string():
    """Test partition spec with string column."""

    dlt_table = {
        "name": "test_table",
        "columns": {
            "id": {"data_type": "bigint", "nullable": False},
            "region": {
                "data_type": "text",
                "nullable": False,
                "partition": True,
            },
        },
    }

    iceberg_schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "region", StringType(), required=True),
    )

    spec = build_partition_spec(dlt_table, iceberg_schema)

    assert spec is not None
    assert len(spec.fields) == 1

    partition_field = spec.fields[0]
    assert partition_field.source_id == 2  # region field
    assert isinstance(partition_field.transform, IdentityTransform)


def test_build_partition_spec_multiple_columns():
    """Test partition spec with multiple columns."""

    dlt_table = {
        "name": "test_table",
        "columns": {
            "id": {"data_type": "bigint", "nullable": False},
            "region": {
                "data_type": "text",
                "nullable": False,
                "partition": True,
            },
            "created_at": {
                "data_type": "timestamp",
                "nullable": False,
                "partition": True,
            },
        },
    }

    iceberg_schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "region", StringType(), required=True),
        NestedField(3, "created_at", TimestampType(), required=True),
    )

    spec = build_partition_spec(dlt_table, iceberg_schema)

    assert spec is not None
    assert len(spec.fields) == 2


def test_choose_partition_transform_timestamp():
    """Test choosing transform for timestamp fields."""

    # Default (no hint)
    transform = choose_partition_transform(TimestampType(), "ts", {})
    assert isinstance(transform, MonthTransform)

    # Explicit day transform
    transform = choose_partition_transform(
        TimestampType(), "ts", {"partition_transform": "day"}
    )
    assert isinstance(transform, DayTransform)


def test_choose_partition_transform_string():
    """Test choosing transform for string fields."""

    transform = choose_partition_transform(StringType(), "region", {})
    assert isinstance(transform, IdentityTransform)
