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
    IntegerType,
    DecimalType,
    DoubleType,
)
from pyiceberg.transforms import (
    MonthTransform,
    DayTransform,
    IdentityTransform,
    BucketTransform,
    TruncateTransform,
)

from dlt_iceberg.partition_builder import (
    build_partition_spec,
    choose_partition_transform,
    parse_transform_hint,
    validate_transform_for_type,
    get_transform_name,
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


# Tests for parse_transform_hint


def test_parse_transform_hint_simple():
    """Test parsing simple transform hints."""
    transform_type, param = parse_transform_hint("month")
    assert transform_type == "month"
    assert param is None

    transform_type, param = parse_transform_hint("identity")
    assert transform_type == "identity"
    assert param is None


def test_parse_transform_hint_bucket():
    """Test parsing bucket transform hint."""
    transform_type, param = parse_transform_hint("bucket[10]")
    assert transform_type == "bucket"
    assert param == 10

    transform_type, param = parse_transform_hint("bucket[100]")
    assert transform_type == "bucket"
    assert param == 100


def test_parse_transform_hint_truncate():
    """Test parsing truncate transform hint."""
    transform_type, param = parse_transform_hint("truncate[5]")
    assert transform_type == "truncate"
    assert param == 5

    transform_type, param = parse_transform_hint("truncate[20]")
    assert transform_type == "truncate"
    assert param == 20


# Tests for bucket transform


def test_bucket_transform_on_integer():
    """Test bucket transform on integer field."""
    transform = choose_partition_transform(
        IntegerType(), "user_id", {"partition_transform": "bucket[10]"}
    )
    assert isinstance(transform, BucketTransform)
    assert transform.num_buckets == 10


def test_bucket_transform_on_long():
    """Test bucket transform on long field."""
    transform = choose_partition_transform(
        LongType(), "user_id", {"partition_transform": "bucket[50]"}
    )
    assert isinstance(transform, BucketTransform)
    assert transform.num_buckets == 50


def test_bucket_transform_on_string():
    """Test bucket transform on string field."""
    transform = choose_partition_transform(
        StringType(), "category", {"partition_transform": "bucket[20]"}
    )
    assert isinstance(transform, BucketTransform)
    assert transform.num_buckets == 20


def test_bucket_transform_invalid_on_timestamp():
    """Test that bucket transform validates properly on timestamp."""
    # This should actually work - bucket can be applied to timestamps
    transform = choose_partition_transform(
        TimestampType(), "created_at", {"partition_transform": "bucket[10]"}
    )
    assert isinstance(transform, BucketTransform)


# Tests for truncate transform


def test_truncate_transform_on_string():
    """Test truncate transform on string field."""
    transform = choose_partition_transform(
        StringType(), "email", {"partition_transform": "truncate[4]"}
    )
    assert isinstance(transform, TruncateTransform)
    assert transform.width == 4


def test_truncate_transform_on_integer():
    """Test truncate transform on integer field."""
    transform = choose_partition_transform(
        IntegerType(), "amount", {"partition_transform": "truncate[100]"}
    )
    assert isinstance(transform, TruncateTransform)
    assert transform.width == 100


def test_truncate_transform_on_long():
    """Test truncate transform on long field."""
    transform = choose_partition_transform(
        LongType(), "id", {"partition_transform": "truncate[1000]"}
    )
    assert isinstance(transform, TruncateTransform)
    assert transform.width == 1000


def test_truncate_transform_on_decimal():
    """Test truncate transform on decimal field."""
    transform = choose_partition_transform(
        DecimalType(10, 2), "price", {"partition_transform": "truncate[10]"}
    )
    assert isinstance(transform, TruncateTransform)
    assert transform.width == 10


def test_truncate_transform_invalid_on_timestamp():
    """Test that truncate transform fails on timestamp."""
    with pytest.raises(ValueError, match="Truncate transform cannot be applied"):
        choose_partition_transform(
            TimestampType(), "created_at", {"partition_transform": "truncate[10]"}
        )


def test_truncate_transform_invalid_on_double():
    """Test that truncate transform fails on double."""
    with pytest.raises(ValueError, match="Truncate transform cannot be applied"):
        choose_partition_transform(
            DoubleType(), "score", {"partition_transform": "truncate[10]"}
        )


# Tests for validation


def test_validate_temporal_transform_on_non_temporal():
    """Test that temporal transforms fail on non-temporal types."""
    with pytest.raises(ValueError, match="Temporal transform 'month' cannot be applied"):
        validate_transform_for_type("month", None, StringType(), "region")

    with pytest.raises(ValueError, match="Temporal transform 'day' cannot be applied"):
        validate_transform_for_type("day", None, IntegerType(), "user_id")


def test_validate_bucket_requires_positive_param():
    """Test that bucket transform requires positive parameter."""
    with pytest.raises(ValueError, match="Bucket transform requires a positive integer"):
        validate_transform_for_type("bucket", 0, IntegerType(), "user_id")

    with pytest.raises(ValueError, match="Bucket transform requires a positive integer"):
        validate_transform_for_type("bucket", -5, IntegerType(), "user_id")


def test_validate_truncate_requires_positive_param():
    """Test that truncate transform requires positive parameter."""
    with pytest.raises(ValueError, match="Truncate transform requires a positive integer"):
        validate_transform_for_type("truncate", 0, StringType(), "email")

    with pytest.raises(ValueError, match="Truncate transform requires a positive integer"):
        validate_transform_for_type("truncate", -1, StringType(), "email")


# Tests for build_partition_spec with new transforms


def test_build_partition_spec_with_bucket():
    """Test partition spec with bucket transform."""
    dlt_table = {
        "name": "test_table",
        "columns": {
            "id": {"data_type": "bigint", "nullable": False},
            "user_id": {
                "data_type": "bigint",
                "nullable": False,
                "partition": True,
                "partition_transform": "bucket[10]",
            },
        },
    }

    iceberg_schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "user_id", LongType(), required=True),
    )

    spec = build_partition_spec(dlt_table, iceberg_schema)

    assert spec is not None
    assert len(spec.fields) == 1

    partition_field = spec.fields[0]
    assert partition_field.source_id == 2
    assert isinstance(partition_field.transform, BucketTransform)
    assert partition_field.transform.num_buckets == 10
    assert partition_field.name == "user_id_bucket_10"


def test_build_partition_spec_with_truncate():
    """Test partition spec with truncate transform."""
    dlt_table = {
        "name": "test_table",
        "columns": {
            "id": {"data_type": "bigint", "nullable": False},
            "email": {
                "data_type": "text",
                "nullable": False,
                "partition": True,
                "partition_transform": "truncate[4]",
            },
        },
    }

    iceberg_schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "email", StringType(), required=True),
    )

    spec = build_partition_spec(dlt_table, iceberg_schema)

    assert spec is not None
    assert len(spec.fields) == 1

    partition_field = spec.fields[0]
    assert partition_field.source_id == 2
    assert isinstance(partition_field.transform, TruncateTransform)
    assert partition_field.transform.width == 4
    assert partition_field.name == "email_truncate_4"


def test_build_partition_spec_mixed_transforms():
    """Test partition spec with multiple different transforms."""
    dlt_table = {
        "name": "test_table",
        "columns": {
            "id": {"data_type": "bigint", "nullable": False},
            "user_id": {
                "data_type": "bigint",
                "nullable": False,
                "partition": True,
                "partition_transform": "bucket[10]",
            },
            "region": {
                "data_type": "text",
                "nullable": False,
                "partition": True,
                "partition_transform": "truncate[2]",
            },
            "created_at": {
                "data_type": "timestamp",
                "nullable": False,
                "partition": True,
                "partition_transform": "month",
            },
        },
    }

    iceberg_schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "user_id", LongType(), required=True),
        NestedField(3, "region", StringType(), required=True),
        NestedField(4, "created_at", TimestampType(), required=True),
    )

    spec = build_partition_spec(dlt_table, iceberg_schema)

    assert spec is not None
    assert len(spec.fields) == 3

    # Check each partition field
    assert isinstance(spec.fields[0].transform, BucketTransform)
    assert spec.fields[0].transform.num_buckets == 10

    assert isinstance(spec.fields[1].transform, TruncateTransform)
    assert spec.fields[1].transform.width == 2

    assert isinstance(spec.fields[2].transform, MonthTransform)


def test_get_transform_name_bucket():
    """Test get_transform_name for bucket transform."""
    transform = BucketTransform(num_buckets=10)
    assert get_transform_name(transform) == "bucket_10"


def test_get_transform_name_truncate():
    """Test get_transform_name for truncate transform."""
    transform = TruncateTransform(width=5)
    assert get_transform_name(transform) == "truncate_5"


def test_unknown_transform_type():
    """Test that unknown transform types raise error."""
    with pytest.raises(ValueError, match="Unknown transform type 'invalid'"):
        choose_partition_transform(
            StringType(), "test", {"partition_transform": "invalid[10]"}
        )
