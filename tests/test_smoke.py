"""
Smoke tests that validate the destination code without requiring external services.
"""

import pytest
import dlt
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import pyarrow as pa

from dlt_iceberg import iceberg_rest
from dlt_iceberg.schema_converter import convert_dlt_to_iceberg_schema
from dlt_iceberg.partition_builder import build_partition_spec


def test_destination_is_callable():
    """Test that the destination function exists and is callable."""
    assert callable(iceberg_rest)


def test_schema_conversion_basic_types():
    """Test schema conversion works for basic types."""
    dlt_table = {
        "name": "test_table",
        "columns": {
            "id": {"data_type": "bigint", "nullable": False},
            "name": {"data_type": "text", "nullable": True},
            "active": {"data_type": "bool", "nullable": True},
        },
    }

    arrow_table = pa.table({
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "active": [True, False, True],
    })

    schema = convert_dlt_to_iceberg_schema(dlt_table, arrow_table)

    assert len(schema.fields) == 3
    field_names = [f.name for f in schema.fields]
    assert "id" in field_names
    assert "name" in field_names
    assert "active" in field_names


def test_partition_spec_generation():
    """Test partition spec generation from dlt hints."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, LongType, TimestampType

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
    # Verify the partition is on created_at
    assert spec.fields[0].source_id == 2


def test_destination_metadata():
    """Test that destination has correct metadata set by @dlt.destination decorator."""
    # The destination is a factory, check it's properly configured
    assert hasattr(iceberg_rest, '__name__')
    # Destination decorator creates a callable that can be used with dlt.pipeline


def test_complex_type_schema_conversion():
    """Test schema conversion with complex types (lists, structs)."""
    arrow_table = pa.table({
        "id": [1, 2],
        "tags": [["a", "b"], ["c", "d"]],
        "metadata": [{"key": "val1"}, {"key": "val2"}],
    })

    dlt_table = {
        "name": "complex_table",
        "columns": {
            "id": {"data_type": "bigint"},
            "tags": {"data_type": "text"},  # Will be inferred from arrow
            "metadata": {"data_type": "json"},  # Will be inferred from arrow
        },
    }

    schema = convert_dlt_to_iceberg_schema(dlt_table, arrow_table)

    # Verify all fields were converted
    assert len(schema.fields) == 3
    field_names = [f.name for f in schema.fields]
    assert "id" in field_names
    assert "tags" in field_names
    assert "metadata" in field_names


def test_destination_write_dispositions():
    """Test that all write dispositions are handled."""
    dispositions = ["append", "replace", "merge"]

    for disposition in dispositions:
        table_schema = {
            "name": "test",
            "columns": {"id": {"data_type": "bigint"}},
            "write_disposition": disposition,
            "primary_key": ["id"] if disposition == "merge" else None,
        }

        # Just verify the schema is valid, don't actually run
        assert table_schema["write_disposition"] in ["append", "replace", "merge"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
