"""
Tests for schema conversion from dlt to Iceberg.
"""

import pytest
import pyarrow as pa
from pyiceberg.types import (
    StringType,
    LongType,
    IntegerType,
    BooleanType,
    TimestampType,
    DoubleType,
)

from dlt_iceberg.schema_converter import (
    convert_dlt_to_iceberg_schema,
    convert_arrow_to_iceberg_type,
)


def test_convert_arrow_to_iceberg_basic_types():
    """Test conversion of basic PyArrow types to Iceberg types."""
    assert isinstance(convert_arrow_to_iceberg_type(pa.bool_()), BooleanType)
    assert isinstance(convert_arrow_to_iceberg_type(pa.int32()), IntegerType)
    assert isinstance(convert_arrow_to_iceberg_type(pa.int64()), LongType)
    assert isinstance(convert_arrow_to_iceberg_type(pa.float64()), DoubleType)
    assert isinstance(convert_arrow_to_iceberg_type(pa.string()), StringType)
    assert isinstance(convert_arrow_to_iceberg_type(pa.timestamp("us")), TimestampType)


def test_convert_dlt_to_iceberg_schema():
    """Test full schema conversion from dlt to Iceberg."""

    # Create sample dlt table schema
    dlt_table = {
        "name": "test_table",
        "columns": {
            "id": {"data_type": "bigint", "nullable": False},
            "name": {"data_type": "text", "nullable": True},
            "active": {"data_type": "bool", "nullable": True},
            "created_at": {"data_type": "timestamp", "nullable": False},
        },
    }

    # Create corresponding PyArrow table
    arrow_table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            "active": pa.array([True, False, True], type=pa.bool_()),
            "created_at": pa.array(
                [1704067200000000, 1704153600000000, 1704240000000000],
                type=pa.timestamp("us"),
            ),
        }
    )

    # Convert
    iceberg_schema = convert_dlt_to_iceberg_schema(dlt_table, arrow_table)

    # Verify schema
    assert len(iceberg_schema.fields) == 4

    # Check field names
    field_names = [f.name for f in iceberg_schema.fields]
    assert "id" in field_names
    assert "name" in field_names
    assert "active" in field_names
    assert "created_at" in field_names

    # Check field types
    id_field = [f for f in iceberg_schema.fields if f.name == "id"][0]
    assert isinstance(id_field.field_type, LongType)
    assert id_field.required  # Not nullable

    name_field = [f for f in iceberg_schema.fields if f.name == "name"][0]
    assert isinstance(name_field.field_type, StringType)
    assert not name_field.required  # Nullable

    active_field = [f for f in iceberg_schema.fields if f.name == "active"][0]
    assert isinstance(active_field.field_type, BooleanType)

    created_field = [f for f in iceberg_schema.fields if f.name == "created_at"][0]
    assert isinstance(created_field.field_type, TimestampType)
    assert created_field.required  # Not nullable


def test_complex_types():
    """Test conversion of complex PyArrow types."""
    from pyiceberg.types import ListType, StructType

    # List type
    list_type = pa.list_(pa.string())
    iceberg_list = convert_arrow_to_iceberg_type(list_type)
    assert isinstance(iceberg_list, ListType)
    assert isinstance(iceberg_list.element_type, StringType)

    # Struct type
    struct_type = pa.struct([("field1", pa.string()), ("field2", pa.int64())])
    iceberg_struct = convert_arrow_to_iceberg_type(struct_type)
    assert isinstance(iceberg_struct, StructType)
    assert len(iceberg_struct.fields) == 2
