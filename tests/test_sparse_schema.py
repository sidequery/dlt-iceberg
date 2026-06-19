"""
Tests for sparse schema support and allow_column_drops implementation.

Covers three behaviors:
1. Sparse incoming data (fewer columns than existing schema) should not raise
   SchemaEvolutionError. Columns remain in the table; new rows get nulls.
2. cast_table_safe should fill missing columns with nulls before casting.
3. allow_column_drops=True should remove columns from the Iceberg schema via
   apply_schema_evolution.
"""

import pytest
import tempfile
import shutil
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    LongType,
    StringType,
    DoubleType,
)

from dlt_iceberg.schema_evolution import (
    compare_schemas,
    validate_schema_changes,
    apply_schema_evolution,
    evolve_schema_if_needed,
    SchemaEvolutionError,
)
from dlt_iceberg.schema_casting import cast_table_safe, CastingError


# ---------------------------------------------------------------------------
# Bug 1: Sparse data should not raise SchemaEvolutionError
# ---------------------------------------------------------------------------


def test_validate_sparse_data_allow_column_drops_false():
    """Sparse incoming data with allow_column_drops=False leaves columns
    in the schema and fills nulls at write time."""
    added = []
    type_changes = []
    dropped = ["extra_col_a", "extra_col_b"]

    validate_schema_changes(
        added, type_changes, dropped, allow_column_drops=False
    )


def test_validate_sparse_data_allow_column_drops_true():
    """allow_column_drops=True does not raise; columns are removed via
    apply_schema_evolution instead."""
    added = []
    type_changes = []
    dropped = ["extra_col_a"]

    validate_schema_changes(
        added, type_changes, dropped, allow_column_drops=True
    )


def test_evolve_schema_if_needed_sparse_data():
    """evolve_schema_if_needed with sparse data and allow_column_drops=False
    preserves the full table schema without error."""
    temp_dir = tempfile.mkdtemp()

    print(f"\nTest: Sparse data - evolve_schema_if_needed")
    print(f"   Temp dir: {temp_dir}")

    try:
        from pyiceberg.catalog import load_catalog

        catalog = load_catalog(
            "test",
            type="sql",
            uri=f"sqlite:///{temp_dir}/catalog.db",
            warehouse=f"file://{temp_dir}/warehouse",
        )
        catalog.create_namespace("ns")

        wide_schema = Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "name", StringType(), required=False),
            NestedField(3, "score", DoubleType(), required=False),
        )
        table = catalog.create_table("ns.wide_table", schema=wide_schema)
        print(f"   Created table with schema: {[f.name for f in wide_schema.fields]}")

        narrow_schema = Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )
        print(f"   Incoming schema (sparse): {[f.name for f in narrow_schema.fields]}")

        result = evolve_schema_if_needed(
            table, narrow_schema, allow_column_drops=False
        )

        refreshed = catalog.load_table("ns.wide_table")
        field_names = [f.name for f in refreshed.schema().fields]
        assert "score" in field_names, (
            "score column should remain in schema for sparse data"
        )
        print(f"   Table schema preserved: {field_names}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Bug 2: cast_table_safe should fill missing columns with nulls
# ---------------------------------------------------------------------------


def test_cast_missing_column_filled_with_nulls():
    """A column present in the target but absent in the source is filled
    with nulls."""
    source_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
    ])
    target_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float64(), nullable=True),
    ])

    table = pa.table(
        {"id": [1, 2, 3], "name": ["a", "b", "c"]},
        schema=source_schema,
    )

    result = cast_table_safe(table, target_schema, strict=False)

    assert len(result) == 3
    assert result.schema == target_schema
    assert result.column("score").to_pylist() == [None, None, None]


def test_cast_multiple_missing_columns_filled():
    """Multiple missing columns are all filled with nulls."""
    source_schema = pa.schema([
        pa.field("id", pa.int64()),
    ])
    target_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float64()),
        pa.field("active", pa.bool_()),
    ])

    table = pa.table({"id": [1, 2]}, schema=source_schema)

    result = cast_table_safe(table, target_schema, strict=False)

    assert len(result) == 2
    assert result.schema == target_schema
    assert result.column("name").to_pylist() == [None, None]
    assert result.column("score").to_pylist() == [None, None]
    assert result.column("active").to_pylist() == [None, None]


def test_cast_missing_columns_work_in_strict_mode():
    """Missing nullable columns are warnings, not errors, so strict mode succeeds."""
    source_schema = pa.schema([
        pa.field("id", pa.int64()),
    ])
    target_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("value", pa.int64(), nullable=True),
    ])

    table = pa.table({"id": [10]}, schema=source_schema)

    result = cast_table_safe(table, target_schema, strict=True)

    assert len(result) == 1
    assert result.schema == target_schema
    assert result.column("value").to_pylist() == [None]


def test_cast_missing_required_column_fails_non_strict():
    """Missing required columns are rejected instead of null-filled."""
    source_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
    ])
    target_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("value", pa.int64(), nullable=False),
    ])

    table = pa.table({"id": [10]}, schema=source_schema)

    with pytest.raises(CastingError) as exc_info:
        cast_table_safe(table, target_schema, strict=False)

    assert "required field 'value'" in str(exc_info.value).lower()
    assert "not in source schema" in str(exc_info.value).lower()


def test_cast_missing_required_column_fails_strict_mode():
    """Strict mode does not silently proceed for missing required columns."""
    source_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
    ])
    target_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("value", pa.int64(), nullable=False),
    ])

    table = pa.table({"id": [10]}, schema=source_schema)

    with pytest.raises(CastingError) as exc_info:
        cast_table_safe(table, target_schema, strict=True)

    assert "required field 'value'" in str(exc_info.value).lower()
    assert "not in source schema" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# Bug 3: allow_column_drops=True should actually remove columns
# ---------------------------------------------------------------------------


def test_apply_schema_evolution_deletes_columns():
    """apply_schema_evolution removes columns listed in dropped_fields."""
    temp_dir = tempfile.mkdtemp()

    print(f"\nTest: apply_schema_evolution with dropped_fields")
    print(f"   Temp dir: {temp_dir}")

    try:
        from pyiceberg.catalog import load_catalog

        catalog = load_catalog(
            "test",
            type="sql",
            uri=f"sqlite:///{temp_dir}/catalog.db",
            warehouse=f"file://{temp_dir}/warehouse",
        )
        catalog.create_namespace("ns")

        schema = Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "name", StringType(), required=False),
            NestedField(3, "obsolete", StringType(), required=False),
        )
        table = catalog.create_table("ns.drop_test", schema=schema)
        print(f"   Created table with schema: {[f.name for f in schema.fields]}")

        apply_schema_evolution(
            table,
            added_fields=[],
            type_changes=[],
            dropped_fields=["obsolete"],
        )

        refreshed = catalog.load_table("ns.drop_test")
        field_names = [f.name for f in refreshed.schema().fields]
        assert "obsolete" not in field_names
        assert "id" in field_names
        assert "name" in field_names
        print(f"   Schema after drop: {field_names}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_evolve_schema_drops_columns_when_allowed():
    """evolve_schema_if_needed with allow_column_drops=True removes columns
    missing from the incoming schema."""
    temp_dir = tempfile.mkdtemp()

    print(f"\nTest: evolve_schema_if_needed with allow_column_drops=True")
    print(f"   Temp dir: {temp_dir}")

    try:
        from pyiceberg.catalog import load_catalog

        catalog = load_catalog(
            "test",
            type="sql",
            uri=f"sqlite:///{temp_dir}/catalog.db",
            warehouse=f"file://{temp_dir}/warehouse",
        )
        catalog.create_namespace("ns")

        schema = Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "name", StringType(), required=False),
            NestedField(3, "to_drop", StringType(), required=False),
        )
        table = catalog.create_table("ns.evolve_drop", schema=schema)
        print(f"   Created table with schema: {[f.name for f in schema.fields]}")

        narrow_schema = Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )
        print(f"   Incoming schema: {[f.name for f in narrow_schema.fields]}")

        evolved = evolve_schema_if_needed(
            table, narrow_schema, allow_column_drops=True
        )
        assert evolved, "Schema should have evolved (column dropped)"

        refreshed = catalog.load_table("ns.evolve_drop")
        field_names = [f.name for f in refreshed.schema().fields]
        assert "to_drop" not in field_names
        print(f"   Schema after evolution: {field_names}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
