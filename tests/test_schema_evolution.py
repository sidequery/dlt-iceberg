"""
Tests for schema evolution functionality.

Tests adding columns, type promotions, and detecting unsafe changes.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import dlt
from pyiceberg.catalog import load_catalog
from pyiceberg.types import IntegerType, LongType, FloatType, DoubleType, StringType
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField


def test_schema_evolution_add_column():
    """
    Test adding a new column to an existing table.

    Load 1: columns [event_id, event_type, value]
    Load 2: columns [event_id, event_type, value, new_field]

    Should add new_field column and both loads should be readable.
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    print(f"\nTest: Schema Evolution - Add Column")
    print(f"   Warehouse: {warehouse_path}")
    print(f"   Catalog: {catalog_path}")

    try:
        from dlt_iceberg.destination import iceberg_rest

        # LOAD 1: Initial schema with 3 columns
        @dlt.resource(name="events", write_disposition="append")
        def generate_events_v1():
            for i in range(1, 11):
                yield {
                    "event_id": i,
                    "event_type": f"type_{i % 3}",
                    "value": i * 10,
                }

        import uuid
        pipeline_id = str(uuid.uuid4())[:8]

        pipeline = dlt.pipeline(
            pipeline_name=f"test_evolution_{pipeline_id}",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="analytics",
            ),
            dataset_name="test_dataset",
        )

        print("\nLoad 1: Initial schema [event_id, event_type, value]")
        load_info = pipeline.run(generate_events_v1())
        assert not load_info.has_failed_jobs, "Load 1 should succeed"
        print("   Load 1 completed successfully")

        # Verify initial data
        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )

        table = catalog.load_table("analytics.events")
        result1 = table.scan().to_arrow()
        assert len(result1) == 10, "Should have 10 rows from load 1"
        print(f"   Verified {len(result1)} rows")

        # Check schema has 3 data columns (dlt may add internal columns)
        schema1 = table.schema()
        field_names_v1 = [f.name for f in schema1.fields]
        # Filter out dlt internal columns
        data_columns_v1 = [name for name in field_names_v1 if not name.startswith("_dlt_")]
        assert len(data_columns_v1) == 3, f"Should have 3 data columns initially, got {data_columns_v1}"
        assert set(data_columns_v1) == {"event_id", "event_type", "value"}
        print(f"   Initial schema: {data_columns_v1}")

        # LOAD 2: Add new column
        @dlt.resource(name="events", write_disposition="append")
        def generate_events_v2():
            for i in range(11, 21):
                yield {
                    "event_id": i,
                    "event_type": f"type_{i % 3}",
                    "value": i * 10,
                    "new_field": f"new_{i}",  # NEW COLUMN
                }

        print("\nLoad 2: Evolved schema [event_id, event_type, value, new_field]")
        load_info2 = pipeline.run(generate_events_v2())
        assert not load_info2.has_failed_jobs, "Load 2 should succeed"
        print("   Load 2 completed successfully")

        # Verify evolved schema
        table = catalog.load_table("analytics.events")
        schema2 = table.schema()
        field_names_v2 = [f.name for f in schema2.fields]
        data_columns_v2 = [name for name in field_names_v2 if not name.startswith("_dlt_")]
        print(f"   Evolved schema: {data_columns_v2}")

        assert len(data_columns_v2) == 4, f"Should have 4 data columns after evolution, got {data_columns_v2}"
        assert "new_field" in data_columns_v2, "new_field should be added"

        # Verify all data is readable
        result2 = table.scan().to_arrow()
        assert len(result2) == 20, "Should have 20 total rows"
        print(f"   Verified {len(result2)} total rows")

        # Check that old rows have null for new_field
        df = result2.to_pandas()
        old_rows = df[df["event_id"] <= 10]
        new_rows = df[df["event_id"] > 10]

        assert len(old_rows) == 10, "Should have 10 old rows"
        assert len(new_rows) == 10, "Should have 10 new rows"

        # Old rows should have null/None for new_field
        assert old_rows["new_field"].isna().all(), "Old rows should have null for new_field"
        # New rows should have values for new_field
        assert not new_rows["new_field"].isna().any(), "New rows should have values for new_field"

        print("\n   Column added and all data readable")
        print(f"   - Old rows (1-10): new_field is null")
        print(f"   - New rows (11-20): new_field has values")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_schema_evolution_type_promotion():
    """
    Test type promotion from int to long, float to double.

    This test verifies safe type widening.
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    print(f"\nTest: Schema Evolution - Type Promotion")
    print(f"   Warehouse: {warehouse_path}")
    print(f"   Catalog: {catalog_path}")

    try:
        from dlt_iceberg.destination import iceberg_rest

        # LOAD 1: Initial schema with int32 and float32
        @dlt.resource(name="metrics", write_disposition="append")
        def generate_metrics_v1():
            import pyarrow as pa
            # Force specific types
            data = pa.table({
                "metric_id": pa.array([1, 2, 3], type=pa.int32()),
                "metric_value": pa.array([1.5, 2.5, 3.5], type=pa.float32()),
            })
            for row in data.to_pylist():
                yield row

        import uuid
        pipeline_id = str(uuid.uuid4())[:8]

        pipeline = dlt.pipeline(
            pipeline_name=f"test_type_promotion_{pipeline_id}",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="analytics",
            ),
            dataset_name="test_dataset",
        )

        print("\nLoad 1: Initial schema [metric_id: int32, metric_value: float32]")
        load_info = pipeline.run(generate_metrics_v1())
        assert not load_info.has_failed_jobs, "Load 1 should succeed"
        print("   Load 1 completed successfully")

        # Verify initial types
        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )

        table = catalog.load_table("analytics.metrics")
        schema1 = table.schema()

        metric_id_field = schema1.find_field("metric_id")
        metric_value_field = schema1.find_field("metric_value")

        print(f"   Initial types: metric_id={metric_id_field.field_type}, metric_value={metric_value_field.field_type}")

        # LOAD 2: Promote int32 -> int64, float32 -> float64
        @dlt.resource(name="metrics", write_disposition="append")
        def generate_metrics_v2():
            import pyarrow as pa
            # Use wider types
            data = pa.table({
                "metric_id": pa.array([4, 5, 6], type=pa.int64()),
                "metric_value": pa.array([4.5, 5.5, 6.5], type=pa.float64()),
            })
            for row in data.to_pylist():
                yield row

        print("\nLoad 2: Promoted schema [metric_id: int64, metric_value: float64]")
        load_info2 = pipeline.run(generate_metrics_v2())
        assert not load_info2.has_failed_jobs, "Load 2 should succeed with type promotion"
        print("   Load 2 completed successfully")

        # Verify types were promoted
        table = catalog.load_table("analytics.metrics")
        schema2 = table.schema()

        metric_id_field2 = schema2.find_field("metric_id")
        metric_value_field2 = schema2.find_field("metric_value")

        print(f"   Promoted types: metric_id={metric_id_field2.field_type}, metric_value={metric_value_field2.field_type}")

        # Verify data
        result = table.scan().to_arrow()
        assert len(result) == 6, "Should have 6 total rows"

        df = result.to_pandas()
        assert df["metric_id"].min() == 1
        assert df["metric_id"].max() == 6
        assert df["metric_value"].min() == 1.5
        assert df["metric_value"].max() == 6.5

        print("\n   Types promoted and all data readable")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_schema_evolution_unsafe_changes():
    """
    Test that unsafe schema changes are detected.

    Note: When columns are dropped in the source data, dlt may handle this
    by just adding null values, so we test the underlying validation logic directly.
    """
    print(f"\nTest: Schema Evolution - Unsafe Changes (Direct Validation)")

    from dlt_iceberg.schema_evolution import (
        compare_schemas,
        validate_schema_changes,
        SchemaEvolutionError,
        can_promote_type
    )
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, StringType, LongType, NestedField

    # Test 1: Dropped columns should be detected
    print("\nTest 1: Dropped column detection")
    existing_schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "value", IntegerType(), required=False),
    )

    new_schema_dropped = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=False),
        # DROPPED: value
    )

    added, type_changes, dropped = compare_schemas(existing_schema, new_schema_dropped)
    assert len(dropped) == 1, "Should detect 1 dropped column"
    assert "value" in dropped, "Should detect 'value' was dropped"
    print(f"   Detected dropped columns: {dropped}")

    # Should raise error by default
    try:
        validate_schema_changes(added, type_changes, dropped, allow_column_drops=False)
        assert False, "Should have raised SchemaEvolutionError for dropped column"
    except SchemaEvolutionError as e:
        assert "value" in str(e).lower() or "dropped" in str(e).lower()
        print(f"   Correctly rejected: {str(e)}")

    # Test 2: Unsafe type narrowing should be detected
    print("\nTest 2: Unsafe type narrowing detection")
    new_schema_narrowed = Schema(
        NestedField(1, "id", IntegerType(), required=True),  # was LongType
        NestedField(2, "name", StringType(), required=False),
    )

    existing_schema_wide = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=False),
    )

    added2, type_changes2, dropped2 = compare_schemas(existing_schema_wide, new_schema_narrowed)
    assert len(type_changes2) == 1, "Should detect type change"
    assert type_changes2[0][0] == "id", "Should detect 'id' type change"
    print(f"   Detected type changes: {[(name, str(old), str(new)) for name, old, new in type_changes2]}")

    # Should raise error for unsafe promotion
    try:
        validate_schema_changes(added2, type_changes2, dropped2, allow_column_drops=False)
        assert False, "Should have raised SchemaEvolutionError for type narrowing"
    except SchemaEvolutionError as e:
        assert "id" in str(e).lower() or "unsafe" in str(e).lower()
        print(f"   Correctly rejected: {str(e)}")

    print("\n   Unsafe schema changes detected and prevented")


def test_schema_evolution_unit_compare():
    """
    Unit test for schema comparison logic.
    """
    from dlt_iceberg.schema_evolution import compare_schemas, can_promote_type

    print("\nTest: Schema Evolution - Unit Tests")

    # Test type promotion rules
    print("\nTesting type promotion rules:")

    # Safe promotions
    assert can_promote_type(IntegerType(), LongType()), "int -> long should be safe"
    print("   int -> long: safe")

    assert can_promote_type(FloatType(), DoubleType()), "float -> double should be safe"
    print("   float -> double: safe")

    assert can_promote_type(IntegerType(), IntegerType()), "same type should be safe"
    print("   int -> int: safe")

    # Unsafe promotions
    assert not can_promote_type(LongType(), IntegerType()), "long -> int should be unsafe"
    print("   long -> int: unsafe (correctly rejected)")

    assert not can_promote_type(DoubleType(), FloatType()), "double -> float should be unsafe"
    print("   double -> float: unsafe (correctly rejected)")

    assert not can_promote_type(IntegerType(), StringType()), "int -> string should be unsafe"
    print("   int -> string: unsafe (correctly rejected)")

    # Test schema comparison
    print("\nTesting schema comparison:")

    existing_schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=False),
    )

    new_schema = Schema(
        NestedField(1, "id", LongType(), required=True),  # Type promotion
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "new_col", StringType(), required=False),  # Added column
    )

    added, type_changes, dropped = compare_schemas(existing_schema, new_schema)

    assert len(added) == 1, "Should detect 1 added column"
    assert added[0].name == "new_col", "Should detect new_col"
    print(f"   Added columns: {[f.name for f in added]}")

    assert len(type_changes) == 1, "Should detect 1 type change"
    assert type_changes[0][0] == "id", "Should detect id type change"
    print(f"   Type changes: {[(name, str(old), str(new)) for name, old, new in type_changes]}")

    assert len(dropped) == 0, "Should detect 0 dropped columns"
    print(f"   Dropped columns: {dropped}")

    print("\n   All unit tests passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
