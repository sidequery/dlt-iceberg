"""
End-to-end tests for state synchronization functionality.

Tests that verify schema restoration from destination when running
pipelines from different execution contexts.
"""

import pytest
import tempfile
import shutil
import dlt
from pyiceberg.catalog import load_catalog


def test_fresh_pipeline_restores_schema_from_destination():
    """
    Test that fresh pipeline instances can restore schemas from the destination.

    This requires IcebergRestClient to implement WithStateSync.
    With WithStateSync, a fresh pipeline can restore the schema from the
    destination, allowing it to handle incoming data that is missing columns.

    Scenario (simulating different execution contexts):
    - Pipeline 1 (context A): Creates table with columns [a, b, c, d]
    - Pipeline 2 (context B): Fresh instance, loads data with only [a, b, c]

    Expected behavior: Pipeline 2 restores schema from destination,
    recognizes column 'd' exists, and writes NULL for missing values.
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    print("\nTest: Fresh Pipeline Restores Schema from Destination")
    print(f"   Warehouse: {warehouse_path}")
    print(f"   Catalog: {catalog_path}")

    try:
        from dlt_iceberg import iceberg_rest
        import uuid

        pipeline_name = f"test_state_sync_{uuid.uuid4().hex[:8]}"

        # PIPELINE 1 (Context A): Create table with columns a, b, c, d
        @dlt.resource(name="test_table", write_disposition="append")
        def generate_data_v1():
            for i in range(1, 6):
                yield {
                    "a": i,
                    "b": i * 10,
                    "c": f"value_{i}",
                    "d": f"optional_{i}",
                }

        pipeline1 = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="test",
            ),
            dataset_name="test_dataset",
        )

        print("\nPipeline 1 (Context A): Create table with [a, b, c, d]")
        load_info1 = pipeline1.run(generate_data_v1())
        assert not load_info1.has_failed_jobs, "Pipeline 1 load should succeed"
        print("   Pipeline 1 completed successfully")

        # Verify table was created with all columns
        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )
        table = catalog.load_table("test.test_table")
        table_columns = [f.name for f in table.schema().fields]
        print(f"   Table columns: {table_columns}")
        assert "d" in table_columns, "Column 'd' should exist in table"

        # Drop pipeline 1's local state to simulate different execution context
        pipeline1.drop()
        print("   Dropped pipeline 1 local state (simulating context switch)")

        # PIPELINE 2 (Context B): Fresh instance, same pipeline_name, missing column 'd'
        @dlt.resource(name="test_table", write_disposition="append")
        def generate_data_v2():
            for i in range(6, 11):
                yield {
                    "a": i,
                    "b": i * 10,
                    "c": f"value_{i}",
                    # 'd' is intentionally missing
                }

        pipeline2 = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="test",
            ),
            dataset_name="test_dataset",
        )

        print("\nPipeline 2 (Context B): Append data with [a, b, c] (missing 'd')")

        # With WithStateSync implemented, Pipeline 2 should restore schema
        # from destination and succeed with 'd' as NULL for new rows
        load_info2 = pipeline2.run(generate_data_v2())
        assert not load_info2.has_failed_jobs, (
            "Pipeline 2 should succeed after restoring schema from destination"
        )
        print("   Pipeline 2 completed successfully")

        # Verify all data is present
        table = catalog.load_table("test.test_table")
        result = table.scan().to_arrow()
        assert len(result) == 10, f"Should have 10 total rows, got {len(result)}"

        df = result.to_pandas()
        old_rows = df[df["a"] <= 5]
        new_rows = df[df["a"] > 5]

        assert len(old_rows) == 5, "Should have 5 old rows"
        assert len(new_rows) == 5, "Should have 5 new rows"

        # Old rows should have values for 'd'
        assert not old_rows["d"].isna().any(), "Old rows should have values for 'd'"
        # New rows should have NULL for 'd'
        assert new_rows["d"].isna().all(), "New rows should have NULL for 'd'"

        print("   Verified: old rows have 'd' values, new rows have 'd' as NULL")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_derive_schema_from_iceberg_when_dlt_version_empty():
    """
    Test that schema is derived from Iceberg tables when _dlt_version is empty.

    When _dlt_version has no stored schema but the Iceberg table already exists,
    the destination should derive the schema from the existing Iceberg table
    metadata to avoid treating existing columns as "dropped".

    Scenario:
    1. Pipeline 1 creates table with columns [a, b, c, d]
    2. _dlt_version table is deleted (simulating corrupted/empty state)
    3. Pipeline 2 runs with data containing only [a, b, c] (missing 'd')
    4. Without schema derivation: fails with "columns dropped: d"
    5. With schema derivation: succeeds, 'd' is NULL for new rows
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    print("\nTest: Derive Schema from Iceberg When _dlt_version Empty")
    print(f"   Warehouse: {warehouse_path}")
    print(f"   Catalog: {catalog_path}")

    try:
        from dlt_iceberg import iceberg_rest
        import uuid

        pipeline_name = f"test_derive_schema_{uuid.uuid4().hex[:8]}"

        # PIPELINE 1: Create table with columns a, b, c, d
        @dlt.resource(name="test_table", write_disposition="append")
        def generate_data_v1():
            for i in range(1, 6):
                yield {
                    "a": i,
                    "b": i * 10,
                    "c": f"value_{i}",
                    "d": f"optional_{i}",
                }

        pipeline1 = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="test",
            ),
            dataset_name="test_dataset",
        )

        print("\nPipeline 1: Create table with [a, b, c, d]")
        load_info1 = pipeline1.run(generate_data_v1())
        assert not load_info1.has_failed_jobs, "Pipeline 1 load should succeed"
        print("   Pipeline 1 completed successfully")

        # Verify table was created with all columns
        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )
        table = catalog.load_table("test.test_table")
        table_columns = [f.name for f in table.schema().fields]
        print(f"   Table columns: {table_columns}")
        assert "d" in table_columns, "Column 'd' should exist in table"

        # Verify _dlt_version exists
        version_table = catalog.load_table("test._dlt_version")
        version_data = version_table.scan().to_arrow()
        print(f"   _dlt_version has {len(version_data)} rows")
        assert len(version_data) > 0, "_dlt_version should have data"

        # DELETE _dlt_version to simulate corrupted/empty state
        catalog.drop_table("test._dlt_version")
        print("   Deleted _dlt_version table (simulating empty/corrupted state)")

        # Drop pipeline 1's local state to simulate different execution context
        pipeline1.drop()
        print("   Dropped pipeline 1 local state")

        # PIPELINE 2: Fresh instance, data missing column 'd'
        @dlt.resource(name="test_table", write_disposition="append")
        def generate_data_v2():
            for i in range(6, 11):
                yield {
                    "a": i,
                    "b": i * 10,
                    "c": f"value_{i}",
                    # 'd' is intentionally missing
                }

        pipeline2 = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="test",
            ),
            dataset_name="test_dataset",
        )

        print("\nPipeline 2: Append data with [a, b, c] (missing 'd', no _dlt_version)")

        # This should succeed by deriving schema from existing Iceberg table
        load_info2 = pipeline2.run(generate_data_v2())
        assert not load_info2.has_failed_jobs, (
            "Pipeline 2 should succeed by deriving schema from Iceberg table"
        )
        print("   Pipeline 2 completed successfully")

        # Verify all data is present
        table = catalog.load_table("test.test_table")
        result = table.scan().to_arrow()
        assert len(result) == 10, f"Should have 10 total rows, got {len(result)}"

        df = result.to_pandas()
        old_rows = df[df["a"] <= 5]
        new_rows = df[df["a"] > 5]

        assert len(old_rows) == 5, "Should have 5 old rows"
        assert len(new_rows) == 5, "Should have 5 new rows"

        # Old rows should have values for 'd'
        assert not old_rows["d"].isna().any(), "Old rows should have values for 'd'"
        # New rows should have NULL for 'd'
        assert new_rows["d"].isna().all(), "New rows should have NULL for 'd'"

        print("   Verified: old rows have 'd' values, new rows have 'd' as NULL")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
