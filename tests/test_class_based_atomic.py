"""
E2E test for class-based destination with atomic multi-file commits.

This test verifies that the new IcebergRestClient properly accumulates files
and commits them atomically via complete_load().
"""

import pytest
import tempfile
import shutil
from datetime import datetime, timedelta
import dlt


def test_class_based_atomic_commits():
    """
    Test that class-based destination commits multiple files atomically.

    Verifies:
    1. Multiple files are accumulated (not committed immediately)
    2. complete_load() commits all files in one snapshot
    3. Data is correct after atomic commit
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    print(f"\nTest environment:")
    print(f"   Warehouse: {warehouse_path}")
    print(f"   Catalog: {catalog_path}")

    try:
        # Import the class-based destination
        from dlt_iceberg import iceberg_rest

        base_time = datetime(2024, 1, 1)

        # Create a resource that will generate multiple files
        @dlt.resource(name="events", write_disposition="append")
        def generate_events():
            """Generate events that will be split into multiple files"""
            for i in range(1, 101):
                yield {
                    "event_id": i,
                    "event_type": f"type_{i % 5}",
                    "event_time": base_time + timedelta(hours=i),
                    "value": i * 10,
                }

        # Create pipeline with class-based destination
        print("\nCreating pipeline with class-based destination...")
        pipeline = dlt.pipeline(
            pipeline_name="test_atomic",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="test_ns",
            ),
            dataset_name="test_dataset",
        )

        print("Pipeline created")

        # Run pipeline - this should accumulate files and commit atomically
        print("\nRunning pipeline (should generate multiple files)...")
        load_info = pipeline.run(generate_events())

        if load_info.has_failed_jobs:
            print("Pipeline failed")
            for job in load_info.load_packages[0].jobs["failed_jobs"]:
                print(f"   Failed: {job.file_path}")
                print(f"   Error: {job.exception()}")
            pytest.fail("Pipeline run failed")

        print("Pipeline completed successfully")

        # Verify data was loaded
        print("\nVerifying atomic commit...")
        from pyiceberg.catalog import load_catalog

        # Use same catalog name as destination to share SQLite connection
        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )

        events_table = catalog.load_table("test_ns.events")

        # Check snapshot count - should be exactly 1 (atomic commit)
        snapshots = list(events_table.metadata.snapshots)
        print(f"   Snapshots created: {len(snapshots)}")

        # Read data
        events_data = events_table.scan().to_arrow()
        print(f"   Rows loaded: {len(events_data)}")

        # Verify atomicity
        assert len(snapshots) == 1, (
            f"Expected 1 snapshot (atomic commit), got {len(snapshots)}. "
            "Files were not committed atomically!"
        )

        # Verify data count
        assert len(events_data) == 100, f"Expected 100 rows, got {len(events_data)}"

        print("\nClass-based destination commits atomically")
        print(f"   1 snapshot (not {len(snapshots)} separate commits)")
        print(f"   100 rows loaded correctly")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_class_based_multiple_tables():
    """
    Test atomic commits across multiple tables.

    Each table should get 1 snapshot, not N snapshots for N files.
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    print(f"\nTest environment (multi-table):")
    print(f"   Warehouse: {warehouse_path}")

    try:
        from dlt_iceberg import iceberg_rest

        base_time = datetime(2024, 1, 1)

        @dlt.resource(name="events", write_disposition="append")
        def generate_events():
            for i in range(1, 51):
                yield {
                    "event_id": i,
                    "event_type": f"type_{i % 3}",
                    "timestamp": base_time + timedelta(hours=i),
                }

        @dlt.resource(name="users", write_disposition="append")
        def generate_users():
            for i in range(1, 31):
                yield {
                    "user_id": i,
                    "username": f"user_{i}",
                    "created_at": base_time + timedelta(days=i),
                }

        print("\nCreating pipeline...")
        pipeline = dlt.pipeline(
            pipeline_name="test_multi_table",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="test_ns",
            ),
            dataset_name="test_dataset",
        )

        print("Running pipeline with 2 tables...")
        load_info = pipeline.run([generate_events(), generate_users()])

        assert not load_info.has_failed_jobs, "Pipeline failed"

        print("Pipeline completed")

        # Verify each table has exactly 1 snapshot
        print("\nVerifying atomic commits per table...")
        from pyiceberg.catalog import load_catalog

        # Use same catalog name as destination to share SQLite connection
        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )

        events_table = catalog.load_table("test_ns.events")
        users_table = catalog.load_table("test_ns.users")

        events_snapshots = list(events_table.metadata.snapshots)
        users_snapshots = list(users_table.metadata.snapshots)

        print(f"   Events snapshots: {len(events_snapshots)}")
        print(f"   Users snapshots: {len(users_snapshots)}")

        assert len(events_snapshots) == 1, (
            f"Events table should have 1 snapshot, got {len(events_snapshots)}"
        )
        assert len(users_snapshots) == 1, (
            f"Users table should have 1 snapshot, got {len(users_snapshots)}"
        )

        # Verify data counts
        events_data = events_table.scan().to_arrow()
        users_data = users_table.scan().to_arrow()

        assert len(events_data) == 50
        assert len(users_data) == 30

        print("\nMultiple tables committed atomically")
        print(f"   Events: 1 snapshot, 50 rows")
        print(f"   Users: 1 snapshot, 30 rows")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_class_based_incremental():
    """
    Test that multiple pipeline runs create separate snapshots.

    Run 1: 1 snapshot with 50 rows
    Run 2: 1 snapshot with 30 rows
    Total: 2 snapshots, 80 rows
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    print(f"\nTest environment (incremental):")

    try:
        from dlt_iceberg import iceberg_rest

        base_time = datetime(2024, 1, 1)

        @dlt.resource(name="events", write_disposition="append")
        def generate_batch_1():
            for i in range(1, 51):
                yield {"event_id": i, "value": i * 10}

        @dlt.resource(name="events", write_disposition="append")
        def generate_batch_2():
            for i in range(51, 81):
                yield {"event_id": i, "value": i * 10}

        pipeline = dlt.pipeline(
            pipeline_name="test_incremental",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="test_ns",
            ),
            dataset_name="test_dataset",
        )

        print("\nRun 1: Loading batch 1 (50 rows)...")
        load_info1 = pipeline.run(generate_batch_1())
        assert not load_info1.has_failed_jobs

        print("Run 2: Loading batch 2 (30 rows)...")
        load_info2 = pipeline.run(generate_batch_2())
        assert not load_info2.has_failed_jobs

        print("Both runs completed")

        # Verify snapshots
        print("\nVerifying incremental snapshots...")
        from pyiceberg.catalog import load_catalog

        # Use same catalog name as destination to share SQLite connection
        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )

        events_table = catalog.load_table("test_ns.events")
        snapshots = list(events_table.metadata.snapshots)
        events_data = events_table.scan().to_arrow()

        print(f"   Total snapshots: {len(snapshots)}")
        print(f"   Total rows: {len(events_data)}")

        assert len(snapshots) == 2, (
            f"Expected 2 snapshots (1 per run), got {len(snapshots)}"
        )
        assert len(events_data) == 80, (
            f"Expected 80 rows total, got {len(events_data)}"
        )

        print("\nIncremental loads work correctly")
        print(f"   2 snapshots (1 per pipeline run)")
        print(f"   80 rows total")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
