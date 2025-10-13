"""
End-to-end test using the actual dlt destination with iceberg_rest().
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import dlt
from pyiceberg.catalog import load_catalog


def test_destination_end_to_end():
    """
    End-to-end test:
    1. Creates dlt pipeline with iceberg_rest destination
    2. Loads data through dlt
    3. Verifies data in Iceberg catalog
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    print(f"\nTest environment:")
    print(f"   Warehouse: {warehouse_path}")
    print(f"   Catalog: {catalog_path}")

    try:
        # Create test data
        base_time = datetime(2024, 1, 1)

        @dlt.resource(name="events", write_disposition="append")
        def generate_events():
            for i in range(1, 26):
                yield {
                    "event_id": i,
                    "event_type": f"type_{i % 3}",
                    "event_timestamp": base_time + timedelta(hours=i),
                    "user_id": i % 10,
                    "value": i * 10,
                }

        print(f"\nCreated test data generator")

        # Import destination
        from dlt_iceberg.destination import iceberg_rest

        # Create dlt pipeline with our destination
        pipeline = dlt.pipeline(
            pipeline_name="test_iceberg",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="analytics",
            ),
            dataset_name="test_dataset",
        )

        print(f"Created dlt pipeline with iceberg_rest destination")

        # Load data through dlt
        print(f"\nLoading data through dlt...")
        load_info = pipeline.run(generate_events())

        print(f"DLT load completed")
        print(f"   Package info: {load_info.load_packages}")
        print(f"   Has failed jobs: {load_info.has_failed_jobs}")
        if load_info.has_failed_jobs:
            print(f"   Failed jobs: {load_info.failed_jobs}")
            raise AssertionError("Load had failed jobs!")

        # Verify data in Iceberg catalog
        print(f"\nVerifying data in Iceberg catalog...")

        catalog = load_catalog(
            "dlt_catalog",  # Use same name as destination
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )

        # List tables to see what was created
        namespaces = catalog.list_namespaces()
        print(f"   Namespaces: {namespaces}")
        for ns in namespaces:
            tables = catalog.list_tables(ns)
            print(f"   Tables in {ns}: {tables}")

        # Load table
        table = catalog.load_table("analytics.events")
        print(f"Loaded table from catalog: {table.name()}")

        # Scan data
        result = table.scan().to_arrow()
        print(f"Scanned data: {len(result)} rows")

        # Verify
        assert len(result) == 25, f"Expected 25 rows, got {len(result)}"

        df = result.to_pandas()
        assert df["event_id"].min() == 1
        assert df["event_id"].max() == 25
        assert len(df["event_type"].unique()) == 3

        print(f"Data verified")
        print(f"\nSample data:")
        print(df.head(10))

        # Test incremental load
        print(f"\nTesting incremental load...")

        @dlt.resource(name="events", write_disposition="append")
        def generate_more_events():
            for i in range(26, 36):
                yield {
                    "event_id": i,
                    "event_type": f"type_{i % 3}",
                    "event_timestamp": base_time + timedelta(hours=i),
                    "user_id": i % 10,
                    "value": i * 10,
                }

        load_info2 = pipeline.run(generate_more_events())
        print(f"Incremental load completed")

        # Verify incremental data
        table = catalog.load_table("analytics.events")
        result2 = table.scan().to_arrow()
        assert len(result2) == 35, f"Expected 35 rows after increment, got {len(result2)}"

        print(f"Incremental data verified: {len(result2)} total rows")

        print(f"\nDestination works end-to-end")
        print(f"\nSummary:")
        print(f"   Created dlt pipeline with iceberg_rest destination")
        print(f"   Loaded 25 rows through dlt")
        print(f"   Verified data in Iceberg catalog")
        print(f"   Incremental load added 10 more rows")
        print(f"   Total: 35 rows in Iceberg table")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
