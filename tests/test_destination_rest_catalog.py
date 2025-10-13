"""
REAL REST CATALOG E2E TEST: Uses actual Nessie REST catalog.
This proves the destination works with real REST catalogs like Nessie, Polaris, AWS Glue, etc.

Prerequisites:
    1. Start docker services:
       docker compose up -d

    2. Wait for services to be healthy (Nessie takes ~30 seconds):
       docker compose ps

    3. Verify Nessie is ready:
       curl http://localhost:19120/api/v2/config

    4. Run this test:
       uv run pytest tests/test_destination_rest_catalog.py -v -s

Services required:
    - Nessie (REST catalog): http://localhost:19120
    - MinIO (S3 storage): http://localhost:9000
"""

import pytest
import dlt
import requests
from datetime import datetime, timedelta
from pyiceberg.catalog import load_catalog


def is_nessie_available():
    """Check if Nessie REST catalog is accessible."""
    try:
        response = requests.get("http://localhost:19120/api/v2/config", timeout=2)
        return response.status_code == 200
    except Exception:
        return False


@pytest.mark.integration
@pytest.mark.skipif(
    not is_nessie_available(),
    reason="Nessie REST catalog not available. Run: docker compose up -d"
)
def test_destination_with_nessie_rest_catalog():
    """
    END-TO-END TEST with Nessie REST catalog.

    This test verifies:
    1. dlt pipeline creation with REST catalog
    2. Initial data load (25 rows)
    3. Data verification in Nessie catalog
    4. Incremental load (10 more rows)
    5. Total data verification (35 rows)

    The test cleans up before running (drops table if exists)
    so it can be run multiple times reliably.
    """
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

    print(f"\n✅ Created test data generator")

    from sidequery_dlt.destination import iceberg_rest

    # Clean up: Drop table if exists from previous runs
    print(f"\n🧹 Cleaning up from previous test runs...")
    cleanup_catalog = load_catalog(
        "nessie_cleanup",
        type="rest",
        uri="http://localhost:19120/iceberg/main",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
        },
    )
    try:
        cleanup_catalog.drop_table("analytics.events")
        print(f"✅ Dropped existing table analytics.events")
    except Exception as e:
        print(f"ℹ️  No existing table to drop: {e}")

    # Create dlt pipeline with Nessie REST catalog
    pipeline = dlt.pipeline(
        pipeline_name="test_nessie_rest",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="analytics",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="test_dataset",
    )

    print(f"✅ Created dlt pipeline with Nessie REST catalog")

    # Load data through dlt
    print(f"\n✍️  Loading data through Nessie REST catalog...")
    load_info = pipeline.run(generate_events())

    print(f"✅ DLT LOAD COMPLETED!")
    print(f"   Has failed jobs: {load_info.has_failed_jobs}")
    if load_info.has_failed_jobs:
        print(f"   Failed jobs: {load_info.failed_jobs}")
        raise AssertionError("Load had failed jobs!")

    # Verify data in Nessie catalog
    print(f"\n🔍 Verifying data in Nessie REST catalog...")

    catalog = load_catalog(
        "nessie_verify",
        type="rest",
        uri="http://localhost:19120/iceberg/main",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
        },
    )

    # List tables
    namespaces = catalog.list_namespaces()
    print(f"   Namespaces: {namespaces}")
    for ns in namespaces:
        tables = catalog.list_tables(ns)
        print(f"   Tables in {ns}: {tables}")

    # Load table
    table = catalog.load_table("analytics.events")
    print(f"✅ Loaded table from Nessie: {table.name()}")

    # Scan data
    result = table.scan().to_arrow()
    print(f"✅ Scanned data: {len(result)} rows")

    # Verify
    assert len(result) == 25, f"Expected 25 rows, got {len(result)}"

    df = result.to_pandas()
    assert df["event_id"].min() == 1
    assert df["event_id"].max() == 25
    assert len(df["event_type"].unique()) == 3

    print(f"✅ Data verified!")
    print(f"\nSample data:")
    print(df.head(10))

    # Test incremental load
    print(f"\n📦 Testing incremental load...")

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
    print(f"✅ Incremental load completed")

    # Verify incremental data
    table = catalog.load_table("analytics.events")
    result2 = table.scan().to_arrow()
    assert len(result2) == 35, f"Expected 35 rows after increment, got {len(result2)}"

    print(f"✅ Incremental data verified: {len(result2)} total rows")

    print(f"\n🎉 SUCCESS! DESTINATION WORKS WITH NESSIE REST CATALOG!")
    print(f"\n Summary:")
    print(f"   ✅ Created dlt pipeline with Nessie REST catalog")
    print(f"   ✅ Loaded 25 rows through dlt")
    print(f"   ✅ Verified data in Nessie REST catalog")
    print(f"   ✅ Incremental load added 10 more rows")
    print(f"   ✅ Total: 35 rows in Iceberg table")
    print(f"\n THIS PROVES REST CATALOG WORKS!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
