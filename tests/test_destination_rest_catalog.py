"""
REST catalog e2e test using Nessie REST catalog.
Tests integration with REST catalogs like Nessie, Polaris, AWS Glue, etc.

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
    End-to-end test with Nessie REST catalog.

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

    print(f"\nCreated test data generator")

    from dlt_iceberg import iceberg_rest  # Class-based with atomic commits

    # Clean up: Drop table if exists from previous runs
    print(f"\nCleaning up from previous test runs...")
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
        print(f"Dropped existing table analytics.events")
    except Exception as e:
        print(f"No existing table to drop: {e}")

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

    print(f"Created dlt pipeline with Nessie REST catalog")

    # Load data through dlt
    print(f"\nLoading data through Nessie REST catalog...")
    load_info = pipeline.run(generate_events())

    print(f"DLT load completed")
    print(f"   Has failed jobs: {load_info.has_failed_jobs}")
    if load_info.has_failed_jobs:
        print(f"   Failed jobs: {load_info.failed_jobs}")
        raise AssertionError("Load had failed jobs!")

    # Verify data in Nessie catalog
    print(f"\nVerifying data in Nessie REST catalog...")

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
    print(f"Loaded table from Nessie: {table.name()}")

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

    print(f"\nDestination works with Nessie REST catalog")
    print(f"\nSummary:")
    print(f"   Created dlt pipeline with Nessie REST catalog")
    print(f"   Loaded 25 rows through dlt")
    print(f"   Verified data in Nessie REST catalog")
    print(f"   Incremental load added 10 more rows")
    print(f"   Total: 35 rows in Iceberg table")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_nessie_available(),
    reason="Nessie REST catalog not available. Run: docker compose up -d"
)
def test_rest_catalog_atomic_multi_file_commits():
    """
    Test atomic multi-file commits with REST catalog.

    Verifies that multiple files are committed in a single Iceberg snapshot,
    which is the key feature of the class-based destination.
    """
    from dlt_iceberg import iceberg_rest

    # Clean up
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
        cleanup_catalog.drop_table("test_atomic.multi_file_events")
    except Exception:
        pass

    @dlt.resource(name="multi_file_events", write_disposition="append")
    def generate_large_dataset():
        """Generate enough data to create multiple parquet files."""
        for i in range(1, 1001):
            yield {
                "id": i,
                "value": i * 100,
                "category": f"cat_{i % 5}",
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_atomic_multi_file",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="test_atomic",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="atomic_test",
    )

    load_info = pipeline.run(generate_large_dataset())
    assert not load_info.has_failed_jobs

    # Verify data
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

    table = catalog.load_table("test_atomic.multi_file_events")
    result = table.scan().to_arrow()

    assert len(result) == 1000

    # Verify atomic commit: should have exactly 1 snapshot
    snapshots = list(table.snapshots())
    assert len(snapshots) == 1, f"Expected 1 snapshot (atomic commit), got {len(snapshots)}"

    print(f"Atomic multi-file commit verified: 1000 rows in 1 snapshot")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_nessie_available(),
    reason="Nessie REST catalog not available. Run: docker compose up -d"
)
def test_rest_catalog_replace_disposition():
    """Test replace disposition with REST catalog."""
    from dlt_iceberg import iceberg_rest

    # Clean up
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
        cleanup_catalog.drop_table("test_replace.sales")
    except Exception:
        pass

    @dlt.resource(name="sales", write_disposition="replace")
    def generate_sales():
        for i in range(1, 101):
            yield {
                "sale_id": i,
                "region": f"region_{i % 4}",
                "amount": i * 50,
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_replace",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="test_replace",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="replace_test",
    )

    # Initial load
    load_info = pipeline.run(generate_sales())
    assert not load_info.has_failed_jobs

    # Verify initial data
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

    table = catalog.load_table("test_replace.sales")
    result = table.scan().to_arrow()
    assert len(result) == 100

    # Replace with new data
    @dlt.resource(name="sales", write_disposition="replace")
    def new_sales():
        for i in range(1, 51):
            yield {
                "sale_id": i + 1000,
                "region": f"region_{i % 2}",
                "amount": i * 100,
            }

    load_info = pipeline.run(new_sales())
    assert not load_info.has_failed_jobs

    # Verify replaced data
    table = catalog.load_table("test_replace.sales")
    result = table.scan().to_arrow()

    assert len(result) == 50, f"Expected 50 rows after replace, got {len(result)}"

    df = result.to_pandas()
    assert df["sale_id"].min() == 1001, "Data should be replaced, not appended"

    print(f"Replace disposition verified: {len(result)} rows, old data replaced")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_nessie_available(),
    reason="Nessie REST catalog not available. Run: docker compose up -d"
)
def test_rest_catalog_merge_disposition():
    """Test merge (upsert) disposition with REST catalog."""
    from dlt_iceberg import iceberg_rest

    # Clean up
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
        cleanup_catalog.drop_table("test_merge.users")
    except Exception:
        pass

    @dlt.resource(name="users", write_disposition="merge", primary_key="user_id")
    def initial_users():
        for i in range(1, 11):
            yield {
                "user_id": i,
                "name": f"User {i}",
                "status": "active",
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_merge",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="test_merge",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="merge_test",
    )

    # Initial load
    load_info = pipeline.run(initial_users())
    assert not load_info.has_failed_jobs

    # Verify initial data
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

    table = catalog.load_table("test_merge.users")
    result = table.scan().to_arrow()
    assert len(result) == 10

    # Merge update: update some existing + add new
    @dlt.resource(name="users", write_disposition="merge", primary_key="user_id")
    def updated_users():
        # Update existing users
        for i in [1, 2, 3]:
            yield {
                "user_id": i,
                "name": f"User {i}",
                "status": "inactive",
            }
        # Add new users
        for i in range(11, 16):
            yield {
                "user_id": i,
                "name": f"User {i}",
                "status": "active",
            }

    load_info = pipeline.run(updated_users())
    assert not load_info.has_failed_jobs

    # Verify merged data
    table = catalog.load_table("test_merge.users")
    result = table.scan().to_arrow()

    assert len(result) == 15, f"Expected 15 rows after merge, got {len(result)}"

    df = result.to_pandas()
    inactive_count = len(df[df["status"] == "inactive"])
    assert inactive_count == 3, f"Expected 3 inactive users, got {inactive_count}"

    print(f"Merge disposition verified: {len(result)} rows, 3 updated to inactive")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_nessie_available(),
    reason="Nessie REST catalog not available. Run: docker compose up -d"
)
def test_rest_catalog_incremental_loads():
    """Test incremental loads with cursor tracking with REST catalog."""
    from dlt_iceberg import iceberg_rest

    # Clean up
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
        cleanup_catalog.drop_table("test_incremental.events")
    except Exception:
        pass

    base_time = datetime(2024, 1, 1)

    @dlt.resource(name="events", write_disposition="append")
    def events_batch_1():
        for i in range(1, 51):
            yield {
                "event_id": i,
                "timestamp": base_time + timedelta(hours=i),
                "value": i * 10,
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_incremental",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="test_incremental",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="incremental_test",
    )

    # Initial load
    load_info = pipeline.run(events_batch_1())
    assert not load_info.has_failed_jobs

    # Verify initial data
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

    table = catalog.load_table("test_incremental.events")
    result = table.scan().to_arrow()
    assert len(result) == 50

    # Incremental load 2
    @dlt.resource(name="events", write_disposition="append")
    def events_batch_2():
        for i in range(51, 101):
            yield {
                "event_id": i,
                "timestamp": base_time + timedelta(hours=i),
                "value": i * 10,
            }

    load_info = pipeline.run(events_batch_2())
    assert not load_info.has_failed_jobs

    # Verify incremental data
    table = catalog.load_table("test_incremental.events")
    result = table.scan().to_arrow()

    assert len(result) == 100, f"Expected 100 rows after incremental load, got {len(result)}"

    df = result.to_pandas()
    assert df["event_id"].min() == 1
    assert df["event_id"].max() == 100

    # Verify atomic commits: should have at least 1 snapshot
    snapshots = list(table.snapshots())
    assert len(snapshots) >= 1, f"Expected at least 1 snapshot, got {len(snapshots)}"

    print(f"Incremental loads verified: {len(result)} rows")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_nessie_available(),
    reason="Nessie REST catalog not available. Run: docker compose up -d"
)
def test_rest_catalog_multiple_tables():
    """Test loading multiple tables in a single pipeline run with REST catalog."""
    from dlt_iceberg import iceberg_rest

    # Clean up
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
    for table_name in ["orders", "customers", "line_items"]:
        try:
            cleanup_catalog.drop_table(f"test_multi.{table_name}")
        except Exception:
            pass

    @dlt.resource(name="orders", write_disposition="append")
    def generate_orders():
        for i in range(1, 21):
            yield {
                "order_id": i,
                "customer_id": i % 5,
                "total": i * 100.0,
            }

    @dlt.resource(name="customers", write_disposition="append")
    def generate_customers():
        for i in range(1, 6):
            yield {
                "customer_id": i,
                "name": f"Customer {i}",
                "email": f"customer{i}@example.com",
            }

    @dlt.resource(name="line_items", write_disposition="append")
    def generate_line_items():
        for i in range(1, 51):
            yield {
                "line_item_id": i,
                "order_id": (i % 20) + 1,
                "product": f"Product {i % 10}",
                "quantity": i % 5 + 1,
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_multi_table",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="test_multi",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="multi_table_test",
    )

    # Load all three tables
    load_info = pipeline.run([generate_orders(), generate_customers(), generate_line_items()])
    assert not load_info.has_failed_jobs

    # Verify all tables
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

    orders_table = catalog.load_table("test_multi.orders")
    customers_table = catalog.load_table("test_multi.customers")
    line_items_table = catalog.load_table("test_multi.line_items")

    orders_result = orders_table.scan().to_arrow()
    customers_result = customers_table.scan().to_arrow()
    line_items_result = line_items_table.scan().to_arrow()

    assert len(orders_result) == 20
    assert len(customers_result) == 5
    assert len(line_items_result) == 50

    # Verify atomic commits: each table should have exactly 1 snapshot
    assert len(list(orders_table.snapshots())) == 1
    assert len(list(customers_table.snapshots())) == 1
    assert len(list(line_items_table.snapshots())) == 1

    print(f"Multiple tables verified: orders={len(orders_result)}, customers={len(customers_result)}, line_items={len(line_items_result)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
