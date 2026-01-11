"""
Unity Catalog e2e tests.

Tests integration with Unity Catalog REST catalog.

NOTE: Unity Catalog OSS (as of v0.3.1) does NOT support Iceberg table writes
via the Iceberg REST API. The /iceberg endpoint is read-only and intended for
reading Delta tables via UniForm. These tests are placeholders for when
Unity Catalog adds Iceberg write support.

See: https://github.com/unitycatalog/unitycatalog

Prerequisites:
    1. Start docker services:
       docker compose up -d

    2. Wait for services to be healthy:
       docker compose ps

    3. Verify Unity Catalog is ready:
       curl http://localhost:8083/api/2.1/unity-catalog/catalogs

    4. Run this test:
       uv run pytest tests/test_unity_catalog.py -v -s

Services required:
    - Unity Catalog: http://localhost:8083
"""

import pytest
import dlt
import requests
from datetime import datetime, timedelta
from pyiceberg.catalog import load_catalog


# NOTE: Unity Catalog OSS (as of v0.3.1) doesn't support Iceberg writes yet.
# These tests will fail until Unity Catalog adds Iceberg REST write support.


def is_unity_catalog_available():
    """Check if Unity Catalog is accessible with schema configured."""
    try:
        response = requests.get(
            "http://localhost:8083/api/2.1/unity-catalog/catalogs",
            timeout=5
        )
        if response.status_code != 200:
            return False
        # Check if dlt_test schema exists
        response = requests.get(
            "http://localhost:8083/api/2.1/unity-catalog/schemas",
            params={"catalog_name": "unity"},
            timeout=5
        )
        if response.status_code != 200:
            return False
        data = response.json()
        schemas = data.get("schemas", [])
        return any(s.get("name") == "dlt_test" for s in schemas)
    except Exception:
        return False


def get_unity_catalog(name: str = "unity"):
    """Get a PyIceberg catalog connected to Unity Catalog."""
    return load_catalog(
        name,
        type="rest",
        uri="http://localhost:8083/api/2.1/unity-catalog/iceberg",
        warehouse="unity",
    )


def cleanup_table(namespace: str, table_name: str):
    """Drop table if exists."""
    try:
        catalog = get_unity_catalog("cleanup")
        catalog.drop_table(f"{namespace}.{table_name}")
        print(f"Dropped existing table {namespace}.{table_name}")
    except Exception as e:
        print(f"No existing table to drop: {e}")


def get_unity_destination(namespace: str = "dlt_test"):
    """Get iceberg_rest destination configured for Unity Catalog."""
    from dlt_iceberg import iceberg_rest
    return iceberg_rest(
        catalog_uri="http://localhost:8083/api/2.1/unity-catalog/iceberg",
        namespace=namespace,
        warehouse="unity",
    )


@pytest.mark.integration
@pytest.mark.skipif(
    not is_unity_catalog_available(),
    reason="Unity Catalog not available. Run: docker compose up -d"
)
def test_unity_catalog_basic_load():
    """
    Basic end-to-end test with Unity Catalog.

    This test verifies:
    1. dlt pipeline creation with Unity Catalog
    2. Initial data load
    3. Data verification via PyIceberg
    4. Incremental load
    """
    cleanup_table("dlt_test", "events")

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

    pipeline = dlt.pipeline(
        pipeline_name="test_unity_basic",
        destination=get_unity_destination("dlt_test"),
        dataset_name="dlt_test_dataset",
    )

    print("\nLoading data through Unity Catalog...")
    load_info = pipeline.run(generate_events())

    assert not load_info.has_failed_jobs, f"Load failed: {load_info.failed_jobs}"
    print("Initial load completed")

    # Verify data
    catalog = get_unity_catalog("verify")
    table = catalog.load_table("dlt_test.events")
    result = table.scan().to_arrow()

    assert len(result) == 25, f"Expected 25 rows, got {len(result)}"
    print(f"Verified {len(result)} rows in Unity Catalog")

    # Test incremental load
    @dlt.resource(name="events", write_disposition="append")
    def more_events():
        for i in range(26, 36):
            yield {
                "event_id": i,
                "event_type": f"type_{i % 3}",
                "event_timestamp": base_time + timedelta(hours=i),
                "user_id": i % 10,
                "value": i * 10,
            }

    load_info = pipeline.run(more_events())
    assert not load_info.has_failed_jobs

    table = catalog.load_table("dlt_test.events")
    result = table.scan().to_arrow()
    assert len(result) == 35, f"Expected 35 rows, got {len(result)}"

    print(f"Incremental load verified: {len(result)} total rows")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_unity_catalog_available(),
    reason="Unity Catalog not available. Run: docker compose up -d"
)
def test_unity_catalog_merge_upsert():
    """Test merge/upsert with Unity Catalog."""
    cleanup_table("dlt_test", "users")

    @dlt.resource(name="users", write_disposition="merge", primary_key="user_id")
    def initial_users():
        for i in range(1, 11):
            yield {
                "user_id": i,
                "name": f"User {i}",
                "status": "active",
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_unity_merge",
        destination=get_unity_destination("dlt_test"),
        dataset_name="dlt_test_merge",
    )

    # Initial load
    load_info = pipeline.run(initial_users())
    assert not load_info.has_failed_jobs

    catalog = get_unity_catalog("verify")
    table = catalog.load_table("dlt_test.users")
    result = table.scan().to_arrow()
    assert len(result) == 10

    # Upsert: update some, add new
    @dlt.resource(name="users", write_disposition="merge", primary_key="user_id")
    def updated_users():
        # Update existing
        for i in [1, 2, 3]:
            yield {"user_id": i, "name": f"User {i}", "status": "inactive"}
        # Add new
        for i in range(11, 16):
            yield {"user_id": i, "name": f"User {i}", "status": "active"}

    load_info = pipeline.run(updated_users())
    assert not load_info.has_failed_jobs

    table = catalog.load_table("dlt_test.users")
    result = table.scan().to_arrow()

    assert len(result) == 15, f"Expected 15 rows after merge, got {len(result)}"

    df = result.to_pandas()
    inactive_count = len(df[df["status"] == "inactive"])
    assert inactive_count == 3, f"Expected 3 inactive users, got {inactive_count}"

    print(f"Merge verified: {len(result)} rows, {inactive_count} inactive")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_unity_catalog_available(),
    reason="Unity Catalog not available. Run: docker compose up -d"
)
def test_unity_catalog_replace():
    """Test replace disposition with Unity Catalog."""
    cleanup_table("dlt_test", "metrics")

    @dlt.resource(name="metrics", write_disposition="replace")
    def initial_metrics():
        for i in range(1, 101):
            yield {"metric_id": i, "value": i * 10}

    pipeline = dlt.pipeline(
        pipeline_name="test_unity_replace",
        destination=get_unity_destination("dlt_test"),
        dataset_name="dlt_test_replace",
    )

    # Initial load
    load_info = pipeline.run(initial_metrics())
    assert not load_info.has_failed_jobs

    catalog = get_unity_catalog("verify")
    table = catalog.load_table("dlt_test.metrics")
    result = table.scan().to_arrow()
    assert len(result) == 100

    # Replace with new data
    @dlt.resource(name="metrics", write_disposition="replace")
    def new_metrics():
        for i in range(1, 51):
            yield {"metric_id": i + 1000, "value": i * 100}

    load_info = pipeline.run(new_metrics())
    assert not load_info.has_failed_jobs

    table = catalog.load_table("dlt_test.metrics")
    result = table.scan().to_arrow()

    assert len(result) == 50, f"Expected 50 rows after replace, got {len(result)}"

    df = result.to_pandas()
    assert df["metric_id"].min() == 1001, "Data should be replaced"

    print(f"Replace verified: {len(result)} rows, old data replaced")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_unity_catalog_available(),
    reason="Unity Catalog not available. Run: docker compose up -d"
)
def test_unity_catalog_multiple_tables():
    """Test loading multiple tables with Unity Catalog."""
    for table in ["orders", "customers"]:
        cleanup_table("dlt_test", table)

    @dlt.resource(name="orders", write_disposition="append")
    def orders():
        for i in range(1, 21):
            yield {"order_id": i, "customer_id": i % 5, "total": i * 100.0}

    @dlt.resource(name="customers", write_disposition="append")
    def customers():
        for i in range(1, 6):
            yield {"customer_id": i, "name": f"Customer {i}"}

    pipeline = dlt.pipeline(
        pipeline_name="test_unity_multi",
        destination=get_unity_destination("dlt_test"),
        dataset_name="dlt_test_multi",
    )

    load_info = pipeline.run([orders(), customers()])
    assert not load_info.has_failed_jobs

    catalog = get_unity_catalog("verify")

    orders_table = catalog.load_table("dlt_test.orders")
    customers_table = catalog.load_table("dlt_test.customers")

    orders_result = orders_table.scan().to_arrow()
    customers_result = customers_table.scan().to_arrow()

    assert len(orders_result) == 20
    assert len(customers_result) == 5

    print(f"Multiple tables verified: orders={len(orders_result)}, customers={len(customers_result)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
