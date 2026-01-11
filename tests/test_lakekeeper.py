"""
Lakekeeper catalog e2e tests.

Tests integration with Lakekeeper REST catalog which supports credential vending.

Prerequisites:
    1. Start docker services:
       docker compose up -d

    2. Wait for services to be healthy:
       docker compose ps

    3. Verify Lakekeeper is ready:
       curl http://localhost:8282/health

    4. Run this test:
       uv run pytest tests/test_lakekeeper.py -v -s

Services required:
    - Lakekeeper (REST catalog): http://localhost:8282
    - MinIO (S3 storage): http://localhost:9002
"""

import pytest
import dlt
import requests
from datetime import datetime, timedelta
from pyiceberg.catalog import load_catalog


def is_lakekeeper_available():
    """Check if Lakekeeper REST catalog is accessible with warehouse configured."""
    try:
        response = requests.get("http://localhost:8282/health", timeout=2)
        if response.status_code != 200:
            return False
        response = requests.get("http://localhost:8282/management/v1/warehouse", timeout=2)
        if response.status_code != 200:
            return False
        data = response.json()
        warehouses = data.get("warehouses", [])
        return any(w.get("name") == "test-warehouse" for w in warehouses)
    except Exception:
        return False


def _setup_host_docker_internal():
    """Make 'host.docker.internal' resolvable to localhost on the host machine.

    Lakekeeper vends 'host.docker.internal:9002' as the S3 endpoint. This works
    inside Docker containers (via extra_hosts), but from the host we need to
    resolve it to 127.0.0.1.
    """
    import socket
    _original_getaddrinfo = socket.getaddrinfo

    def _patched_getaddrinfo(host, port, *args, **kwargs):
        if host == "host.docker.internal":
            host = "127.0.0.1"
        return _original_getaddrinfo(host, port, *args, **kwargs)

    socket.getaddrinfo = _patched_getaddrinfo


_setup_host_docker_internal()


def get_lakekeeper_catalog(name: str = "lakekeeper"):
    """Get a PyIceberg catalog connected to Lakekeeper."""
    return load_catalog(
        name,
        type="rest",
        uri="http://localhost:8282/catalog/",
        warehouse="test-warehouse",
    )


def cleanup_table(namespace: str, table_name: str):
    """Drop table if exists."""
    try:
        catalog = get_lakekeeper_catalog("cleanup")
        catalog.drop_table(f"{namespace}.{table_name}")
        print(f"Dropped existing table {namespace}.{table_name}")
    except Exception as e:
        print(f"No existing table to drop: {e}")


def get_lakekeeper_destination(namespace: str = "lk_test"):
    """Get iceberg_rest destination configured for Lakekeeper."""
    from dlt_iceberg import iceberg_rest
    return iceberg_rest(
        catalog_uri="http://localhost:8282/catalog/",
        namespace=namespace,
        warehouse="test-warehouse",
    )


@pytest.mark.integration
@pytest.mark.skipif(
    not is_lakekeeper_available(),
    reason="Lakekeeper catalog not available. Run: docker compose up -d"
)
def test_lakekeeper_basic_load():
    """
    Basic end-to-end test with Lakekeeper REST catalog.

    This test verifies:
    1. dlt pipeline creation with Lakekeeper
    2. Initial data load
    3. Data verification via PyIceberg
    4. Incremental load
    """
    cleanup_table("lk_basic", "events")

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
        pipeline_name="test_lakekeeper_basic",
        destination=get_lakekeeper_destination("lk_basic"),
        dataset_name="lk_basic_dataset",
    )

    print("\nLoading data through Lakekeeper...")
    load_info = pipeline.run(generate_events())

    assert not load_info.has_failed_jobs, f"Load failed: {load_info.failed_jobs}"
    print("Initial load completed")

    # Verify data
    catalog = get_lakekeeper_catalog("verify")
    table = catalog.load_table("lk_basic.events")
    result = table.scan().to_arrow()

    assert len(result) == 25, f"Expected 25 rows, got {len(result)}"
    print(f"Verified {len(result)} rows in Lakekeeper")

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

    table = catalog.load_table("lk_basic.events")
    result = table.scan().to_arrow()
    assert len(result) == 35, f"Expected 35 rows, got {len(result)}"

    print(f"Incremental load verified: {len(result)} total rows")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_lakekeeper_available(),
    reason="Lakekeeper catalog not available. Run: docker compose up -d"
)
def test_lakekeeper_merge_upsert():
    """Test merge/upsert with Lakekeeper."""
    cleanup_table("lk_merge", "users")

    @dlt.resource(name="users", write_disposition="merge", primary_key="user_id")
    def initial_users():
        for i in range(1, 11):
            yield {
                "user_id": i,
                "name": f"User {i}",
                "status": "active",
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_lakekeeper_merge",
        destination=get_lakekeeper_destination("lk_merge"),
        dataset_name="lk_merge_dataset",
    )

    # Initial load
    load_info = pipeline.run(initial_users())
    assert not load_info.has_failed_jobs

    catalog = get_lakekeeper_catalog("verify")
    table = catalog.load_table("lk_merge.users")
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

    table = catalog.load_table("lk_merge.users")
    result = table.scan().to_arrow()

    assert len(result) == 15, f"Expected 15 rows after merge, got {len(result)}"

    df = result.to_pandas()
    inactive_count = len(df[df["status"] == "inactive"])
    assert inactive_count == 3, f"Expected 3 inactive users, got {inactive_count}"

    print(f"Merge verified: {len(result)} rows, {inactive_count} inactive")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_lakekeeper_available(),
    reason="Lakekeeper catalog not available. Run: docker compose up -d"
)
def test_lakekeeper_delete_insert():
    """Test delete-insert merge strategy with Lakekeeper."""
    cleanup_table("lk_delete", "products")

    @dlt.resource(
        name="products",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        primary_key="product_id"
    )
    def initial_products():
        for i in range(1, 11):
            yield {
                "product_id": i,
                "name": f"Product {i}",
                "price": i * 10.0,
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_lakekeeper_delete_insert",
        destination=get_lakekeeper_destination("lk_delete"),
        dataset_name="lk_delete_dataset",
    )

    # Initial load
    load_info = pipeline.run(initial_products())
    assert not load_info.has_failed_jobs

    catalog = get_lakekeeper_catalog("verify")
    table = catalog.load_table("lk_delete.products")
    result = table.scan().to_arrow()
    assert len(result) == 10

    # Delete-insert: update prices for some products
    @dlt.resource(
        name="products",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        primary_key="product_id"
    )
    def updated_products():
        for i in [1, 2, 3]:
            yield {
                "product_id": i,
                "name": f"Product {i} Updated",
                "price": i * 20.0,  # doubled prices
            }

    load_info = pipeline.run(updated_products())
    assert not load_info.has_failed_jobs

    table = catalog.load_table("lk_delete.products")
    result = table.scan().to_arrow()
    df = result.to_pandas()

    # Should still have 10 rows
    assert len(result) == 10, f"Expected 10 rows, got {len(result)}"

    # Check updated prices
    updated = df[df["product_id"].isin([1, 2, 3])]
    for _, row in updated.iterrows():
        expected_price = row["product_id"] * 20.0
        assert row["price"] == expected_price, f"Product {row['product_id']} price not updated"

    print(f"Delete-insert verified: {len(result)} rows, 3 updated")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_lakekeeper_available(),
    reason="Lakekeeper catalog not available. Run: docker compose up -d"
)
def test_lakekeeper_replace():
    """Test replace disposition with Lakekeeper."""
    cleanup_table("lk_replace", "metrics")

    @dlt.resource(name="metrics", write_disposition="replace")
    def initial_metrics():
        for i in range(1, 101):
            yield {"metric_id": i, "value": i * 10}

    pipeline = dlt.pipeline(
        pipeline_name="test_lakekeeper_replace",
        destination=get_lakekeeper_destination("lk_replace"),
        dataset_name="lk_replace_dataset",
    )

    # Initial load
    load_info = pipeline.run(initial_metrics())
    assert not load_info.has_failed_jobs

    catalog = get_lakekeeper_catalog("verify")
    table = catalog.load_table("lk_replace.metrics")
    result = table.scan().to_arrow()
    assert len(result) == 100

    # Replace with new data
    @dlt.resource(name="metrics", write_disposition="replace")
    def new_metrics():
        for i in range(1, 51):
            yield {"metric_id": i + 1000, "value": i * 100}

    load_info = pipeline.run(new_metrics())
    assert not load_info.has_failed_jobs

    table = catalog.load_table("lk_replace.metrics")
    result = table.scan().to_arrow()

    assert len(result) == 50, f"Expected 50 rows after replace, got {len(result)}"

    df = result.to_pandas()
    assert df["metric_id"].min() == 1001, "Data should be replaced"

    print(f"Replace verified: {len(result)} rows, old data replaced")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_lakekeeper_available(),
    reason="Lakekeeper catalog not available. Run: docker compose up -d"
)
def test_lakekeeper_multiple_tables():
    """Test loading multiple tables with Lakekeeper."""
    for table in ["orders", "customers"]:
        cleanup_table("lk_multi", table)

    @dlt.resource(name="orders", write_disposition="append")
    def orders():
        for i in range(1, 21):
            yield {"order_id": i, "customer_id": i % 5, "total": i * 100.0}

    @dlt.resource(name="customers", write_disposition="append")
    def customers():
        for i in range(1, 6):
            yield {"customer_id": i, "name": f"Customer {i}"}

    pipeline = dlt.pipeline(
        pipeline_name="test_lakekeeper_multi",
        destination=get_lakekeeper_destination("lk_multi"),
        dataset_name="lk_multi_dataset",
    )

    load_info = pipeline.run([orders(), customers()])
    assert not load_info.has_failed_jobs

    catalog = get_lakekeeper_catalog("verify")

    orders_table = catalog.load_table("lk_multi.orders")
    customers_table = catalog.load_table("lk_multi.customers")

    orders_result = orders_table.scan().to_arrow()
    customers_result = customers_table.scan().to_arrow()

    assert len(orders_result) == 20
    assert len(customers_result) == 5

    # Verify atomic commits
    assert len(list(orders_table.snapshots())) == 1
    assert len(list(customers_table.snapshots())) == 1

    print(f"Multiple tables verified: orders={len(orders_result)}, customers={len(customers_result)}")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_lakekeeper_available(),
    reason="Lakekeeper catalog not available. Run: docker compose up -d"
)
def test_lakekeeper_hard_delete():
    """Test hard delete functionality with Lakekeeper."""
    cleanup_table("lk_hard_delete", "users")

    @dlt.resource(
        name="users",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        primary_key="user_id"
    )
    def initial_users():
        for i in range(1, 6):
            yield {"user_id": i, "name": f"User {i}", "_dlt_deleted_at": None}

    pipeline = dlt.pipeline(
        pipeline_name="test_lakekeeper_hard_delete",
        destination=get_lakekeeper_destination("lk_hard_delete"),
        dataset_name="lk_hard_delete_dataset",
    )

    # Initial load
    load_info = pipeline.run(initial_users())
    assert not load_info.has_failed_jobs

    catalog = get_lakekeeper_catalog("verify")
    table = catalog.load_table("lk_hard_delete.users")
    result = table.scan().to_arrow()
    assert len(result) == 5

    # Hard delete users 1 and 2
    @dlt.resource(
        name="users",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        primary_key="user_id"
    )
    def delete_users():
        yield {"user_id": 1, "name": "User 1", "_dlt_deleted_at": datetime.now()}
        yield {"user_id": 2, "name": "User 2", "_dlt_deleted_at": datetime.now()}

    load_info = pipeline.run(delete_users())
    assert not load_info.has_failed_jobs

    table = catalog.load_table("lk_hard_delete.users")
    result = table.scan().to_arrow()

    # Should have 3 users remaining
    assert len(result) == 3, f"Expected 3 rows after hard delete, got {len(result)}"

    df = result.to_pandas()
    remaining_ids = set(df["user_id"].tolist())
    assert remaining_ids == {3, 4, 5}, f"Expected users 3,4,5 remaining, got {remaining_ids}"

    print(f"Hard delete verified: {len(result)} rows remaining")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
