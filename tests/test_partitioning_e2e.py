"""End-to-end partitioning tests with REST catalog."""

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
def test_partitioned_table_creation():
    """Test creating a partitioned table with temporal partitioning."""
    from dlt_iceberg import iceberg_rest

    # Clean up
    cleanup_catalog = load_catalog(
        "cleanup",
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
        cleanup_catalog.drop_table("test_partitions.events")
    except:
        pass

    base_date = datetime(2024, 1, 1)

    @dlt.resource(
        name="events",
        write_disposition="append",
        columns={
            "event_date": {
                "data_type": "date",
                "x-partition": True,
                "x-partition-transform": "day",
            },
            "region": {
                "data_type": "text",
                "x-partition": True,
            }
        }
    )
    def generate_events():
        for i in range(10):
            yield {
                "event_id": i,
                "event_date": base_date + timedelta(days=i % 3),
                "region": f"region_{i % 2}",
                "value": i * 100,
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_partitioning",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="test_partitions",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="test",
    )

    load_info = pipeline.run(generate_events())
    assert not load_info.has_failed_jobs

    # Verify partitioning
    catalog = load_catalog(
        "verify",
        type="rest",
        uri="http://localhost:19120/iceberg/main",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
        },
    )

    table = catalog.load_table("test_partitions.events")

    # Check partition spec
    partition_spec = table.spec()
    assert len(partition_spec.fields) == 2, f"Expected 2 partition fields, got {len(partition_spec.fields)}"

    # Verify data
    result = table.scan().to_arrow()
    assert len(result) == 10

    print(f"Created partitioned table with {len(partition_spec.fields)} partition fields")
    for field in partition_spec.fields:
        print(f"  {field}")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_nessie_available(),
    reason="Nessie REST catalog not available. Run: docker compose up -d"
)
def test_bucket_partitioning():
    """Test bucket partitioning on integer column."""
    from dlt_iceberg import iceberg_rest

    # Clean up
    cleanup_catalog = load_catalog(
        "cleanup",
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
        cleanup_catalog.drop_table("test_bucket.users")
    except:
        pass

    @dlt.resource(
        name="users",
        write_disposition="append",
        columns={
            "user_id": {
                "data_type": "bigint",
                "x-partition": True,
                "x-partition-transform": "bucket[10]",
            }
        }
    )
    def generate_users():
        for i in range(100):
            yield {
                "user_id": i,
                "name": f"User {i}",
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_bucket",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="test_bucket",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="test",
    )

    load_info = pipeline.run(generate_users())
    assert not load_info.has_failed_jobs

    # Verify bucket partitioning
    catalog = load_catalog(
        "verify",
        type="rest",
        uri="http://localhost:19120/iceberg/main",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
        },
    )

    table = catalog.load_table("test_bucket.users")
    partition_spec = table.spec()

    assert len(partition_spec.fields) == 1
    # Check that it's a bucket transform
    field = partition_spec.fields[0]
    assert "bucket" in str(field.transform).lower()

    result = table.scan().to_arrow()
    assert len(result) == 100

    print(f"Created bucket-partitioned table: {field}")


@pytest.mark.integration
@pytest.mark.skipif(
    not is_nessie_available(),
    reason="Nessie REST catalog not available. Run: docker compose up -d"
)
def test_mixed_partitioning():
    """Test table with multiple partition transform types."""
    from dlt_iceberg import iceberg_rest

    # Clean up
    cleanup_catalog = load_catalog(
        "cleanup",
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
        cleanup_catalog.drop_table("test_mixed.sales")
    except:
        pass

    base_time = datetime(2024, 1, 1, 0, 0, 0)

    @dlt.resource(
        name="sales",
        write_disposition="append",
        columns={
            "sale_timestamp": {
                "data_type": "timestamp",
                "x-partition": True,
                "x-partition-transform": "hour",
            },
            "region": {
                "data_type": "text",
                "x-partition": True,  # Identity transform
            },
            "customer_id": {
                "data_type": "bigint",
                "x-partition": True,
                "x-partition-transform": "bucket[5]",
            }
        }
    )
    def generate_sales():
        for i in range(20):
            yield {
                "sale_id": i,
                "sale_timestamp": base_time + timedelta(hours=i % 6),
                "region": f"region_{i % 3}",
                "customer_id": i % 10,
                "amount": i * 50.0,
            }

    pipeline = dlt.pipeline(
        pipeline_name="test_mixed",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="test_mixed",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="test",
    )

    load_info = pipeline.run(generate_sales())
    assert not load_info.has_failed_jobs

    # Verify mixed partitioning
    catalog = load_catalog(
        "verify",
        type="rest",
        uri="http://localhost:19120/iceberg/main",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
        },
    )

    table = catalog.load_table("test_mixed.sales")
    partition_spec = table.spec()

    assert len(partition_spec.fields) == 3, f"Expected 3 partition fields, got {len(partition_spec.fields)}"

    result = table.scan().to_arrow()
    assert len(result) == 20

    print(f"Created mixed-partition table with {len(partition_spec.fields)} fields:")
    for field in partition_spec.fields:
        print(f"  {field}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
