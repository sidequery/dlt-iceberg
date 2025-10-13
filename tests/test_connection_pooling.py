"""
Test catalog connection pooling in the iceberg_rest destination.

This test verifies that the destination reuses catalog connections instead of
creating a new one for each file.
"""

import pytest
import tempfile
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path
import dlt


def test_catalog_connection_pooling(caplog):
    """
    Test that catalog connections are pooled and reused.

    This creates a pipeline that loads multiple batches of data,
    and verifies that only ONE catalog connection is created despite
    processing multiple files.
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    # Set logging level to capture connection creation messages
    caplog.set_level(logging.INFO, logger="sidequery_dlt.destination")

    print(f"\nTest environment:")
    print(f"   Warehouse: {warehouse_path}")
    print(f"   Catalog: {catalog_path}")

    try:
        # Create test data source that yields multiple batches
        base_time = datetime(2024, 1, 1)

        @dlt.resource(name="events", write_disposition="append")
        def generate_events_batch_1():
            """First batch of events"""
            for i in range(1, 11):
                yield {
                    "event_id": i,
                    "event_type": f"type_{i % 3}",
                    "event_timestamp": base_time + timedelta(hours=i),
                    "value": i * 10,
                }

        @dlt.resource(name="events", write_disposition="append")
        def generate_events_batch_2():
            """Second batch of events"""
            for i in range(11, 21):
                yield {
                    "event_id": i,
                    "event_type": f"type_{i % 3}",
                    "event_timestamp": base_time + timedelta(hours=i),
                    "value": i * 10,
                }

        @dlt.resource(name="users", write_disposition="append")
        def generate_users():
            """User data (different table)"""
            for i in range(1, 6):
                yield {
                    "user_id": i,
                    "username": f"user_{i}",
                    "created_at": base_time + timedelta(days=i),
                }

        # Import destination
        from sidequery_dlt.destination import iceberg_rest

        # Create dlt pipeline
        pipeline = dlt.pipeline(
            pipeline_name="test_pooling",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="test_ns",
            ),
            dataset_name="test_dataset",
        )

        print("\nLoading first batch (events)...")
        caplog.clear()
        load_info1 = pipeline.run(generate_events_batch_1())
        assert not load_info1.has_failed_jobs, "First load failed"

        # Count catalog creation messages
        new_catalog_logs_1 = [
            record for record in caplog.records
            if "Creating NEW catalog connection" in record.message
        ]
        print(f"   Catalog creations in batch 1: {len(new_catalog_logs_1)}")

        print("\nLoading second batch (events - same table)...")
        caplog.clear()
        load_info2 = pipeline.run(generate_events_batch_2())
        assert not load_info2.has_failed_jobs, "Second load failed"

        # Count catalog creation messages
        new_catalog_logs_2 = [
            record for record in caplog.records
            if "Creating NEW catalog connection" in record.message
        ]
        print(f"   Catalog creations in batch 2: {len(new_catalog_logs_2)}")

        print("\nLoading third batch (users - different table)...")
        caplog.clear()
        load_info3 = pipeline.run(generate_users())
        assert not load_info3.has_failed_jobs, "Third load failed"

        # Count catalog creation messages
        new_catalog_logs_3 = [
            record for record in caplog.records
            if "Creating NEW catalog connection" in record.message
        ]
        print(f"   Catalog creations in batch 3: {len(new_catalog_logs_3)}")

        # Count cache reuse messages across all logs
        reuse_logs = [
            record for record in caplog.records
            if "Reusing cached catalog" in record.message
        ]
        print(f"\nTotal catalog reuses: {len(reuse_logs)}")

        # Verify data was loaded correctly
        # Use the cached catalog from the destination
        from sidequery_dlt.destination import _get_or_create_catalog

        verify_config = {
            "type": "sql",
            "uri": f"sqlite:///{catalog_path}",
            "warehouse": f"file://{warehouse_path}",
        }
        catalog = _get_or_create_catalog(verify_config)

        events_table = catalog.load_table("test_ns.events")
        events_data = events_table.scan().to_arrow()
        print(f"\nEvents table: {len(events_data)} rows")
        assert len(events_data) == 20, f"Expected 20 events, got {len(events_data)}"

        users_table = catalog.load_table("test_ns.users")
        users_data = users_table.scan().to_arrow()
        print(f"Users table: {len(users_data)} rows")
        assert len(users_data) == 5, f"Expected 5 users, got {len(users_data)}"

        # Verify connection pooling worked
        # We should see only 1 catalog creation (in the first batch)
        # All subsequent calls should reuse the cached catalog
        total_new_catalogs = len(new_catalog_logs_1) + len(new_catalog_logs_2) + len(new_catalog_logs_3)

        print(f"\n" + "=" * 60)
        print(f"CONNECTION POOLING VERIFICATION:")
        print(f"   Total catalog creations: {total_new_catalogs}")
        print(f"   Expected: 1 (connection pooling working)")
        print(f"=" * 60)

        # With connection pooling, we should only create the catalog once
        assert total_new_catalogs == 1, (
            f"Expected 1 catalog creation (pooling), but got {total_new_catalogs}. "
            "Connection pooling is not working!"
        )

        print("\n✅ SUCCESS! Catalog connections are properly pooled and reused!")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_catalog_pooling_different_configs():
    """
    Test that different catalog configurations create different catalog instances.
    """
    temp_dir = tempfile.mkdtemp()

    try:
        from sidequery_dlt.destination import _get_or_create_catalog, _catalog_cache

        # Clear cache before test
        _catalog_cache.clear()

        # Create two different catalog configs
        config1 = {
            "type": "sql",
            "uri": f"sqlite:///{temp_dir}/catalog1.db",
            "warehouse": f"file://{temp_dir}/warehouse1",
        }

        config2 = {
            "type": "sql",
            "uri": f"sqlite:///{temp_dir}/catalog2.db",
            "warehouse": f"file://{temp_dir}/warehouse2",
        }

        # Get catalogs
        catalog1 = _get_or_create_catalog(config1)
        catalog2 = _get_or_create_catalog(config2)

        # Should be different instances
        assert catalog1 is not catalog2, "Different configs should create different catalogs"

        # Verify cache size
        assert len(_catalog_cache) == 2, f"Expected 2 cached catalogs, got {len(_catalog_cache)}"

        # Get same config again - should reuse
        catalog1_again = _get_or_create_catalog(config1)
        assert catalog1_again is catalog1, "Same config should return cached catalog"

        # Cache size should still be 2
        assert len(_catalog_cache) == 2, f"Cache size should remain 2, got {len(_catalog_cache)}"

        print("✅ Different configs correctly create different catalog instances")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_catalog_config_hash():
    """Test that config hashing is deterministic and handles various value types."""
    from sidequery_dlt.destination import _config_hash

    # Same configs should produce same hash
    config1 = {"type": "rest", "uri": "http://localhost:8181", "warehouse": "s3://bucket"}
    config2 = {"type": "rest", "uri": "http://localhost:8181", "warehouse": "s3://bucket"}
    assert _config_hash(config1) == _config_hash(config2), "Same configs should have same hash"

    # Different order should produce same hash
    config3 = {"warehouse": "s3://bucket", "uri": "http://localhost:8181", "type": "rest"}
    assert _config_hash(config1) == _config_hash(config3), "Config order shouldn't affect hash"

    # Different values should produce different hash
    config4 = {"type": "rest", "uri": "http://localhost:8182", "warehouse": "s3://bucket"}
    assert _config_hash(config1) != _config_hash(config4), "Different URIs should have different hashes"

    # Handle bool values
    config5 = {"type": "rest", "uri": "http://localhost:8181", "sigv4_enabled": True}
    config6 = {"type": "rest", "uri": "http://localhost:8181", "sigv4_enabled": False}
    assert _config_hash(config5) != _config_hash(config6), "Different bool values should differ"

    # Handle None values
    config7 = {"type": "rest", "uri": "http://localhost:8181", "token": None}
    config8 = {"type": "rest", "uri": "http://localhost:8181", "token": "abc123"}
    assert _config_hash(config7) != _config_hash(config8), "None vs value should differ"

    print("✅ Config hashing works correctly")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
