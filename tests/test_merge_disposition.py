"""
Test merge disposition (upsert) functionality with the iceberg_rest destination.

This test proves that:
1. Initial data with keys [1,2,3] is inserted
2. Merge with keys [2,3,4] updates existing rows and inserts new ones
3. Final data has [1,2,3,4] with correct values (2 and 3 are updated, not duplicated)
"""

import pytest
import tempfile
import shutil
from datetime import datetime, timedelta
import dlt
from pyiceberg.catalog import load_catalog


def test_merge_upsert():
    """
    Test that merge disposition properly upserts data:
    - Updates existing rows based on primary key
    - Inserts new rows that don't match existing keys
    - No duplicates in final data
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    print(f"\nTest environment:")
    print(f"   Warehouse: {warehouse_path}")
    print(f"   Catalog: {catalog_path}")

    try:
        # Import destination
        from dlt_iceberg.destination import iceberg_rest

        # Create dlt pipeline with our destination
        pipeline = dlt.pipeline(
            pipeline_name="test_merge",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="test_merge",
            ),
            dataset_name="test_dataset",
        )

        print(f"\nStep 1: Insert initial data with keys [1,2,3]")

        @dlt.resource(
            name="users",
            write_disposition="merge",
            primary_key="user_id",
        )
        def initial_users():
            base_time = datetime(2024, 1, 1)
            return [
                {
                    "user_id": 1,
                    "username": "alice",
                    "email": "alice@example.com",
                    "created_at": base_time,
                    "score": 100,
                },
                {
                    "user_id": 2,
                    "username": "bob",
                    "email": "bob@example.com",
                    "created_at": base_time + timedelta(hours=1),
                    "score": 200,
                },
                {
                    "user_id": 3,
                    "username": "charlie",
                    "email": "charlie@example.com",
                    "created_at": base_time + timedelta(hours=2),
                    "score": 300,
                },
            ]

        # Load initial data
        load_info = pipeline.run(initial_users())
        print(f"   Load completed: {load_info.has_failed_jobs}")
        if load_info.has_failed_jobs:
            print(f"   Failed jobs: {load_info.failed_jobs}")
            raise AssertionError("Initial load failed!")

        # Verify initial data
        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )

        table = catalog.load_table("test_merge.users")
        result = table.scan().to_arrow()
        df = result.to_pandas().sort_values("user_id")

        print(f"\n   Initial data (3 rows):")
        print(df)

        assert len(df) == 3
        assert list(df["user_id"]) == [1, 2, 3]
        assert list(df["username"]) == ["alice", "bob", "charlie"]
        assert list(df["score"]) == [100, 200, 300]

        print(f"\nStep 2: Merge with keys [2,3,4] (update 2,3 and insert 4)")

        @dlt.resource(
            name="users",
            write_disposition="merge",
            primary_key="user_id",
        )
        def updated_users():
            base_time = datetime(2024, 1, 1)
            return [
                {
                    "user_id": 2,
                    "username": "bob_updated",
                    "email": "bob_new@example.com",
                    "created_at": base_time + timedelta(hours=1),
                    "score": 250,  # Updated score
                },
                {
                    "user_id": 3,
                    "username": "charlie_updated",
                    "email": "charlie_new@example.com",
                    "created_at": base_time + timedelta(hours=2),
                    "score": 350,  # Updated score
                },
                {
                    "user_id": 4,
                    "username": "diana",
                    "email": "diana@example.com",
                    "created_at": base_time + timedelta(hours=3),
                    "score": 400,
                },
            ]

        # Load merge data
        load_info2 = pipeline.run(updated_users())
        print(f"   Merge completed: {load_info2.has_failed_jobs}")
        if load_info2.has_failed_jobs:
            print(f"   Failed jobs: {load_info2.failed_jobs}")
            raise AssertionError("Merge load failed!")

        # Verify merged data
        table = catalog.load_table("test_merge.users")
        result2 = table.scan().to_arrow()
        df2 = result2.to_pandas().sort_values("user_id")

        print(f"\n   Final data after merge (4 rows, no duplicates):")
        print(df2)

        # Verify: Should have 4 rows (not 6!)
        assert len(df2) == 4, f"Expected 4 rows, got {len(df2)}"

        # Verify all user_ids are present
        assert list(df2["user_id"]) == [1, 2, 3, 4]

        # Verify user 1 is unchanged
        user1 = df2[df2["user_id"] == 1].iloc[0]
        assert user1["username"] == "alice"
        assert user1["score"] == 100

        # Verify user 2 is updated
        user2 = df2[df2["user_id"] == 2].iloc[0]
        assert user2["username"] == "bob_updated"
        assert user2["score"] == 250

        # Verify user 3 is updated
        user3 = df2[df2["user_id"] == 3].iloc[0]
        assert user3["username"] == "charlie_updated"
        assert user3["score"] == 350

        # Verify user 4 is inserted
        user4 = df2[df2["user_id"] == 4].iloc[0]
        assert user4["username"] == "diana"
        assert user4["score"] == 400

        print(f"\nMerge disposition works correctly")
        print(f"   - User 1 unchanged (not in merge)")
        print(f"   - User 2 updated (score 200 -> 250)")
        print(f"   - User 3 updated (score 300 -> 350)")
        print(f"   - User 4 inserted (new)")
        print(f"   - No duplicates")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_merge_without_primary_key_falls_back_to_append():
    """
    Test that merge without primary_key falls back to append.
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"
    catalog_path = f"{temp_dir}/catalog.db"

    try:
        from dlt_iceberg.destination import iceberg_rest

        pipeline = dlt.pipeline(
            pipeline_name="test_merge_fallback",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
                namespace="test_fallback",
            ),
            dataset_name="test_dataset",
        )

        @dlt.resource(
            name="events",
            write_disposition="merge",
            # NO primary_key specified - should fall back to append
        )
        def events():
            return [
                {"event_id": 1, "value": 100},
                {"event_id": 2, "value": 200},
            ]

        # Load data
        load_info = pipeline.run(events())
        assert not load_info.has_failed_jobs

        # Load again with merge (should append since no primary key)
        load_info2 = pipeline.run(events())
        assert not load_info2.has_failed_jobs

        # Verify data - should have 4 rows (2 + 2 appended)
        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )

        table = catalog.load_table("test_fallback.events")
        result = table.scan().to_arrow()

        # Should have 4 rows (appended, not merged)
        assert len(result) == 4

        print(f"\nMerge without primary_key correctly falls back to append")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
