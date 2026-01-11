"""
Tests for delete-insert merge strategy.
"""

import pytest
import tempfile
import shutil
import dlt
from pyiceberg.catalog import load_catalog


class TestDeleteInsertMerge:
    """Tests for delete-insert merge strategy."""

    def test_delete_insert_basic(self):
        """Test delete-insert strategy correctly replaces matching rows."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_delete_insert",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_delete_insert",
                ),
                dataset_name="test_dataset",
            )

            # Initial load
            @dlt.resource(
                name="users",
                write_disposition={"disposition": "merge", "strategy": "delete-insert"},
                primary_key="user_id",
            )
            def initial_users():
                return [
                    {"user_id": 1, "name": "alice", "score": 100},
                    {"user_id": 2, "name": "bob", "score": 200},
                    {"user_id": 3, "name": "charlie", "score": 300},
                ]

            load_info = pipeline.run(initial_users())
            assert not load_info.has_failed_jobs

            # Verify initial data
            catalog = load_catalog(
                "dlt_catalog",
                type="sql",
                uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
            )
            table = catalog.load_table("test_delete_insert.users")
            result = table.scan().to_arrow()
            assert len(result) == 3

            # Update with delete-insert (user 2 updated, user 4 new)
            @dlt.resource(
                name="users",
                write_disposition={"disposition": "merge", "strategy": "delete-insert"},
                primary_key="user_id",
            )
            def updated_users():
                return [
                    {"user_id": 2, "name": "bob_updated", "score": 250},
                    {"user_id": 4, "name": "diana", "score": 400},
                ]

            load_info = pipeline.run(updated_users())
            assert not load_info.has_failed_jobs

            # Verify: should have 4 rows (1, 2_updated, 3, 4)
            table = catalog.load_table("test_delete_insert.users")
            result = table.scan().to_arrow()
            df = result.to_pandas().sort_values("user_id")

            assert len(df) == 4
            assert list(df["user_id"]) == [1, 2, 3, 4]

            # User 2 should be replaced (not duplicated)
            user2 = df[df["user_id"] == 2].iloc[0]
            assert user2["name"] == "bob_updated"
            assert user2["score"] == 250

            # User 4 should be new
            user4 = df[df["user_id"] == 4].iloc[0]
            assert user4["name"] == "diana"

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_delete_insert_composite_key(self):
        """Test delete-insert with composite primary key."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_composite_key",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_composite",
                ),
                dataset_name="test_dataset",
            )

            # Initial load
            @dlt.resource(
                name="events",
                write_disposition={"disposition": "merge", "strategy": "delete-insert"},
                primary_key=["user_id", "event_date"],
            )
            def initial_events():
                return [
                    {"user_id": 1, "event_date": "2024-01-01", "count": 10},
                    {"user_id": 1, "event_date": "2024-01-02", "count": 20},
                    {"user_id": 2, "event_date": "2024-01-01", "count": 30},
                ]

            load_info = pipeline.run(initial_events())
            assert not load_info.has_failed_jobs

            # Update specific composite key
            @dlt.resource(
                name="events",
                write_disposition={"disposition": "merge", "strategy": "delete-insert"},
                primary_key=["user_id", "event_date"],
            )
            def updated_events():
                return [
                    {"user_id": 1, "event_date": "2024-01-01", "count": 15},  # Update
                    {"user_id": 3, "event_date": "2024-01-01", "count": 40},  # New
                ]

            load_info = pipeline.run(updated_events())
            assert not load_info.has_failed_jobs

            catalog = load_catalog(
                "dlt_catalog",
                type="sql",
                uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
            )
            table = catalog.load_table("test_composite.events")
            result = table.scan().to_arrow()
            df = result.to_pandas()

            # Should have 4 rows
            assert len(df) == 4

            # Check updated row
            updated_row = df[(df["user_id"] == 1) & (df["event_date"] == "2024-01-01")]
            assert len(updated_row) == 1
            assert updated_row.iloc[0]["count"] == 15

            # Check new row
            new_row = df[(df["user_id"] == 3) & (df["event_date"] == "2024-01-01")]
            assert len(new_row) == 1
            assert new_row.iloc[0]["count"] == 40

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_upsert_backward_compatibility(self):
        """Test that string 'merge' disposition still uses upsert."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_backward_compat",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_compat",
                ),
                dataset_name="test_dataset",
            )

            # Initial load with string write_disposition
            @dlt.resource(
                name="users",
                write_disposition="merge",
                primary_key="user_id",
            )
            def initial_users():
                return [
                    {"user_id": 1, "name": "alice", "score": 100},
                    {"user_id": 2, "name": "bob", "score": 200},
                ]

            load_info = pipeline.run(initial_users())
            assert not load_info.has_failed_jobs

            # Update (still using string "merge" - should use upsert)
            @dlt.resource(
                name="users",
                write_disposition="merge",
                primary_key="user_id",
            )
            def updated_users():
                return [
                    {"user_id": 2, "name": "bob_updated", "score": 250},
                    {"user_id": 3, "name": "charlie", "score": 300},
                ]

            load_info = pipeline.run(updated_users())
            assert not load_info.has_failed_jobs

            # Verify
            catalog = load_catalog(
                "dlt_catalog",
                type="sql",
                uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
            )
            table = catalog.load_table("test_compat.users")
            result = table.scan().to_arrow()
            df = result.to_pandas().sort_values("user_id")

            # Should have 3 rows
            assert len(df) == 3
            assert list(df["user_id"]) == [1, 2, 3]

            # User 2 should be updated via upsert
            user2 = df[df["user_id"] == 2].iloc[0]
            assert user2["name"] == "bob_updated"

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_explicit_upsert_strategy(self):
        """Test explicit upsert strategy with dict write_disposition."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_explicit_upsert",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_explicit",
                ),
                dataset_name="test_dataset",
            )

            # Load with explicit upsert strategy
            @dlt.resource(
                name="users",
                write_disposition={"disposition": "merge", "strategy": "upsert"},
                primary_key="user_id",
            )
            def users():
                return [
                    {"user_id": 1, "name": "alice", "score": 100},
                ]

            load_info = pipeline.run(users())
            assert not load_info.has_failed_jobs

            # Update with upsert
            @dlt.resource(
                name="users",
                write_disposition={"disposition": "merge", "strategy": "upsert"},
                primary_key="user_id",
            )
            def more_users():
                return [
                    {"user_id": 1, "name": "alice_updated", "score": 150},
                    {"user_id": 2, "name": "bob", "score": 200},
                ]

            load_info = pipeline.run(more_users())
            assert not load_info.has_failed_jobs

            catalog = load_catalog(
                "dlt_catalog",
                type="sql",
                uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
            )
            table = catalog.load_table("test_explicit.users")
            df = table.scan().to_arrow().to_pandas().sort_values("user_id")

            assert len(df) == 2
            assert df[df["user_id"] == 1].iloc[0]["name"] == "alice_updated"

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
