"""Tests for new Iceberg capabilities: hard delete, table_location_layout, etc."""

import pytest
import tempfile
import shutil
import dlt
from datetime import datetime
from pyiceberg.catalog import load_catalog


class TestHardDelete:
    """Tests for hard delete functionality."""

    def test_hard_delete_with_delete_insert(self):
        """Test that rows marked with _dlt_deleted_at are deleted during merge."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_hard_delete",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_hard_delete",
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
            table = catalog.load_table("test_hard_delete.users")
            result = table.scan().to_arrow()
            assert len(result) == 3

            # Now send updates with hard delete marker
            @dlt.resource(
                name="users",
                write_disposition={"disposition": "merge", "strategy": "delete-insert"},
                primary_key="user_id",
            )
            def updated_users():
                return [
                    # Update user 1
                    {"user_id": 1, "name": "alice_updated", "score": 150, "_dlt_deleted_at": None},
                    # Hard delete user 2 (set _dlt_deleted_at to a timestamp)
                    {"user_id": 2, "name": "bob", "score": 200, "_dlt_deleted_at": datetime.now()},
                    # Add new user 4
                    {"user_id": 4, "name": "diana", "score": 400, "_dlt_deleted_at": None},
                ]

            load_info = pipeline.run(updated_users())
            assert not load_info.has_failed_jobs

            # Verify: user 2 should be deleted, user 1 updated, user 4 added
            table = catalog.load_table("test_hard_delete.users")
            result = table.scan().to_arrow()
            df = result.to_pandas().sort_values("user_id")

            # Should have 3 rows (1, 3, 4) - user 2 was hard deleted
            assert len(df) == 3
            assert list(df["user_id"]) == [1, 3, 4]

            # User 1 should be updated
            user1 = df[df["user_id"] == 1].iloc[0]
            assert user1["name"] == "alice_updated"
            assert user1["score"] == 150

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_hard_delete_with_upsert(self):
        """Test hard delete with upsert strategy."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_hard_delete_upsert",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_hard_delete_upsert",
                ),
                dataset_name="test_dataset",
            )

            # Initial load
            @dlt.resource(
                name="users",
                write_disposition={"disposition": "merge", "strategy": "upsert"},
                primary_key="user_id",
            )
            def initial_users():
                return [
                    {"user_id": 1, "name": "alice", "score": 100},
                    {"user_id": 2, "name": "bob", "score": 200},
                ]

            load_info = pipeline.run(initial_users())
            assert not load_info.has_failed_jobs

            # Now hard delete user 1
            @dlt.resource(
                name="users",
                write_disposition={"disposition": "merge", "strategy": "upsert"},
                primary_key="user_id",
            )
            def delete_user():
                return [
                    {"user_id": 1, "name": "alice", "score": 100, "_dlt_deleted_at": datetime.now()},
                ]

            load_info = pipeline.run(delete_user())
            assert not load_info.has_failed_jobs

            # Verify: user 1 should be deleted
            catalog = load_catalog(
                "dlt_catalog",
                type="sql",
                uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
            )
            table = catalog.load_table("test_hard_delete_upsert.users")
            result = table.scan().to_arrow()
            df = result.to_pandas()

            assert len(df) == 1
            assert df.iloc[0]["user_id"] == 2

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_hard_delete_all_rows(self):
        """Test when all incoming rows are marked for hard delete."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_hard_delete_all",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_hard_delete_all",
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
                ]

            load_info = pipeline.run(initial_users())
            assert not load_info.has_failed_jobs

            # Hard delete both users
            @dlt.resource(
                name="users",
                write_disposition={"disposition": "merge", "strategy": "delete-insert"},
                primary_key="user_id",
            )
            def delete_all():
                return [
                    {"user_id": 1, "name": "alice", "score": 100, "_dlt_deleted_at": datetime.now()},
                    {"user_id": 2, "name": "bob", "score": 200, "_dlt_deleted_at": datetime.now()},
                ]

            load_info = pipeline.run(delete_all())
            assert not load_info.has_failed_jobs

            # Verify: all rows should be deleted
            catalog = load_catalog(
                "dlt_catalog",
                type="sql",
                uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
            )
            table = catalog.load_table("test_hard_delete_all.users")
            result = table.scan().to_arrow()

            assert len(result) == 0

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestTableLocationLayout:
    """Tests for table_location_layout capability."""

    def test_custom_table_location(self):
        """Test that custom table location is used when creating tables."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest
            import os

            pipeline = dlt.pipeline(
                pipeline_name="test_location",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_location",
                    table_location_layout="custom_path/{namespace}/{table_name}",
                ),
                dataset_name="test_dataset",
            )

            @dlt.resource(name="events")
            def events():
                yield {"id": 1, "data": "test"}

            load_info = pipeline.run(events())
            assert not load_info.has_failed_jobs

            # Verify table was created at custom location
            catalog = load_catalog(
                "dlt_catalog",
                type="sql",
                uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
            )
            table = catalog.load_table("test_location.events")
            location = table.location()

            # Location should contain our custom path
            assert "custom_path" in location
            assert "test_location" in location
            assert "events" in location

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestCustomPartitionNames:
    """Tests for custom partition field names."""

    def test_custom_partition_name_e2e(self):
        """Test that custom partition names are used in table creation."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest, iceberg_adapter, iceberg_partition

            pipeline = dlt.pipeline(
                pipeline_name="test_custom_partition_name",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_partition_name",
                ),
                dataset_name="test_dataset",
            )

            @dlt.resource(name="events")
            def events():
                yield {"id": 1, "created_at": datetime(2024, 1, 15), "region": "US"}

            # Use custom partition names
            adapted = iceberg_adapter(
                events,
                partition=[
                    iceberg_partition.month("created_at", "event_month"),
                    iceberg_partition.identity("region", "event_region"),
                ],
            )

            load_info = pipeline.run(adapted)
            assert not load_info.has_failed_jobs

            # Verify partition field names
            catalog = load_catalog(
                "dlt_catalog",
                type="sql",
                uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
            )
            table = catalog.load_table("test_partition_name.events")
            partition_spec = table.spec()

            # Check custom names were used
            field_names = [f.name for f in partition_spec.fields]
            assert "event_month" in field_names
            assert "event_region" in field_names

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
