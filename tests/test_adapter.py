"""Tests for iceberg_adapter function."""

import pytest
import tempfile
import shutil
import dlt
from pyiceberg.catalog import load_catalog

from dlt_iceberg import iceberg_adapter, iceberg_partition, PartitionTransform


class TestPartitionTransform:
    """Tests for partition transform factory methods."""

    def test_identity_transform(self):
        pt = iceberg_partition.identity("region")
        assert pt.column == "region"
        assert pt.transform == "identity"
        assert pt.param is None
        assert pt.to_hint_value() == "identity"

    def test_year_transform(self):
        pt = iceberg_partition.year("created_at")
        assert pt.column == "created_at"
        assert pt.transform == "year"
        assert pt.to_hint_value() == "year"

    def test_month_transform(self):
        pt = iceberg_partition.month("event_date")
        assert pt.column == "event_date"
        assert pt.transform == "month"
        assert pt.to_hint_value() == "month"

    def test_day_transform(self):
        pt = iceberg_partition.day("ts")
        assert pt.column == "ts"
        assert pt.transform == "day"
        assert pt.to_hint_value() == "day"

    def test_hour_transform(self):
        pt = iceberg_partition.hour("timestamp")
        assert pt.column == "timestamp"
        assert pt.transform == "hour"
        assert pt.to_hint_value() == "hour"

    def test_bucket_transform(self):
        pt = iceberg_partition.bucket(10, "user_id")
        assert pt.column == "user_id"
        assert pt.transform == "bucket"
        assert pt.param == 10
        assert pt.to_hint_value() == "bucket[10]"

    def test_bucket_invalid_count(self):
        with pytest.raises(ValueError, match="must be positive"):
            iceberg_partition.bucket(0, "user_id")
        with pytest.raises(ValueError, match="must be positive"):
            iceberg_partition.bucket(-5, "user_id")

    def test_truncate_transform(self):
        pt = iceberg_partition.truncate(4, "email")
        assert pt.column == "email"
        assert pt.transform == "truncate"
        assert pt.param == 4
        assert pt.to_hint_value() == "truncate[4]"

    def test_truncate_invalid_width(self):
        with pytest.raises(ValueError, match="must be positive"):
            iceberg_partition.truncate(0, "email")

    def test_custom_partition_name(self):
        pt = iceberg_partition.year("created_at", "year_created")
        assert pt.column == "created_at"
        assert pt.transform == "year"
        assert pt.name == "year_created"

    def test_bucket_with_custom_name(self):
        pt = iceberg_partition.bucket(8, "user_id", "user_bucket")
        assert pt.column == "user_id"
        assert pt.param == 8
        assert pt.name == "user_bucket"


class TestIcebergAdapter:
    """Tests for iceberg_adapter function."""

    def test_adapter_with_dlt_resource(self):
        @dlt.resource(name="events")
        def events():
            yield {"id": 1, "created_at": "2024-01-01"}

        adapted = iceberg_adapter(events, partition=iceberg_partition.month("created_at"))

        assert adapted is not None
        # Check hints were applied
        hints = adapted._hints
        columns = hints.get("columns", {})
        assert "created_at" in columns
        assert columns["created_at"].get("x-partition") is True
        assert columns["created_at"].get("x-partition-transform") == "month"

    def test_adapter_with_multiple_partitions(self):
        @dlt.resource(name="sales")
        def sales():
            yield {"sale_date": "2024-01-01", "region": "US", "user_id": 123}

        adapted = iceberg_adapter(
            sales,
            partition=[
                iceberg_partition.day("sale_date"),
                iceberg_partition.identity("region"),
                iceberg_partition.bucket(5, "user_id"),
            ],
        )

        columns = adapted._hints.get("columns", {})

        assert columns["sale_date"]["x-partition"] is True
        assert columns["sale_date"]["x-partition-transform"] == "day"

        assert columns["region"]["x-partition"] is True
        # identity doesn't set transform (it's handled as default)

        assert columns["user_id"]["x-partition"] is True
        assert columns["user_id"]["x-partition-transform"] == "bucket[5]"

    def test_adapter_with_raw_data(self):
        data = [{"id": 1, "ts": "2024-01-01"}]
        adapted = iceberg_adapter(data, partition=iceberg_partition.month("ts"))

        # Should be converted to DltResource
        from dlt.extract.resource import DltResource

        assert isinstance(adapted, DltResource)

    def test_adapter_no_partition(self):
        @dlt.resource(name="events")
        def events():
            yield {"id": 1}

        adapted = iceberg_adapter(events)
        # Should return same resource when no partition specified
        assert adapted.name == "events"

    def test_adapter_single_partition(self):
        """Test adapter with single PartitionTransform (not list)."""

        @dlt.resource(name="logs")
        def logs():
            yield {"ts": "2024-01-01", "msg": "test"}

        adapted = iceberg_adapter(logs, partition=iceberg_partition.day("ts"))

        columns = adapted._hints.get("columns", {})
        assert columns["ts"]["x-partition"] is True
        assert columns["ts"]["x-partition-transform"] == "day"

    def test_adapter_source_multiple_resources_error(self):
        @dlt.source
        def my_source():
            @dlt.resource(name="r1")
            def r1():
                yield {"id": 1}

            @dlt.resource(name="r2")
            def r2():
                yield {"id": 2}

            return r1, r2

        source = my_source()
        with pytest.raises(ValueError, match="multiple resources"):
            iceberg_adapter(source, partition=iceberg_partition.month("ts"))


class TestIcebergAdapterE2E:
    """End-to-end tests for adapter with actual Iceberg tables."""

    def test_adapter_creates_partitioned_table(self):
        """Test that adapter hints result in partitioned table."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_adapter_e2e",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_adapter",
                ),
                dataset_name="test",
            )

            @dlt.resource(name="events")
            def events():
                from datetime import datetime

                for i in range(10):
                    yield {
                        "event_id": i,
                        "event_date": datetime(2024, 1, i % 3 + 1),
                        "user_id": i % 5,
                    }

            adapted = iceberg_adapter(
                events,
                partition=[
                    iceberg_partition.day("event_date"),
                    iceberg_partition.bucket(3, "user_id"),
                ],
            )

            load_info = pipeline.run(adapted)
            assert not load_info.has_failed_jobs

            # Verify partition spec
            catalog = load_catalog(
                "dlt_catalog",
                type="sql",
                uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
            )

            table = catalog.load_table("test_adapter.events")
            partition_spec = table.spec()

            assert len(partition_spec.fields) == 2

            # Check transforms
            transforms = [str(f.transform).lower() for f in partition_spec.fields]
            assert any("day" in t for t in transforms)
            assert any("bucket" in t for t in transforms)

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_adapter_with_month_partition(self):
        """Test adapter with month partition transform."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_month_partition",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_month",
                ),
                dataset_name="test",
            )

            @dlt.resource(name="sales")
            def sales():
                from datetime import datetime

                yield {"sale_id": 1, "sale_date": datetime(2024, 1, 15), "amount": 100}
                yield {"sale_id": 2, "sale_date": datetime(2024, 2, 20), "amount": 200}

            adapted = iceberg_adapter(sales, partition=iceberg_partition.month("sale_date"))

            load_info = pipeline.run(adapted)
            assert not load_info.has_failed_jobs

            catalog = load_catalog(
                "dlt_catalog",
                type="sql",
                uri=f"sqlite:///{catalog_path}",
                warehouse=f"file://{warehouse_path}",
            )

            table = catalog.load_table("test_month.sales")
            partition_spec = table.spec()

            assert len(partition_spec.fields) == 1
            assert "month" in str(partition_spec.fields[0].transform).lower()

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
