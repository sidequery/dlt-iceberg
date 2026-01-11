"""Tests for pipeline.dataset() DuckDB integration."""

import pytest
import tempfile
import shutil
import dlt
from datetime import datetime
from pyiceberg.catalog import load_catalog


class TestDatasetIntegration:
    """Tests for pipeline.dataset() with Iceberg tables."""

    def test_dataset_query_basic(self):
        """Test basic dataset query functionality."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_dataset_basic",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_dataset",
                ),
                dataset_name="test_dataset",
            )

            @dlt.resource(name="users")
            def users():
                yield {"id": 1, "name": "alice", "age": 30}
                yield {"id": 2, "name": "bob", "age": 25}
                yield {"id": 3, "name": "charlie", "age": 35}

            load_info = pipeline.run(users())
            assert not load_info.has_failed_jobs

            # Use dataset() to query the data
            dataset = pipeline.dataset()

            # Query all rows
            result = dataset["users"].fetchall()
            assert len(result) == 3

            # Check data is correct
            names = {row[1] for row in result}  # name is second column
            assert names == {"alice", "bob", "charlie"}

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_dataset_df(self):
        """Test dataset returns proper dataframe."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_dataset_df",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_dataset_df",
                ),
                dataset_name="test_dataset_df",
            )

            @dlt.resource(name="events")
            def events():
                yield {"event_id": 1, "event_type": "click", "timestamp": datetime(2024, 1, 15)}
                yield {"event_id": 2, "event_type": "view", "timestamp": datetime(2024, 1, 16)}

            load_info = pipeline.run(events())
            assert not load_info.has_failed_jobs

            # Get as dataframe
            dataset = pipeline.dataset()
            df = dataset["events"].df()

            assert len(df) == 2
            assert "event_id" in df.columns
            assert "event_type" in df.columns

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_dataset_arrow(self):
        """Test dataset returns proper Arrow table."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest
            import pyarrow as pa

            pipeline = dlt.pipeline(
                pipeline_name="test_dataset_arrow",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_dataset_arrow",
                ),
                dataset_name="test_dataset_arrow",
            )

            @dlt.resource(name="metrics")
            def metrics():
                yield {"metric_id": 1, "value": 100.5}
                yield {"metric_id": 2, "value": 200.7}

            load_info = pipeline.run(metrics())
            assert not load_info.has_failed_jobs

            # Get as Arrow table
            dataset = pipeline.dataset()
            arrow_table = dataset["metrics"].arrow()

            assert isinstance(arrow_table, pa.Table)
            assert len(arrow_table) == 2
            assert "metric_id" in arrow_table.column_names

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_dataset_sql_query(self):
        """Test running SQL queries via dataset."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_dataset_sql",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_dataset_sql",
                ),
                dataset_name="test_dataset_sql",
            )

            @dlt.resource(name="products")
            def products():
                yield {"product_id": 1, "name": "Widget", "price": 9.99}
                yield {"product_id": 2, "name": "Gadget", "price": 19.99}
                yield {"product_id": 3, "name": "Gizmo", "price": 29.99}

            load_info = pipeline.run(products())
            assert not load_info.has_failed_jobs

            # Run SQL query
            dataset = pipeline.dataset()
            result = dataset.query("SELECT name, price FROM products WHERE price > 15").fetchall()

            assert len(result) == 2
            names = {row[0] for row in result}
            assert names == {"Gadget", "Gizmo"}

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_dataset_multiple_tables(self):
        """Test dataset with multiple tables."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_dataset_multi",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_dataset_multi",
                ),
                dataset_name="test_dataset_multi",
            )

            @dlt.resource(name="customers")
            def customers():
                yield {"customer_id": 1, "name": "Alice"}
                yield {"customer_id": 2, "name": "Bob"}

            @dlt.resource(name="orders")
            def orders():
                yield {"order_id": 101, "customer_id": 1, "amount": 50.0}
                yield {"order_id": 102, "customer_id": 2, "amount": 75.0}
                yield {"order_id": 103, "customer_id": 1, "amount": 25.0}

            load_info = pipeline.run([customers(), orders()])
            assert not load_info.has_failed_jobs

            # Query both tables
            dataset = pipeline.dataset()

            customers_result = dataset["customers"].fetchall()
            assert len(customers_result) == 2

            orders_result = dataset["orders"].fetchall()
            assert len(orders_result) == 3

            # Join query
            join_result = dataset.query("""
                SELECT c.name, SUM(o.amount) as total
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.name
                ORDER BY c.name
            """).fetchall()

            assert len(join_result) == 2
            # Alice has orders 50 + 25 = 75
            assert join_result[0] == ("Alice", 75.0)
            # Bob has order 75
            assert join_result[1] == ("Bob", 75.0)

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestOpenTableInterface:
    """Tests for SupportsOpenTables interface."""

    def test_get_open_table_catalog(self):
        """Test getting the PyIceberg catalog."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest
            from pyiceberg.catalog import Catalog

            pipeline = dlt.pipeline(
                pipeline_name="test_catalog",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_catalog",
                ),
                dataset_name="test_catalog",
            )

            # Load some data first to ensure catalog is set up
            @dlt.resource(name="test")
            def test_data():
                yield {"id": 1}

            pipeline.run(test_data())

            # Get the client
            with pipeline.destination_client() as client:
                catalog = client.get_open_table_catalog("iceberg")
                assert catalog is not None
                # Should be a PyIceberg Catalog
                assert isinstance(catalog, Catalog)

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_load_open_table(self):
        """Test loading a PyIceberg Table."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest
            from pyiceberg.table import Table

            pipeline = dlt.pipeline(
                pipeline_name="test_load_table",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_load_table",
                ),
                dataset_name="test_load_table",
            )

            @dlt.resource(name="data")
            def data():
                yield {"id": 1, "value": "test"}

            pipeline.run(data())

            # Get the client and load table
            with pipeline.destination_client() as client:
                table = client.load_open_table("iceberg", "data")
                assert table is not None
                assert isinstance(table, Table)

                # Can scan the table
                result = table.scan().to_arrow()
                assert len(result) == 1

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_get_open_table_location(self):
        """Test getting table location."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_location",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_location",
                ),
                dataset_name="test_location",
            )

            @dlt.resource(name="located")
            def located():
                yield {"id": 1}

            pipeline.run(located())

            # Get the client and check location
            with pipeline.destination_client() as client:
                location = client.get_open_table_location("iceberg", "located")
                assert location is not None
                assert "test_location" in location
                assert "located" in location

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_is_open_table(self):
        """Test is_open_table check."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        try:
            from dlt_iceberg import iceberg_rest

            pipeline = dlt.pipeline(
                pipeline_name="test_is_open",
                destination=iceberg_rest(
                    catalog_uri=f"sqlite:///{catalog_path}",
                    warehouse=f"file://{warehouse_path}",
                    namespace="test_is_open",
                ),
                dataset_name="test_is_open",
            )

            @dlt.resource(name="test")
            def test():
                yield {"id": 1}

            pipeline.run(test())

            with pipeline.destination_client() as client:
                # All tables are Iceberg in this destination
                assert client.is_open_table("iceberg", "test") is True
                assert client.is_open_table("delta", "test") is False

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
