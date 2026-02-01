"""
Unit tests for WithStateSync implementation.

Tests the schema and state storage/retrieval methods directly.
"""

import pytest
import tempfile
import shutil
import json
from datetime import datetime

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType

import dlt
from dlt_iceberg import iceberg_rest


class TestWithStateSyncMethods:
    """Unit tests for WithStateSync interface methods."""

    @pytest.fixture
    def catalog_setup(self):
        """Create a temporary catalog for testing."""
        temp_dir = tempfile.mkdtemp()
        warehouse_path = f"{temp_dir}/warehouse"
        catalog_path = f"{temp_dir}/catalog.db"

        # Use "dlt_catalog" name to match what IcebergRestClient uses internally
        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{catalog_path}",
            warehouse=f"file://{warehouse_path}",
        )

        # Create namespace
        catalog.create_namespace("test")

        yield {
            "temp_dir": temp_dir,
            "warehouse_path": warehouse_path,
            "catalog_path": catalog_path,
            "catalog": catalog,
        }

        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def client(self, catalog_setup):
        """Create an IcebergRestClient for testing."""
        from dlt_iceberg.destination_client import (
            IcebergRestClient,
            IcebergRestConfiguration,
            iceberg_rest_class_based,
        )
        from dlt.common.schema import Schema

        config = IcebergRestConfiguration(
            catalog_uri=f"sqlite:///{catalog_setup['catalog_path']}",
            warehouse=f"file://{catalog_setup['warehouse_path']}",
            namespace="test",
        )

        schema = Schema("test_schema")
        # Get capabilities from the destination class
        dest = iceberg_rest_class_based()
        capabilities = dest._raw_capabilities()

        client = IcebergRestClient(schema, config, capabilities)

        return client

    def test_get_stored_schema_returns_none_when_table_missing(self, client):
        """Test that get_stored_schema returns None when _dlt_version doesn't exist."""
        print("\nTest: get_stored_schema returns None when table missing")

        result = client.get_stored_schema()
        assert result is None, "Should return None when _dlt_version table doesn't exist"
        print("   Correctly returned None")

    def test_get_stored_schema_by_hash_returns_none_when_table_missing(self, client):
        """Test that get_stored_schema_by_hash returns None when _dlt_version doesn't exist."""
        print("\nTest: get_stored_schema_by_hash returns None when table missing")

        result = client.get_stored_schema_by_hash("nonexistent_hash")
        assert result is None, "Should return None when _dlt_version table doesn't exist"
        print("   Correctly returned None")

    def test_get_stored_state_returns_none_when_table_missing(self, client):
        """Test that get_stored_state returns None when _dlt_pipeline_state doesn't exist."""
        print("\nTest: get_stored_state returns None when table missing")

        result = client.get_stored_state("nonexistent_pipeline")
        assert result is None, "Should return None when _dlt_pipeline_state table doesn't exist"
        print("   Correctly returned None")

    def test_get_stored_schema_retrieves_written_schema(self, catalog_setup, client):
        """Test that get_stored_schema retrieves a schema that was written."""
        print("\nTest: get_stored_schema retrieves written schema")

        catalog = catalog_setup["catalog"]

        # Create _dlt_version table with test data
        iceberg_schema = IcebergSchema(
            NestedField(1, "version_hash", StringType(), required=True),
            NestedField(2, "schema_name", StringType(), required=True),
            NestedField(3, "version", LongType(), required=True),
            NestedField(4, "engine_version", LongType(), required=True),
            NestedField(5, "inserted_at", TimestampType(), required=True),
            NestedField(6, "schema", StringType(), required=True),
        )

        version_table = catalog.create_table(
            identifier="test._dlt_version",
            schema=iceberg_schema,
        )

        # Insert test schema
        test_schema_dict = {"name": "test_schema", "tables": {}}
        arrow_schema = pa.schema([
            pa.field("version_hash", pa.string(), nullable=False),
            pa.field("schema_name", pa.string(), nullable=False),
            pa.field("version", pa.int64(), nullable=False),
            pa.field("engine_version", pa.int64(), nullable=False),
            pa.field("inserted_at", pa.timestamp("us"), nullable=False),
            pa.field("schema", pa.string(), nullable=False),
        ])
        arrow_table = pa.table({
            "version_hash": ["hash123"],
            "schema_name": ["test_schema"],
            "version": [1],
            "engine_version": [1],
            "inserted_at": [datetime.now()],
            "schema": [json.dumps(test_schema_dict)],
        }, schema=arrow_schema)

        version_table.append(arrow_table)
        print("   Created _dlt_version table with test data")

        # Retrieve schema
        result = client.get_stored_schema("test_schema")
        assert result is not None, "Should retrieve stored schema"
        assert result.version_hash == "hash123", "Should have correct version_hash"
        assert result.schema_name == "test_schema", "Should have correct schema_name"
        assert result.version == 1, "Should have correct version"
        print(f"   Retrieved schema: {result.schema_name} v{result.version}")

    def test_get_stored_schema_returns_newest_version(self, catalog_setup, client):
        """Test that get_stored_schema returns the newest version when multiple exist."""
        print("\nTest: get_stored_schema returns newest version")

        catalog = catalog_setup["catalog"]

        # Create _dlt_version table
        iceberg_schema = IcebergSchema(
            NestedField(1, "version_hash", StringType(), required=True),
            NestedField(2, "schema_name", StringType(), required=True),
            NestedField(3, "version", LongType(), required=True),
            NestedField(4, "engine_version", LongType(), required=True),
            NestedField(5, "inserted_at", TimestampType(), required=True),
            NestedField(6, "schema", StringType(), required=True),
        )

        version_table = catalog.create_table(
            identifier="test._dlt_version",
            schema=iceberg_schema,
        )

        # Insert multiple versions
        arrow_schema = pa.schema([
            pa.field("version_hash", pa.string(), nullable=False),
            pa.field("schema_name", pa.string(), nullable=False),
            pa.field("version", pa.int64(), nullable=False),
            pa.field("engine_version", pa.int64(), nullable=False),
            pa.field("inserted_at", pa.timestamp("us"), nullable=False),
            pa.field("schema", pa.string(), nullable=False),
        ])
        arrow_table = pa.table({
            "version_hash": ["hash_v1", "hash_v2", "hash_v3"],
            "schema_name": ["test_schema", "test_schema", "test_schema"],
            "version": [1, 2, 3],
            "engine_version": [1, 1, 1],
            "inserted_at": [datetime.now(), datetime.now(), datetime.now()],
            "schema": ['{"v": 1}', '{"v": 2}', '{"v": 3}'],
        }, schema=arrow_schema)

        version_table.append(arrow_table)
        print("   Created _dlt_version table with 3 versions")

        # Should return newest (version 3)
        result = client.get_stored_schema("test_schema")
        assert result is not None, "Should retrieve stored schema"
        assert result.version == 3, f"Should return newest version (3), got {result.version}"
        assert result.version_hash == "hash_v3", "Should return hash for version 3"
        print(f"   Correctly returned newest version: {result.version}")

    def test_get_stored_schema_by_hash_retrieves_exact_match(self, catalog_setup, client):
        """Test that get_stored_schema_by_hash retrieves exact hash match."""
        print("\nTest: get_stored_schema_by_hash retrieves exact match")

        catalog = catalog_setup["catalog"]

        # Create _dlt_version table with multiple versions
        iceberg_schema = IcebergSchema(
            NestedField(1, "version_hash", StringType(), required=True),
            NestedField(2, "schema_name", StringType(), required=True),
            NestedField(3, "version", LongType(), required=True),
            NestedField(4, "engine_version", LongType(), required=True),
            NestedField(5, "inserted_at", TimestampType(), required=True),
            NestedField(6, "schema", StringType(), required=True),
        )

        version_table = catalog.create_table(
            identifier="test._dlt_version",
            schema=iceberg_schema,
        )

        arrow_schema = pa.schema([
            pa.field("version_hash", pa.string(), nullable=False),
            pa.field("schema_name", pa.string(), nullable=False),
            pa.field("version", pa.int64(), nullable=False),
            pa.field("engine_version", pa.int64(), nullable=False),
            pa.field("inserted_at", pa.timestamp("us"), nullable=False),
            pa.field("schema", pa.string(), nullable=False),
        ])
        arrow_table = pa.table({
            "version_hash": ["hash_v1", "hash_v2"],
            "schema_name": ["test_schema", "test_schema"],
            "version": [1, 2],
            "engine_version": [1, 1],
            "inserted_at": [datetime.now(), datetime.now()],
            "schema": ['{"v": 1}', '{"v": 2}'],
        }, schema=arrow_schema)

        version_table.append(arrow_table)

        # Get by specific hash
        result = client.get_stored_schema_by_hash("hash_v1")
        assert result is not None, "Should retrieve schema by hash"
        assert result.version == 1, "Should return version 1 for hash_v1"
        print(f"   Retrieved version {result.version} for hash_v1")

        # Non-existent hash
        result_none = client.get_stored_schema_by_hash("nonexistent")
        assert result_none is None, "Should return None for non-existent hash"
        print("   Correctly returned None for non-existent hash")

    def test_get_stored_state_retrieves_newest_state(self, catalog_setup, client):
        """Test that get_stored_state retrieves the newest state for a pipeline."""
        print("\nTest: get_stored_state retrieves newest state")

        catalog = catalog_setup["catalog"]

        # Create _dlt_pipeline_state table
        iceberg_schema = IcebergSchema(
            NestedField(1, "version", LongType(), required=True),
            NestedField(2, "engine_version", LongType(), required=True),
            NestedField(3, "pipeline_name", StringType(), required=True),
            NestedField(4, "state", StringType(), required=True),
            NestedField(5, "created_at", TimestampType(), required=True),
            NestedField(6, "version_hash", StringType(), required=False),
            NestedField(7, "_dlt_load_id", StringType(), required=False),
        )

        state_table = catalog.create_table(
            identifier="test._dlt_pipeline_state",
            schema=iceberg_schema,
        )

        # Insert multiple states
        arrow_schema = pa.schema([
            pa.field("version", pa.int64(), nullable=False),
            pa.field("engine_version", pa.int64(), nullable=False),
            pa.field("pipeline_name", pa.string(), nullable=False),
            pa.field("state", pa.string(), nullable=False),
            pa.field("created_at", pa.timestamp("us"), nullable=False),
            pa.field("version_hash", pa.string(), nullable=True),
            pa.field("_dlt_load_id", pa.string(), nullable=True),
        ])
        arrow_table = pa.table({
            "version": [1, 2],
            "engine_version": [1, 1],
            "pipeline_name": ["my_pipeline", "my_pipeline"],
            "state": ['{"old": true}', '{"new": true}'],
            "created_at": [
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 2, 10, 0, 0),  # Newer
            ],
            "version_hash": ["hash1", "hash2"],
            "_dlt_load_id": ["load1", "load2"],
        }, schema=arrow_schema)

        state_table.append(arrow_table)
        print("   Created _dlt_pipeline_state table with 2 states")

        # Should return newest state
        result = client.get_stored_state("my_pipeline")
        assert result is not None, "Should retrieve stored state"
        assert result.version == 2, f"Should return newest version, got {result.version}"
        assert '{"new": true}' in result.state, "Should return newest state data"
        print(f"   Retrieved newest state version: {result.version}")

        # Non-existent pipeline
        result_none = client.get_stored_state("other_pipeline")
        assert result_none is None, "Should return None for non-existent pipeline"
        print("   Correctly returned None for non-existent pipeline")

    def test_derive_schema_from_iceberg_tables(self, catalog_setup, client):
        """Test that schema can be derived from existing Iceberg tables."""
        print("\nTest: derive schema from existing Iceberg tables")

        catalog = catalog_setup["catalog"]

        # Create an Iceberg table directly (simulating existing table)
        iceberg_schema = IcebergSchema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=False),
            NestedField(3, "value", LongType(), required=False),
        )

        catalog.create_table(
            identifier="test.user_data",
            schema=iceberg_schema,
        )
        print("   Created test.user_data table with [id, name, value]")

        # _dlt_version does NOT exist - fallback should derive from Iceberg
        result = client.get_stored_schema("test_schema")
        assert result is not None, "Should derive schema from Iceberg tables"
        assert result.version_hash == "derived_from_iceberg", "Should mark as derived"

        # Parse the schema and verify table was derived
        schema_dict = json.loads(result.schema)
        assert "user_data" in schema_dict["tables"], "Should include user_data table"

        user_data = schema_dict["tables"]["user_data"]
        assert "id" in user_data["columns"], "Should have 'id' column"
        assert "name" in user_data["columns"], "Should have 'name' column"
        assert "value" in user_data["columns"], "Should have 'value' column"

        # Check column properties
        assert user_data["columns"]["id"]["nullable"] is False, "id should be required"
        assert user_data["columns"]["name"]["nullable"] is True, "name should be nullable"

        print(f"   Derived schema with table: user_data ({len(user_data['columns'])} columns)")

    def test_get_stored_schema_prefers_dlt_version_over_derivation(self, catalog_setup, client):
        """Test that stored schema in _dlt_version takes precedence over derivation."""
        print("\nTest: _dlt_version takes precedence over Iceberg derivation")

        catalog = catalog_setup["catalog"]

        # Create _dlt_version table with stored schema
        iceberg_schema = IcebergSchema(
            NestedField(1, "version_hash", StringType(), required=True),
            NestedField(2, "schema_name", StringType(), required=True),
            NestedField(3, "version", LongType(), required=True),
            NestedField(4, "engine_version", LongType(), required=True),
            NestedField(5, "inserted_at", TimestampType(), required=True),
            NestedField(6, "schema", StringType(), required=True),
        )

        version_table = catalog.create_table(
            identifier="test._dlt_version",
            schema=iceberg_schema,
        )

        test_schema_dict = {"name": "test_schema", "tables": {"stored_table": {}}}
        arrow_schema = pa.schema([
            pa.field("version_hash", pa.string(), nullable=False),
            pa.field("schema_name", pa.string(), nullable=False),
            pa.field("version", pa.int64(), nullable=False),
            pa.field("engine_version", pa.int64(), nullable=False),
            pa.field("inserted_at", pa.timestamp("us"), nullable=False),
            pa.field("schema", pa.string(), nullable=False),
        ])
        arrow_table = pa.table({
            "version_hash": ["stored_hash"],
            "schema_name": ["test_schema"],
            "version": [5],
            "engine_version": [1],
            "inserted_at": [datetime.now()],
            "schema": [json.dumps(test_schema_dict)],
        }, schema=arrow_schema)

        version_table.append(arrow_table)
        print("   Created _dlt_version with stored schema")

        # Also create an Iceberg table (which should NOT be used)
        user_schema = IcebergSchema(
            NestedField(1, "id", LongType(), required=True),
        )
        catalog.create_table(identifier="test.user_data", schema=user_schema)
        print("   Created test.user_data table")

        # get_stored_schema should return the stored schema, not derived
        result = client.get_stored_schema("test_schema")
        assert result is not None, "Should return stored schema"
        assert result.version_hash == "stored_hash", "Should return stored schema, not derived"
        assert result.version == 5, "Should return stored version"

        print("   Correctly returned stored schema (not derived)")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
