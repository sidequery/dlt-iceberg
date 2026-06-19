"""
Tests for IcebergRestClient.drop_tables (per-table drop contract).

dlt core's refresh="drop_resources" / refresh="drop_sources" paths call
`job_client.drop_tables(*names, delete_schema=True)` when available
(dlt/load/utils.py). Without this method, the load layer logs a warning
and silently skips the drops, leaving stale tables in the destination.

Covers:
1. drop_tables is exposed on IcebergRestClient (hasattr gate in dlt core).
2. drop_tables removes only the named tables from the catalog.
3. drop_tables is idempotent for missing table names.
4. delete_schema=True removes version rows for self.schema.name from _dlt_version.
5. pipeline.run(..., refresh="drop_resources") now actually drops tables end-to-end.
"""

import tempfile
import shutil
from types import SimpleNamespace
from unittest.mock import Mock

import dlt
from pyiceberg.catalog import load_catalog


# ---------------------------------------------------------------------------
# Gate 1: method must exist (dlt core uses hasattr to decide)
# ---------------------------------------------------------------------------


def test_drop_tables_method_exists():
    """dlt's load layer uses hasattr(job_client, 'drop_tables') to decide
    whether to honor refresh='drop_resources'. If we don't expose it, drops
    are silently skipped."""
    from dlt_iceberg.destination_client import IcebergRestClient

    assert hasattr(IcebergRestClient, "drop_tables"), (
        "IcebergRestClient must implement drop_tables(*names, delete_schema=bool) "
        "for dlt refresh modes to work."
    )


def test_drop_tables_prefers_purge_table_when_available():
    """REST catalogs support purge_table, which removes table data and metadata
    files instead of only unregistering the catalog entry."""
    from dlt_iceberg.destination_client import IcebergRestClient

    catalog = SimpleNamespace(purge_table=Mock(), drop_table=Mock())
    client = object.__new__(IcebergRestClient)
    client.config = SimpleNamespace(namespace="drop_ns")
    client._get_catalog = Mock(return_value=catalog)

    client.drop_tables("events", delete_schema=False)

    catalog.purge_table.assert_called_once_with("drop_ns.events")
    catalog.drop_table.assert_not_called()


# ---------------------------------------------------------------------------
# Behavior: drop_tables removes only named tables
# ---------------------------------------------------------------------------


def test_drop_tables_removes_named_tables_only():
    """drop_tables drops the tables passed in and leaves the rest alone."""
    temp_dir = tempfile.mkdtemp()

    print(f"\nTest: drop_tables removes only named tables")
    print(f"   Temp dir: {temp_dir}")

    try:
        from dlt_iceberg import iceberg_rest

        pipeline = dlt.pipeline(
            pipeline_name="test_drop_tables_named",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{temp_dir}/catalog.db",
                warehouse=f"file://{temp_dir}/warehouse",
                namespace="drop_ns",
            ),
            dataset_name="drop_ds",
        )

        @dlt.resource(name="keep_me")
        def keep_me():
            yield {"id": 1, "name": "stay"}

        @dlt.resource(name="drop_me")
        def drop_me():
            yield {"id": 2, "name": "go"}

        load_info = pipeline.run([keep_me(), drop_me()])
        assert not load_info.has_failed_jobs

        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{temp_dir}/catalog.db",
            warehouse=f"file://{temp_dir}/warehouse",
        )
        tables_before = catalog.list_tables("drop_ns")
        print(f"   Tables before drop: {[t[1] for t in tables_before]}")
        assert ("drop_ns", "keep_me") in tables_before
        assert ("drop_ns", "drop_me") in tables_before

        with pipeline.destination_client() as client:
            client.drop_tables("drop_me", delete_schema=False)

        tables_after = catalog.list_tables("drop_ns")
        print(f"   Tables after drop: {[t[1] for t in tables_after]}")
        assert ("drop_ns", "keep_me") in tables_after
        assert ("drop_ns", "drop_me") not in tables_after
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Behavior: idempotent for missing tables
# ---------------------------------------------------------------------------


def test_drop_tables_is_idempotent_on_missing():
    """Dropping a non-existent table is a no-op, not an error. dlt core
    may pass names for tables that were never physically created."""
    temp_dir = tempfile.mkdtemp()

    print(f"\nTest: drop_tables on missing table is a no-op")
    print(f"   Temp dir: {temp_dir}")

    try:
        from dlt_iceberg import iceberg_rest

        pipeline = dlt.pipeline(
            pipeline_name="test_drop_tables_missing",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{temp_dir}/catalog.db",
                warehouse=f"file://{temp_dir}/warehouse",
                namespace="drop_ns",
            ),
            dataset_name="drop_ds",
        )

        @dlt.resource(name="only_real")
        def only_real():
            yield {"id": 1}

        pipeline.run(only_real())

        with pipeline.destination_client() as client:
            client.drop_tables("does_not_exist", delete_schema=False)
        print("   drop_tables('does_not_exist') returned without raising")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Behavior: delete_schema=True clears _dlt_version rows
# ---------------------------------------------------------------------------


def test_drop_tables_delete_schema_clears_version_rows():
    """delete_schema=True must remove all _dlt_version rows for the current
    schema name, matching the SqlJobClientBase.drop_tables contract."""
    temp_dir = tempfile.mkdtemp()

    print(f"\nTest: drop_tables(delete_schema=True) clears _dlt_version rows")
    print(f"   Temp dir: {temp_dir}")

    try:
        from dlt_iceberg import iceberg_rest

        pipeline = dlt.pipeline(
            pipeline_name="test_drop_tables_delete_schema",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{temp_dir}/catalog.db",
                warehouse=f"file://{temp_dir}/warehouse",
                namespace="drop_ns",
            ),
            dataset_name="drop_ds",
        )

        @dlt.resource(name="events")
        def events():
            yield {"id": 1, "kind": "click"}

        pipeline.run(events())

        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{temp_dir}/catalog.db",
            warehouse=f"file://{temp_dir}/warehouse",
        )
        version_table = catalog.load_table("drop_ns._dlt_version")
        schema_name = pipeline.default_schema_name

        before = version_table.scan(
            row_filter=f"schema_name = '{schema_name}'"
        ).to_arrow()
        print(f"   Version rows before drop: {len(before)}")
        assert len(before) >= 1, "expected at least one schema version row"

        with pipeline.destination_client() as client:
            client.drop_tables("events", delete_schema=True)

        version_table.refresh()
        after = version_table.scan(
            row_filter=f"schema_name = '{schema_name}'"
        ).to_arrow()
        print(f"   Version rows after drop: {len(after)}")
        assert len(after) == 0, (
            f"delete_schema=True should remove all version rows for "
            f"schema '{schema_name}'; found {len(after)}"
        )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# End-to-end: refresh='drop_resources' originally warned and skipped
# ---------------------------------------------------------------------------


def test_refresh_drop_resources_actually_drops():
    """The originally reported symptom: pipeline.run(..., refresh='drop_resources')
    logged 'Client for iceberg_rest does not implement drop table' and left
    the target tables in place. After the fix, the old schema is wiped and
    the second run sees only the new columns."""
    temp_dir = tempfile.mkdtemp()

    print(f"\nTest: refresh='drop_resources' actually drops the table")
    print(f"   Temp dir: {temp_dir}")

    try:
        from dlt_iceberg import iceberg_rest

        pipeline = dlt.pipeline(
            pipeline_name="test_refresh_drop_resources",
            destination=iceberg_rest(
                catalog_uri=f"sqlite:///{temp_dir}/catalog.db",
                warehouse=f"file://{temp_dir}/warehouse",
                namespace="drop_ns",
            ),
            dataset_name="drop_ds",
        )

        @dlt.resource(name="refreshable")
        def refreshable_v1():
            yield {"id": 1, "old_col": "old"}

        pipeline.run(refreshable_v1())

        catalog = load_catalog(
            "dlt_catalog",
            type="sql",
            uri=f"sqlite:///{temp_dir}/catalog.db",
            warehouse=f"file://{temp_dir}/warehouse",
        )
        fields_before = {f.name for f in catalog.load_table("drop_ns.refreshable").schema().fields}
        print(f"   Fields before refresh: {sorted(fields_before)}")
        assert "old_col" in fields_before

        @dlt.resource(name="refreshable")
        def refreshable_v2():
            yield {"id": 2, "new_col": "new"}

        pipeline.run(refreshable_v2(), refresh="drop_resources")

        fields_after = {f.name for f in catalog.load_table("drop_ns.refreshable").schema().fields}
        print(f"   Fields after refresh:  {sorted(fields_after)}")
        assert "new_col" in fields_after
        assert "old_col" not in fields_after, (
            "refresh='drop_resources' should have dropped the old table "
            "so old_col no longer exists in the schema"
        )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
