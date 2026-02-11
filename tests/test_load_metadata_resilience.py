"""
Tests for _dlt_loads metadata write resilience in class-based destination.
"""

from unittest.mock import Mock, patch

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema import Schema
from dlt_iceberg.destination_client import IcebergRestClient, IcebergRestConfiguration


def _make_client(max_retries: int = 3) -> IcebergRestClient:
    schema = Schema("test_schema")
    config = IcebergRestConfiguration(
        catalog_uri="sqlite:///unused.db",
        namespace="test_ns",
        max_retries=max_retries,
        retry_backoff_base=2.0,
    )
    return IcebergRestClient(schema, config, DestinationCapabilitiesContext())


def _empty_load_rows() -> pa.Table:
    return pa.table({"load_id": pa.array([], type=pa.string())})


def _existing_load_rows(load_id: str) -> pa.Table:
    return pa.table({"load_id": pa.array([load_id], type=pa.string())})


def test_store_completed_load_retries_transient_append_error() -> None:
    client = _make_client(max_retries=3)
    load_id = "load-1"

    catalog = Mock()
    loads_table = Mock()
    catalog.load_table.return_value = loads_table
    loads_table.scan.return_value.to_arrow.side_effect = [
        _empty_load_rows(),   # Initial idempotency check (attempt 1)
        _empty_load_rows(),   # Read-after-error ambiguity check (attempt 1)
        _empty_load_rows(),   # Initial idempotency check (attempt 2)
    ]
    loads_table.append.side_effect = [
        CommitFailedException("transient commit failure"),
        None,
    ]

    with (
        patch("dlt_iceberg.destination_client.schema_to_pyarrow", return_value=pa.schema([])),
        patch("dlt_iceberg.destination_client.cast_table_safe", side_effect=lambda table, *_args, **_kwargs: table),
        patch("dlt_iceberg.destination_client.time.sleep") as sleep_mock,
    ):
        client._store_completed_load(catalog, load_id)

    assert loads_table.append.call_count == 2
    sleep_mock.assert_called_once_with(1.0)


def test_store_completed_load_is_idempotent_by_load_id() -> None:
    client = _make_client(max_retries=3)
    load_id = "load-1"

    catalog = Mock()
    loads_table = Mock()
    catalog.load_table.return_value = loads_table
    loads_table.scan.return_value.to_arrow.return_value = _existing_load_rows(load_id)

    client._store_completed_load(catalog, load_id)

    loads_table.append.assert_not_called()


def test_store_completed_load_handles_ambiguous_append_outcome() -> None:
    client = _make_client(max_retries=3)
    load_id = "load-1"

    catalog = Mock()
    loads_table = Mock()
    catalog.load_table.return_value = loads_table
    loads_table.scan.return_value.to_arrow.side_effect = [
        _empty_load_rows(),         # Initial idempotency check
        _existing_load_rows(load_id),  # Read-after-error ambiguity check
    ]
    loads_table.append.side_effect = [CommitFailedException("state unknown")]

    with (
        patch("dlt_iceberg.destination_client.schema_to_pyarrow", return_value=pa.schema([])),
        patch("dlt_iceberg.destination_client.cast_table_safe", side_effect=lambda table, *_args, **_kwargs: table),
        patch("dlt_iceberg.destination_client.time.sleep") as sleep_mock,
    ):
        client._store_completed_load(catalog, load_id)

    assert loads_table.append.call_count == 1
    sleep_mock.assert_not_called()


def test_get_or_create_loads_table_honors_custom_location_layout() -> None:
    client = _make_client(max_retries=3)
    client.config.table_location_layout = "custom_path/{namespace}/{table_name}"
    client.config.warehouse = "file:///warehouse_root"

    load_row = pa.table(
        {
            "load_id": ["load-1"],
            "schema_name": ["test_schema"],
            "status": [0],
            "inserted_at": [None],
            "schema_version_hash": ["abc"],
        }
    )
    loads_table_name = client.schema.loads_table_name
    identifier = f"{client.config.namespace}.{loads_table_name}"

    catalog = Mock()
    created_table = Mock()
    catalog.load_table.side_effect = [NoSuchTableError("missing"), created_table]

    with patch("dlt_iceberg.destination_client.convert_dlt_to_iceberg_schema", return_value=Mock()):
        result = client._get_or_create_loads_table(
            catalog=catalog,
            identifier=identifier,
            loads_table_name=loads_table_name,
            load_row=load_row,
        )

    assert result is created_table
    create_kwargs = catalog.create_table.call_args.kwargs
    assert create_kwargs["location"] == "file:///warehouse_root/custom_path/test_ns/_dlt_loads"
