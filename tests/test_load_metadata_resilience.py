"""
Tests for _dlt_loads metadata write resilience in class-based destination.
"""

from unittest.mock import Mock, patch

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException

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
