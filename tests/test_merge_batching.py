from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from dlt_iceberg.destination_client import (
    IcebergRestClient,
    IcebergRestConfiguration,
)


class RecordingTransaction:
    def __init__(self):
        self.delete = Mock()
        self.append = Mock()
        self.upsert = Mock()
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.committed = exc_type is None


class RecordingTable:
    def __init__(self, transaction):
        self._transaction = transaction
        self.transaction_calls = 0

    def transaction(self):
        self.transaction_calls += 1
        return self._transaction


def make_client(batch_size=500000, composite_key_batch_size=500):
    client = object.__new__(IcebergRestClient)
    client.config = SimpleNamespace(
        merge_batch_size=batch_size,
        merge_composite_key_batch_size=composite_key_batch_size,
    )
    return client


def composite_table(row_count):
    return pa.table(
        {
            "account_id": list(range(row_count)),
            "event_id": [f"event-{index}" for index in range(row_count)],
            "value": list(range(row_count)),
        }
    )


def test_delete_insert_batches_reported_composite_key_load_atomically():
    client = make_client()
    transaction = RecordingTransaction()
    table = RecordingTable(transaction)
    incoming = composite_table(1018)

    deleted, inserted, hard_deleted = client._execute_delete_insert(
        table,
        incoming,
        ["account_id", "event_id"],
        "analytics.events",
    )

    assert (deleted, inserted, hard_deleted) == (1018, 1018, 0)
    assert transaction.delete.call_count == 3
    transaction.append.assert_called_once_with(incoming)
    assert table.transaction_calls == 1
    assert transaction.committed


def test_upsert_batches_use_one_transaction_and_preserve_source_validation():
    client = make_client()
    transaction = RecordingTransaction()
    transaction.upsert.side_effect = [
        SimpleNamespace(rows_updated=100, rows_inserted=400),
        SimpleNamespace(rows_updated=200, rows_inserted=300),
        SimpleNamespace(rows_updated=10, rows_inserted=8),
    ]
    table = RecordingTable(transaction)

    updated, inserted, batch_count = client._execute_upsert(
        table,
        composite_table(1018),
        ["account_id", "event_id"],
        "analytics.events",
    )

    assert (updated, inserted) == (310, 708)
    assert batch_count == 3
    assert [len(call.kwargs["df"]) for call in transaction.upsert.call_args_list] == [
        500,
        500,
        18,
    ]
    assert table.transaction_calls == 1
    assert transaction.committed


def test_upsert_batch_failure_does_not_commit_transaction():
    client = make_client()
    transaction = RecordingTransaction()
    transaction.upsert.side_effect = [
        SimpleNamespace(rows_updated=0, rows_inserted=500),
        RuntimeError("native merge failed"),
    ]
    table = RecordingTable(transaction)

    with pytest.raises(RuntimeError, match="native merge failed"):
        client._execute_upsert(
            table,
            composite_table(501),
            ["account_id", "event_id"],
            "analytics.events",
        )

    assert transaction.upsert.call_count == 2
    assert table.transaction_calls == 1
    assert not transaction.committed


def test_upsert_rejects_duplicates_across_batch_boundaries_before_transaction():
    client = make_client(batch_size=2)
    transaction = RecordingTransaction()
    table = RecordingTable(transaction)
    incoming = pa.table(
        {
            "account_id": [1, 2, 1],
            "event_id": ["a", "b", "a"],
            "value": [10, 20, 30],
        }
    )

    with pytest.raises(ValueError, match="Duplicate rows found"):
        client._execute_upsert(
            table,
            incoming,
            ["account_id", "event_id"],
            "analytics.events",
        )

    assert table.transaction_calls == 0


def test_single_key_merge_keeps_general_batch_size():
    client = make_client(batch_size=500000, composite_key_batch_size=500)

    assert client._merge_batch_size(["id"]) == 500000
    assert client._merge_batch_size(["account_id", "event_id"]) == 500


def test_merge_batch_configuration_defaults_are_strategy_specific():
    config = IcebergRestConfiguration()

    assert config.merge_batch_size == 500000
    assert config.merge_composite_key_batch_size == 500


@pytest.mark.parametrize("strategy", ["upsert", "delete-insert"])
def test_pyiceberg_composite_merge_regression_at_reported_row_count(
    tmp_path, strategy
):
    catalog = load_catalog(
        f"regression_{strategy}",
        type="sql",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path / 'warehouse'}",
    )
    catalog.create_namespace("analytics")
    iceberg_table = catalog.create_table(
        "analytics.events",
        schema=Schema(
            NestedField(1, "account_id", LongType(), required=False),
            NestedField(2, "event_id", StringType(), required=False),
            NestedField(3, "value", LongType(), required=False),
        ),
    )
    initial = composite_table(1018)
    iceberg_table.append(initial)
    incoming = initial.set_column(
        2,
        "value",
        pa.array([index + 10000 for index in range(1018)], type=pa.int64()),
    )
    client = make_client()

    if strategy == "upsert":
        updated, inserted, batch_count = client._execute_upsert(
            iceberg_table,
            incoming,
            ["account_id", "event_id"],
            "analytics.events",
        )
        assert (updated, inserted, batch_count) == (1018, 0, 3)
    else:
        deleted, inserted, hard_deleted = client._execute_delete_insert(
            iceberg_table,
            incoming,
            ["account_id", "event_id"],
            "analytics.events",
        )
        assert (deleted, inserted, hard_deleted) == (1018, 1018, 0)

    result = catalog.load_table("analytics.events").scan().to_arrow()
    assert len(result) == 1018
    assert min(result.column("value").to_pylist()) == 10000
    assert max(result.column("value").to_pylist()) == 11017
