import pyarrow as pa
import pytest

from dlt_iceberg.merge_utils import (
    build_primary_key_delete_filter,
    iter_primary_key_delete_filters,
)


class CountingColumn:
    def __init__(self, values):
        self.values = values
        self.to_pylist_calls = 0

    def to_pylist(self):
        self.to_pylist_calls += 1
        return self.values


class CountingTable:
    def __init__(self, columns):
        self.columns = {
            name: CountingColumn(values)
            for name, values in columns.items()
        }

    def column(self, name):
        return self.columns[name]


def test_composite_primary_key_filter_materializes_each_column_once():
    table = CountingTable(
        {
            "user_id": [1, 1, 2, 2, 2],
            "event_date": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-02",
            ],
        }
    )

    _, unique_count = build_primary_key_delete_filter(
        table, ["user_id", "event_date"]
    )

    assert unique_count == 3
    assert table.columns["user_id"].to_pylist_calls == 1
    assert table.columns["event_date"].to_pylist_calls == 1


def test_composite_primary_key_filters_are_bounded_for_reported_row_count():
    table = pa.table(
        {
            "account_id": list(range(1018)),
            "event_id": [f"event-{index}" for index in range(1018)],
        }
    )

    batches = list(
        iter_primary_key_delete_filters(
            table, ["account_id", "event_id"], batch_size=500
        )
    )

    assert [key_count for _, key_count in batches] == [500, 500, 18]


def test_primary_key_filter_batch_size_must_be_positive():
    table = pa.table({"id": [1]})

    with pytest.raises(ValueError, match="merge_batch_size must be greater than 0"):
        list(iter_primary_key_delete_filters(table, ["id"], batch_size=0))
