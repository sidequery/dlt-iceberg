"""Utilities shared by merge strategies."""

from itertools import islice
from typing import Any, Iterable, Iterator, List, Sequence, Tuple

import pyarrow as pa


def unique_primary_key_values(table: pa.Table, primary_keys: List[str]) -> set:
    """Return unique primary-key values without repeatedly materializing Arrow columns."""
    if len(primary_keys) == 1:
        return set(table.column(primary_keys[0]).to_pylist())

    key_columns = [table.column(pk).to_pylist() for pk in primary_keys]
    return set(zip(*key_columns))


def _build_primary_key_delete_filter(
    primary_keys: Sequence[str], unique_key_values: Iterable[Any]
) -> Tuple[Any, int]:
    """Build a PyIceberg delete expression from already-unique key values."""
    from pyiceberg.expressions import AlwaysFalse, And, EqualTo, In, Or

    key_values = list(unique_key_values)

    if len(primary_keys) == 1:
        return In(primary_keys[0], key_values), len(key_values)

    conditions = []
    for pk_tuple in key_values:
        and_conditions = [
            EqualTo(pk, val) for pk, val in zip(primary_keys, pk_tuple)
        ]
        if len(and_conditions) == 1:
            conditions.append(and_conditions[0])
        else:
            conditions.append(And(*and_conditions))

    if len(conditions) == 0:
        return AlwaysFalse(), 0

    if len(conditions) == 1:
        return conditions[0], len(key_values)

    return Or(*conditions), len(key_values)


def build_primary_key_delete_filter(
    table: pa.Table, primary_keys: List[str]
) -> Tuple[Any, int]:
    """Build one PyIceberg delete expression for the unique primary keys in table."""
    return _build_primary_key_delete_filter(
        primary_keys, unique_primary_key_values(table, primary_keys)
    )


def iter_primary_key_delete_filters(
    table: pa.Table,
    primary_keys: List[str],
    batch_size: int,
) -> Iterator[Tuple[Any, int]]:
    """Yield delete expressions whose unique-key count is bounded by ``batch_size``."""
    if batch_size <= 0:
        raise ValueError("merge_batch_size must be greater than 0")

    unique_values = iter(unique_primary_key_values(table, primary_keys))
    while key_batch := list(islice(unique_values, batch_size)):
        yield _build_primary_key_delete_filter(primary_keys, key_batch)
