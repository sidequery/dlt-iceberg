"""Utilities shared by merge strategies."""

from typing import Any, List, Tuple

import pyarrow as pa


def unique_primary_key_values(table: pa.Table, primary_keys: List[str]) -> set:
    """Return unique primary-key values without repeatedly materializing Arrow columns."""
    if len(primary_keys) == 1:
        return set(table.column(primary_keys[0]).to_pylist())

    key_columns = [table.column(pk).to_pylist() for pk in primary_keys]
    return set(zip(*key_columns))


def build_primary_key_delete_filter(table: pa.Table, primary_keys: List[str]) -> Tuple[Any, int]:
    """Build a PyIceberg delete expression for the unique primary keys in table."""
    from pyiceberg.expressions import AlwaysFalse, And, EqualTo, In, Or

    unique_key_values = unique_primary_key_values(table, primary_keys)

    if len(primary_keys) == 1:
        return In(primary_keys[0], list(unique_key_values)), len(unique_key_values)

    conditions = []
    for pk_tuple in unique_key_values:
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
        return conditions[0], len(unique_key_values)

    return Or(*conditions), len(unique_key_values)
