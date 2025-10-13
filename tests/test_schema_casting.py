"""
Tests for defensive schema casting.

Verifies that schema casting properly detects data loss scenarios and
handles timezone/precision conversions safely.
"""

import pytest
import pyarrow as pa

from dlt_iceberg.schema_casting import (
    cast_table_safe,
    validate_cast,
    CastingError,
)


def test_safe_cast_identical_schema():
    """Test that identical schemas pass validation."""
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
    ])

    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        },
        schema=schema
    )

    # Cast to identical schema should work
    result = cast_table_safe(table, schema, strict=True)
    assert len(result) == 3
    assert result.schema == schema


def test_safe_cast_int_upcast():
    """Test that safe integer upcasting works (int32 → int64)."""
    source_schema = pa.schema([pa.field("value", pa.int32())])
    target_schema = pa.schema([pa.field("value", pa.int64())])

    table = pa.table({"value": [1, 2, 3]}, schema=source_schema)

    # Upcasting is safe
    result = cast_table_safe(table, target_schema, strict=True)
    assert len(result) == 3
    assert result.schema == target_schema


def test_unsafe_cast_int_downcast_strict():
    """Test that integer downcasting fails in strict mode (int64 → int32)."""
    source_schema = pa.schema([pa.field("value", pa.int64())])
    target_schema = pa.schema([pa.field("value", pa.int32())])

    table = pa.table({"value": [1, 2, 3]}, schema=source_schema)

    # Downcasting should fail in strict mode
    with pytest.raises(CastingError) as exc_info:
        cast_table_safe(table, target_schema, strict=True)

    assert "64-bit integer to 32-bit" in str(exc_info.value)


def test_unsafe_cast_int_downcast_non_strict():
    """Test that integer downcasting succeeds in non-strict mode with warning."""
    source_schema = pa.schema([pa.field("value", pa.int64())])
    target_schema = pa.schema([pa.field("value", pa.int32())])

    table = pa.table({"value": [1, 2, 3]}, schema=source_schema)

    # Should succeed in non-strict mode
    result = cast_table_safe(table, target_schema, strict=False)
    assert len(result) == 3
    assert result.schema == target_schema


def test_unsafe_cast_float_to_int_strict():
    """Test that float to integer conversion fails in strict mode."""
    source_schema = pa.schema([pa.field("value", pa.float64())])
    target_schema = pa.schema([pa.field("value", pa.int64())])

    table = pa.table({"value": [1.5, 2.7, 3.2]}, schema=source_schema)

    # Float to int truncates decimals
    with pytest.raises(CastingError) as exc_info:
        cast_table_safe(table, target_schema, strict=True)

    assert "truncates decimal" in str(exc_info.value).lower()


def test_safe_cast_int_to_float():
    """Test that integer to float conversion is safe."""
    source_schema = pa.schema([pa.field("value", pa.int32())])
    target_schema = pa.schema([pa.field("value", pa.float64())])

    table = pa.table({"value": [1, 2, 3]}, schema=source_schema)

    # Int to float is safe
    result = cast_table_safe(table, target_schema, strict=True)
    assert len(result) == 3
    assert result.schema == target_schema


def test_timestamp_naive_to_aware_warning():
    """Test that naive → aware timestamp conversion produces warning."""
    source_schema = pa.schema([
        pa.field("timestamp", pa.timestamp("us"))  # No timezone
    ])
    target_schema = pa.schema([
        pa.field("timestamp", pa.timestamp("us", tz="UTC"))  # With timezone
    ])

    table = pa.table(
        {"timestamp": [1704067200000000, 1704153600000000]},
        schema=source_schema
    )

    # This should produce a warning but succeed
    validation = validate_cast(source_schema, target_schema)
    assert len(validation.warnings) > 0
    assert "timezone-naive" in validation.warnings[0].lower()
    assert "utc" in validation.warnings[0].lower()

    # Should succeed in both modes
    result = cast_table_safe(table, target_schema, strict=True)
    assert len(result) == 2


def test_timestamp_aware_to_naive_fails_strict():
    """Test that aware → naive timestamp conversion fails in strict mode."""
    source_schema = pa.schema([
        pa.field("timestamp", pa.timestamp("us", tz="UTC"))
    ])
    target_schema = pa.schema([
        pa.field("timestamp", pa.timestamp("us"))  # No timezone
    ])

    table = pa.table(
        {"timestamp": [1704067200000000, 1704153600000000]},
        schema=source_schema
    )

    # Should fail in strict mode (loses timezone info)
    with pytest.raises(CastingError) as exc_info:
        cast_table_safe(table, target_schema, strict=True)

    assert "timezone-aware" in str(exc_info.value).lower()
    assert "timezone-naive" in str(exc_info.value).lower()
    assert "loses timezone information" in str(exc_info.value).lower()


def test_timestamp_aware_to_different_aware():
    """Test that aware → different aware timestamp conversion produces warning."""
    source_schema = pa.schema([
        pa.field("timestamp", pa.timestamp("us", tz="America/New_York"))
    ])
    target_schema = pa.schema([
        pa.field("timestamp", pa.timestamp("us", tz="Europe/London"))
    ])

    table = pa.table(
        {"timestamp": [1704067200000000, 1704153600000000]},
        schema=source_schema
    )

    # This should produce a warning but succeed (conversion happens)
    validation = validate_cast(source_schema, target_schema)
    assert len(validation.warnings) > 0
    assert "america/new_york" in validation.warnings[0].lower()
    assert "europe/london" in validation.warnings[0].lower()

    # Should succeed
    result = cast_table_safe(table, target_schema, strict=True)
    assert len(result) == 2


def test_timestamp_precision_loss_strict():
    """Test that timestamp precision loss fails in strict mode (us → ms)."""
    source_schema = pa.schema([
        pa.field("timestamp", pa.timestamp("us"))  # Microseconds
    ])
    target_schema = pa.schema([
        pa.field("timestamp", pa.timestamp("ms"))  # Milliseconds
    ])

    table = pa.table(
        {"timestamp": [1704067200000000, 1704153600000001]},  # Note microsecond precision
        schema=source_schema
    )

    # Should fail in strict mode (loses precision)
    with pytest.raises(CastingError) as exc_info:
        cast_table_safe(table, target_schema, strict=True)

    assert "us to ms" in str(exc_info.value).lower()
    assert "precision" in str(exc_info.value).lower()


def test_timestamp_precision_gain_safe():
    """Test that timestamp precision gain is safe (ms → us)."""
    source_schema = pa.schema([
        pa.field("timestamp", pa.timestamp("ms"))
    ])
    target_schema = pa.schema([
        pa.field("timestamp", pa.timestamp("us"))
    ])

    table = pa.table(
        {"timestamp": [1704067200000, 1704153600000]},
        schema=source_schema
    )

    # Precision gain is safe
    result = cast_table_safe(table, target_schema, strict=True)
    assert len(result) == 2
    assert result.schema == target_schema


def test_decimal_precision_loss_strict():
    """Test that decimal precision loss fails in strict mode."""
    from decimal import Decimal

    source_schema = pa.schema([
        pa.field("amount", pa.decimal128(10, 2))  # 10 digits, 2 decimal places
    ])
    target_schema = pa.schema([
        pa.field("amount", pa.decimal128(8, 2))  # 8 digits, 2 decimal places
    ])

    table = pa.table(
        {"amount": pa.array([Decimal("12345.67")], type=pa.decimal128(10, 2))},
        schema=source_schema
    )

    # Should fail (precision loss)
    with pytest.raises(CastingError) as exc_info:
        cast_table_safe(table, target_schema, strict=True)

    assert "precision" in str(exc_info.value).lower()


def test_decimal_scale_loss_strict():
    """Test that decimal scale loss fails in strict mode."""
    from decimal import Decimal

    source_schema = pa.schema([
        pa.field("amount", pa.decimal128(10, 4))  # 4 decimal places
    ])
    target_schema = pa.schema([
        pa.field("amount", pa.decimal128(10, 2))  # 2 decimal places
    ])

    table = pa.table(
        {"amount": pa.array([Decimal("123.4567")], type=pa.decimal128(10, 4))},
        schema=source_schema
    )

    # Should fail (scale loss)
    with pytest.raises(CastingError) as exc_info:
        cast_table_safe(table, target_schema, strict=True)

    assert "scale" in str(exc_info.value).lower()
    assert "truncates" in str(exc_info.value).lower()


def test_unsigned_to_signed_warning():
    """Test that unsigned to signed conversion produces warning."""
    source_schema = pa.schema([pa.field("value", pa.uint32())])
    target_schema = pa.schema([pa.field("value", pa.int32())])

    table = pa.table({"value": [1, 2, 3]}, schema=source_schema)

    validation = validate_cast(source_schema, target_schema)
    assert len(validation.warnings) > 0
    assert "unsigned to signed" in validation.warnings[0].lower()


def test_string_to_binary_safe():
    """Test that string to binary conversion is safe."""
    source_schema = pa.schema([pa.field("data", pa.string())])
    target_schema = pa.schema([pa.field("data", pa.binary())])

    table = pa.table({"data": ["hello", "world"]}, schema=source_schema)

    # String to binary is safe
    result = cast_table_safe(table, target_schema, strict=True)
    assert len(result) == 2


def test_string_to_large_string_safe():
    """Test that string to large_string conversion is safe."""
    source_schema = pa.schema([pa.field("data", pa.string())])
    target_schema = pa.schema([pa.field("data", pa.large_string())])

    table = pa.table({"data": ["hello", "world"]}, schema=source_schema)

    # String to large_string is safe
    result = cast_table_safe(table, target_schema, strict=True)
    assert len(result) == 2
    assert result.schema == target_schema


def test_large_string_to_string_safe():
    """Test that large_string to string conversion is safe."""
    source_schema = pa.schema([pa.field("data", pa.large_string())])
    target_schema = pa.schema([pa.field("data", pa.string())])

    table = pa.table({"data": ["hello", "world"]}, schema=source_schema)

    # Large_string to string is safe
    result = cast_table_safe(table, target_schema, strict=True)
    assert len(result) == 2
    assert result.schema == target_schema


def test_binary_to_string_warning():
    """Test that binary to string conversion produces warning."""
    source_schema = pa.schema([pa.field("data", pa.binary())])
    target_schema = pa.schema([pa.field("data", pa.string())])

    table = pa.table({"data": [b"hello", b"world"]}, schema=source_schema)

    validation = validate_cast(source_schema, target_schema)
    assert len(validation.warnings) > 0
    assert "utf-8" in validation.warnings[0].lower()


def test_missing_field_in_target_error():
    """Test that missing field in target schema produces error."""
    source_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
    ])
    target_schema = pa.schema([
        pa.field("id", pa.int64()),
        # "name" is missing
    ])

    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        },
        schema=source_schema
    )

    # Should fail (field missing in target)
    with pytest.raises(CastingError) as exc_info:
        cast_table_safe(table, target_schema, strict=True)

    assert "name" in str(exc_info.value).lower()
    assert "not in target" in str(exc_info.value).lower()


def test_extra_field_in_target_warning():
    """Test that extra field in target schema produces warning."""
    source_schema = pa.schema([
        pa.field("id", pa.int64()),
    ])
    target_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),  # Extra field
    ])

    validation = validate_cast(source_schema, target_schema)
    assert len(validation.warnings) > 0
    assert "name" in validation.warnings[0].lower()
    assert "not in source" in validation.warnings[0].lower()


def test_complex_multi_field_validation():
    """Test validation with multiple fields and mixed safety."""
    source_schema = pa.schema([
        pa.field("id", pa.int32()),  # Safe upcast
        pa.field("amount", pa.float64()),  # Unsafe to int
        pa.field("timestamp", pa.timestamp("us")),  # Unsafe to aware
        pa.field("name", pa.string()),  # Safe
    ])
    target_schema = pa.schema([
        pa.field("id", pa.int64()),  # Upcast OK
        pa.field("amount", pa.int64()),  # Float to int NOT OK
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),  # Naive to aware WARNING
        pa.field("name", pa.string()),  # Same type OK
    ])

    validation = validate_cast(source_schema, target_schema)

    # Should have errors for amount
    assert not validation.is_safe()
    assert any("truncates decimal" in err.lower() for err in validation.errors)

    # Should have warning for timestamp
    assert any("timezone-naive" in warn.lower() for warn in validation.warnings)


def test_validate_cast_result_structure():
    """Test that validation result has proper structure."""
    source_schema = pa.schema([pa.field("id", pa.int64())])
    target_schema = pa.schema([pa.field("id", pa.int32())])

    result = validate_cast(source_schema, target_schema)

    # Check result structure
    assert hasattr(result, "safe")
    assert hasattr(result, "warnings")
    assert hasattr(result, "errors")
    assert hasattr(result, "is_safe")
    assert callable(result.is_safe)

    # Should be unsafe due to downcast
    assert not result.is_safe()
    assert len(result.errors) > 0


def test_real_world_iceberg_scenario():
    """
    Test a realistic scenario: dlt generates timestamp without timezone,
    but Iceberg table expects timestamp with UTC timezone.
    """
    from decimal import Decimal

    # DLT typically produces timezone-naive timestamps
    dlt_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("created_at", pa.timestamp("us")),  # No timezone
        pa.field("amount", pa.decimal128(18, 2)),
    ])

    # Iceberg often uses UTC timestamps
    iceberg_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("created_at", pa.timestamp("us", tz="UTC")),  # With timezone
        pa.field("amount", pa.decimal128(18, 2)),
    ])

    table = pa.table(
        {
            "id": [1, 2, 3],
            "created_at": [1704067200000000, 1704153600000000, 1704240000000000],
            "amount": pa.array(
                [Decimal("100.50"), Decimal("200.75"), Decimal("300.25")],
                type=pa.decimal128(18, 2)
            ),
        },
        schema=dlt_schema
    )

    # Validation should produce warning about timezone
    validation = validate_cast(dlt_schema, iceberg_schema)
    assert len(validation.warnings) > 0
    assert any("timezone" in w.lower() for w in validation.warnings)

    # But cast should succeed (this is common and expected)
    result = cast_table_safe(table, iceberg_schema, strict=True)
    assert len(result) == 3
    assert result.schema == iceberg_schema
