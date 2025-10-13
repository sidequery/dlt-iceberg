"""
Defensive schema casting for PyArrow tables.

This module provides safe casting operations that detect potential data loss
and allow users to control casting behavior.
"""

import logging
from typing import List, Optional, Tuple
import pyarrow as pa

logger = logging.getLogger(__name__)


class CastingError(Exception):
    """Raised when a cast would result in data loss in strict mode."""
    pass


class CastValidationResult:
    """Result of validating a cast operation."""

    def __init__(self):
        self.safe = True
        self.warnings: List[str] = []
        self.errors: List[str] = []

    def add_warning(self, message: str):
        """Add a warning about the cast."""
        self.warnings.append(message)
        logger.warning(f"Cast warning: {message}")

    def add_error(self, message: str):
        """Add an error about the cast."""
        self.errors.append(message)
        self.safe = False
        logger.error(f"Cast error: {message}")

    def is_safe(self) -> bool:
        """Check if the cast is safe."""
        return self.safe and len(self.errors) == 0


def _check_timestamp_cast(
    source_type: pa.DataType,
    target_type: pa.DataType,
    field_name: str,
    result: CastValidationResult
) -> None:
    """
    Check if timestamp cast is safe.

    Validates:
    - Timezone conversions (naive → aware, aware → naive, aware → different aware)
    - Unit conversions (us → ms → s)
    """
    if not pa.types.is_timestamp(source_type) or not pa.types.is_timestamp(target_type):
        return

    source_tz = source_type.tz
    target_tz = target_type.tz

    # Check timezone conversions
    if source_tz is None and target_tz is not None:
        # Naive to aware - this requires assuming UTC which may not be correct
        result.add_warning(
            f"Field '{field_name}': Converting timezone-naive timestamp to "
            f"timezone-aware ({target_tz}). Values will be interpreted as UTC."
        )
    elif source_tz is not None and target_tz is None:
        # Aware to naive - loses timezone information
        result.add_error(
            f"Field '{field_name}': Converting timezone-aware ({source_tz}) timestamp to "
            f"timezone-naive loses timezone information"
        )
    elif source_tz is not None and target_tz is not None and source_tz != target_tz:
        # Aware to different aware - this is safe (conversion happens)
        result.add_warning(
            f"Field '{field_name}': Converting timestamp from {source_tz} to {target_tz}"
        )

    # Check unit conversions (precision loss)
    source_unit = source_type.unit
    target_unit = target_type.unit

    # Units in order of precision: ns > us > ms > s
    unit_precision = {'ns': 4, 'us': 3, 'ms': 2, 's': 1}

    source_prec = unit_precision.get(source_unit, 0)
    target_prec = unit_precision.get(target_unit, 0)

    if source_prec > target_prec:
        result.add_error(
            f"Field '{field_name}': Converting timestamp from {source_unit} to {target_unit} "
            f"loses precision"
        )


def _check_numeric_cast(
    source_type: pa.DataType,
    target_type: pa.DataType,
    field_name: str,
    result: CastValidationResult
) -> None:
    """
    Check if numeric cast is safe.

    Validates:
    - Integer downcasting (int64 → int32, int32 → int16, etc.)
    - Float to integer conversion (always unsafe due to truncation)
    - Decimal precision/scale changes
    - Unsigned to signed conversions
    """

    # Float to integer
    if pa.types.is_floating(source_type) and pa.types.is_integer(target_type):
        result.add_error(
            f"Field '{field_name}': Converting float to integer truncates decimal values"
        )
        return

    # Integer bit size reductions
    source_bits = _get_integer_bits(source_type)
    target_bits = _get_integer_bits(target_type)

    if source_bits and target_bits and source_bits > target_bits:
        result.add_error(
            f"Field '{field_name}': Converting {source_bits}-bit integer to {target_bits}-bit "
            f"may overflow"
        )

    # Unsigned to signed conversions (can overflow)
    if _is_unsigned_int(source_type) and _is_signed_int(target_type):
        result.add_warning(
            f"Field '{field_name}': Converting unsigned to signed integer may overflow "
            f"for large values"
        )

    # Decimal precision/scale changes
    if pa.types.is_decimal(source_type) and pa.types.is_decimal(target_type):
        if source_type.precision > target_type.precision:
            result.add_error(
                f"Field '{field_name}': Converting decimal({source_type.precision}, "
                f"{source_type.scale}) to decimal({target_type.precision}, "
                f"{target_type.scale}) may lose precision"
            )
        if source_type.scale > target_type.scale:
            result.add_error(
                f"Field '{field_name}': Converting decimal scale from {source_type.scale} "
                f"to {target_type.scale} truncates decimal places"
            )


def _get_integer_bits(dtype: pa.DataType) -> Optional[int]:
    """Get bit width of integer type."""
    if pa.types.is_int8(dtype) or pa.types.is_uint8(dtype):
        return 8
    elif pa.types.is_int16(dtype) or pa.types.is_uint16(dtype):
        return 16
    elif pa.types.is_int32(dtype) or pa.types.is_uint32(dtype):
        return 32
    elif pa.types.is_int64(dtype) or pa.types.is_uint64(dtype):
        return 64
    return None


def _is_unsigned_int(dtype: pa.DataType) -> bool:
    """Check if type is unsigned integer."""
    return (pa.types.is_uint8(dtype) or pa.types.is_uint16(dtype) or
            pa.types.is_uint32(dtype) or pa.types.is_uint64(dtype))


def _is_signed_int(dtype: pa.DataType) -> bool:
    """Check if type is signed integer."""
    return (pa.types.is_int8(dtype) or pa.types.is_int16(dtype) or
            pa.types.is_int32(dtype) or pa.types.is_int64(dtype))


def _check_string_cast(
    source_type: pa.DataType,
    target_type: pa.DataType,
    field_name: str,
    result: CastValidationResult
) -> None:
    """
    Check if string/binary cast is safe.

    Validates:
    - Binary to string conversion (may not be valid UTF-8)
    - String to non-string conversion (except binary/large_string/large_binary)
    """
    if pa.types.is_binary(source_type) and pa.types.is_string(target_type):
        result.add_warning(
            f"Field '{field_name}': Converting binary to string assumes valid UTF-8 encoding"
        )

    # Check if converting string to something else
    if pa.types.is_string(source_type) or pa.types.is_large_string(source_type):
        # These conversions are safe
        if (pa.types.is_string(target_type) or
            pa.types.is_large_string(target_type) or
            pa.types.is_binary(target_type) or
            pa.types.is_large_binary(target_type)):
            return  # Safe conversion

        # All other conversions are unsafe
        result.add_error(
            f"Field '{field_name}': Converting string to {target_type} may lose data"
        )


def validate_cast(
    source_schema: pa.Schema,
    target_schema: pa.Schema
) -> CastValidationResult:
    """
    Validate that casting from source to target schema is safe.

    Checks each field for potential data loss, including:
    - Timestamp timezone and precision changes
    - Numeric downcasting and conversions
    - String/binary conversions
    - Type changes that may lose information

    Args:
        source_schema: Source PyArrow schema
        target_schema: Target PyArrow schema

    Returns:
        CastValidationResult with safety status and warnings/errors
    """
    result = CastValidationResult()

    # Build field lookup for target schema
    target_fields = {field.name: field for field in target_schema}

    for source_field in source_schema:
        field_name = source_field.name

        # Check if field exists in target
        if field_name not in target_fields:
            result.add_error(f"Field '{field_name}' exists in source but not in target schema")
            continue

        target_field = target_fields[field_name]
        source_type = source_field.type
        target_type = target_field.type

        # If types are identical, no cast needed
        if source_type == target_type:
            continue

        # Check timestamp casts
        _check_timestamp_cast(source_type, target_type, field_name, result)

        # Check numeric casts
        _check_numeric_cast(source_type, target_type, field_name, result)

        # Check string/binary casts
        _check_string_cast(source_type, target_type, field_name, result)

        # Generic type compatibility check
        if not _types_compatible(source_type, target_type):
            result.add_error(
                f"Field '{field_name}': Type {source_type} is not compatible with {target_type}"
            )

    # Check for fields in target that are missing from source
    source_field_names = {field.name for field in source_schema}
    for target_field in target_schema:
        if target_field.name not in source_field_names:
            # Missing fields will be filled with nulls - this is usually OK
            result.add_warning(
                f"Field '{target_field.name}' exists in target but not in source "
                f"(will be null)"
            )

    return result


def _types_compatible(source_type: pa.DataType, target_type: pa.DataType) -> bool:
    """
    Check if two types are compatible for casting.

    This is a broad check - more specific checks are done in the individual
    validation functions.
    """
    # Same type is always compatible
    if source_type == target_type:
        return True

    # Numeric types are generally compatible
    if pa.types.is_integer(source_type) and pa.types.is_integer(target_type):
        return True
    if pa.types.is_floating(source_type) and pa.types.is_floating(target_type):
        return True
    if pa.types.is_integer(source_type) and pa.types.is_floating(target_type):
        return True  # Int to float is safe

    # Temporal types with same base type
    if pa.types.is_timestamp(source_type) and pa.types.is_timestamp(target_type):
        return True
    if pa.types.is_date(source_type) and pa.types.is_date(target_type):
        return True
    if pa.types.is_time(source_type) and pa.types.is_time(target_type):
        return True

    # String and binary are interchangeable with caveats
    if pa.types.is_string(source_type) and pa.types.is_binary(target_type):
        return True
    if pa.types.is_binary(source_type) and pa.types.is_string(target_type):
        return True

    # Large variants
    if pa.types.is_string(source_type) and pa.types.is_large_string(target_type):
        return True
    if pa.types.is_large_string(source_type) and pa.types.is_string(target_type):
        return True
    if pa.types.is_binary(source_type) and pa.types.is_large_binary(target_type):
        return True
    if pa.types.is_large_binary(source_type) and pa.types.is_binary(target_type):
        return True

    # Decimal types
    if pa.types.is_decimal(source_type) and pa.types.is_decimal(target_type):
        return True

    # Otherwise incompatible
    return False


def cast_table_safe(
    table: pa.Table,
    target_schema: pa.Schema,
    strict: bool = True
) -> pa.Table:
    """
    Safely cast a PyArrow table to a target schema with validation.

    Args:
        table: Source PyArrow table
        target_schema: Target schema to cast to
        strict: If True, fail on any potential data loss. If False, only warn.

    Returns:
        Cast PyArrow table

    Raises:
        CastingError: If strict=True and cast would result in data loss
    """
    # Validate the cast
    validation = validate_cast(table.schema, target_schema)

    # Log warnings
    for warning in validation.warnings:
        logger.warning(f"Cast warning: {warning}")

    # In strict mode, fail if there are errors
    if strict and not validation.is_safe():
        error_msg = "Cannot cast table safely. Errors:\n" + "\n".join(validation.errors)
        if validation.warnings:
            error_msg += "\nWarnings:\n" + "\n".join(validation.warnings)
        raise CastingError(error_msg)

    # Log errors as warnings in non-strict mode
    if not strict:
        for error in validation.errors:
            logger.warning(f"Cast error (proceeding anyway due to strict=False): {error}")

    # Perform the cast
    logger.info(
        f"Casting table with {len(table)} rows from schema with {len(table.schema)} fields "
        f"to schema with {len(target_schema)} fields"
    )

    try:
        casted_table = table.cast(target_schema)
        logger.info("Cast completed successfully")
        return casted_table
    except pa.ArrowInvalid as e:
        raise CastingError(f"Cast failed during execution: {e}") from e
