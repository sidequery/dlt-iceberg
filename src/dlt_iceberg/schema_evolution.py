"""
Schema evolution support for Apache Iceberg tables.

This module handles comparing schemas and applying safe schema changes:
- Adding new columns
- Promoting types (int→long, float→double)
- Detecting unsafe changes (dropping columns, narrowing types)
"""

import logging
from typing import Set, List, Dict, Tuple, Optional
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IcebergType,
    BooleanType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    BinaryType,
    TimestampType,
    DateType,
    TimeType,
    ListType,
    MapType,
    StructType,
    NestedField,
)

logger = logging.getLogger(__name__)


class SchemaEvolutionError(Exception):
    """Raised when an unsafe schema change is detected."""
    pass


def can_promote_type(from_type: IcebergType, to_type: IcebergType) -> bool:
    """
    Check if one Iceberg type can be safely promoted to another.

    Safe promotions (following Iceberg spec):
    - IntegerType → LongType
    - FloatType → DoubleType
    - DecimalType → DecimalType (with wider precision/scale)

    Args:
        from_type: Current type in existing schema
        to_type: New type in incoming schema

    Returns:
        True if promotion is safe, False otherwise
    """
    # Same type is always safe
    if type(from_type) == type(to_type):
        # For decimals, check precision/scale
        if isinstance(from_type, DecimalType) and isinstance(to_type, DecimalType):
            # Can widen precision and scale
            return (to_type.precision >= from_type.precision and
                    to_type.scale >= from_type.scale)
        return True

    # Integer to Long
    if isinstance(from_type, IntegerType) and isinstance(to_type, LongType):
        return True

    # Float to Double
    if isinstance(from_type, FloatType) and isinstance(to_type, DoubleType):
        return True

    # No other promotions are safe
    return False


def compare_schemas(
    existing_schema: Schema,
    new_schema: Schema
) -> Tuple[List[NestedField], List[Tuple[str, IcebergType, IcebergType]], List[str]]:
    """
    Compare two Iceberg schemas and identify differences.

    Args:
        existing_schema: Current schema of the table
        new_schema: New schema from incoming data

    Returns:
        Tuple of:
        - List of new fields to add
        - List of (field_name, old_type, new_type) for type changes
        - List of field names that were dropped (present in existing but not in new)
    """
    # Build name->field mappings
    existing_fields = {field.name: field for field in existing_schema.fields}
    new_fields = {field.name: field for field in new_schema.fields}

    existing_names = set(existing_fields.keys())
    new_names = set(new_fields.keys())

    # Find added columns
    added_names = new_names - existing_names
    added_fields = [new_fields[name] for name in added_names]

    # Find dropped columns
    dropped_names = existing_names - new_names

    # Find type changes in common columns
    common_names = existing_names & new_names
    type_changes = []

    for name in common_names:
        existing_field = existing_fields[name]
        new_field = new_fields[name]

        if type(existing_field.field_type) != type(new_field.field_type):
            # Types differ - could be promotion or unsafe change
            type_changes.append((name, existing_field.field_type, new_field.field_type))
        elif isinstance(existing_field.field_type, DecimalType) and isinstance(new_field.field_type, DecimalType):
            # Check decimal precision/scale changes
            if (existing_field.field_type.precision != new_field.field_type.precision or
                existing_field.field_type.scale != new_field.field_type.scale):
                type_changes.append((name, existing_field.field_type, new_field.field_type))

    return added_fields, type_changes, list(dropped_names)


def validate_schema_changes(
    added_fields: List[NestedField],
    type_changes: List[Tuple[str, IcebergType, IcebergType]],
    dropped_fields: List[str],
    allow_column_drops: bool = False
) -> None:
    """
    Validate that schema changes are safe to apply.

    Raises SchemaEvolutionError if any unsafe changes are detected.

    Args:
        added_fields: Fields being added
        type_changes: Type changes being made
        dropped_fields: Fields being dropped
        allow_column_drops: If False, raises error on dropped columns
    """
    errors = []

    # Check dropped columns
    if dropped_fields and not allow_column_drops:
        errors.append(
            f"Columns dropped (not safe): {', '.join(dropped_fields)}. "
            f"Dropping columns is not supported by default to prevent data loss."
        )

    # Check type changes
    for field_name, old_type, new_type in type_changes:
        if not can_promote_type(old_type, new_type):
            errors.append(
                f"Unsafe type change for column '{field_name}': "
                f"{old_type} → {new_type}. "
                f"Only safe promotions are allowed (int→long, float→double, decimal widening)."
            )

    if errors:
        raise SchemaEvolutionError(
            "Schema evolution validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        )


def apply_schema_evolution(
    table,
    added_fields: List[NestedField],
    type_changes: List[Tuple[str, IcebergType, IcebergType]]
) -> None:
    """
    Apply schema evolution changes to an Iceberg table.

    Args:
        table: PyIceberg table instance
        added_fields: New fields to add
        type_changes: Type promotions to apply
    """
    if not added_fields and not type_changes:
        logger.info("No schema changes to apply")
        return

    logger.info(
        f"Applying schema evolution: "
        f"{len(added_fields)} new columns, {len(type_changes)} type promotions"
    )

    # Apply changes using update_schema transaction
    with table.update_schema() as update:
        # Add new columns
        for field in added_fields:
            logger.info(f"  Adding column: {field.name} ({field.field_type})")
            update.add_column(
                path=field.name,
                field_type=field.field_type,
                required=field.required,
                doc=field.doc
            )

        # Apply type promotions
        for field_name, old_type, new_type in type_changes:
            logger.info(f"  Promoting column '{field_name}': {old_type} → {new_type}")
            update.update_column(
                path=field_name,
                field_type=new_type
            )

    logger.info("Schema evolution applied successfully")


def evolve_schema_if_needed(
    table,
    new_schema: Schema,
    allow_column_drops: bool = False
) -> bool:
    """
    Compare table schema with new schema and apply evolution if needed.

    This is the main entry point for schema evolution logic.

    Args:
        table: PyIceberg table instance
        new_schema: New schema from incoming data
        allow_column_drops: Whether to allow dropping columns

    Returns:
        True if schema was evolved, False if no changes needed

    Raises:
        SchemaEvolutionError: If unsafe schema changes are detected
    """
    existing_schema = table.schema()

    # Compare schemas
    added_fields, type_changes, dropped_fields = compare_schemas(
        existing_schema, new_schema
    )

    # Log what we found
    if added_fields:
        logger.info(f"Detected {len(added_fields)} new columns: {[f.name for f in added_fields]}")
    if type_changes:
        logger.info(f"Detected {len(type_changes)} type changes: {[(name, str(old), str(new)) for name, old, new in type_changes]}")
    if dropped_fields:
        logger.warning(f"Detected {len(dropped_fields)} dropped columns: {dropped_fields}")

    # If no changes, nothing to do
    if not added_fields and not type_changes and not dropped_fields:
        logger.debug("No schema changes detected")
        return False

    # Validate changes are safe
    validate_schema_changes(added_fields, type_changes, dropped_fields, allow_column_drops)

    # Apply evolution
    apply_schema_evolution(table, added_fields, type_changes)

    return True
