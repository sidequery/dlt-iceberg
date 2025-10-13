"""
Schema conversion from dlt to Apache Iceberg.
"""

import logging
from typing import Dict
import pyarrow as pa
from dlt.common.schema import TTableSchema
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
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
)

logger = logging.getLogger(__name__)


def convert_dlt_to_iceberg_schema(
    dlt_table: TTableSchema, arrow_table: pa.Table
) -> Schema:
    """
    Convert a dlt table schema to an Iceberg schema.

    We use the PyArrow table to infer actual types since dlt may not have
    complete type information.

    Args:
        dlt_table: dlt table schema
        arrow_table: PyArrow table with actual data

    Returns:
        Iceberg Schema object
    """
    fields = []
    arrow_schema = arrow_table.schema

    # Build field ID counter
    field_id = 1

    # Get column information from dlt
    dlt_columns = dlt_table.get("columns", {})

    for arrow_field in arrow_schema:
        col_name = arrow_field.name
        arrow_type = arrow_field.type

        # Get dlt column metadata if available
        dlt_col = dlt_columns.get(col_name, {})
        nullable = dlt_col.get("nullable", True)
        required = not nullable

        # Convert PyArrow type to Iceberg type
        iceberg_type = convert_arrow_to_iceberg_type(arrow_type)

        # Create Iceberg field
        field = NestedField(
            field_id=field_id,
            name=col_name,
            field_type=iceberg_type,
            required=required,
        )
        fields.append(field)
        field_id += 1

    schema = Schema(*fields)
    logger.info(f"Converted schema with {len(fields)} fields")
    return schema


def convert_arrow_to_iceberg_type(arrow_type: pa.DataType):
    """
    Convert PyArrow data type to Iceberg type.

    Args:
        arrow_type: PyArrow data type

    Returns:
        Iceberg type
    """
    # Boolean
    if pa.types.is_boolean(arrow_type):
        return BooleanType()

    # Integers
    elif pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
        return IntegerType()
    elif pa.types.is_int32(arrow_type) or pa.types.is_uint8(arrow_type) or pa.types.is_uint16(arrow_type):
        return IntegerType()
    elif pa.types.is_int64(arrow_type) or pa.types.is_uint32(arrow_type) or pa.types.is_uint64(arrow_type):
        return LongType()

    # Floats
    elif pa.types.is_float32(arrow_type):
        return FloatType()
    elif pa.types.is_float64(arrow_type):
        return DoubleType()

    # Decimal
    elif pa.types.is_decimal(arrow_type):
        return DecimalType(
            precision=arrow_type.precision,
            scale=arrow_type.scale
        )

    # String
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return StringType()

    # Binary
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return BinaryType()

    # Temporal types
    elif pa.types.is_timestamp(arrow_type):
        return TimestampType()
    elif pa.types.is_date(arrow_type):
        return DateType()
    elif pa.types.is_time(arrow_type):
        return TimeType()

    # Complex types
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        element_type = convert_arrow_to_iceberg_type(arrow_type.value_type)
        return ListType(
            element_id=1,
            element_type=element_type,
            element_required=False,
        )

    elif pa.types.is_map(arrow_type):
        key_type = convert_arrow_to_iceberg_type(arrow_type.key_type)
        value_type = convert_arrow_to_iceberg_type(arrow_type.item_type)
        return MapType(
            key_id=1,
            key_type=key_type,
            value_id=2,
            value_type=value_type,
            value_required=False,
        )

    elif pa.types.is_struct(arrow_type):
        struct_fields = []
        for i, field in enumerate(arrow_type):
            field_type = convert_arrow_to_iceberg_type(field.type)
            struct_fields.append(
                NestedField(
                    field_id=i + 1,
                    name=field.name,
                    field_type=field_type,
                    required=not field.nullable,
                )
            )
        return StructType(*struct_fields)

    # Fallback to string for unknown types
    else:
        logger.warning(
            f"Unknown PyArrow type {arrow_type}, using StringType as fallback"
        )
        return StringType()


def convert_dlt_type_to_iceberg_type(dlt_type: str):
    """
    Convert dlt data type string to Iceberg type (fallback method).

    Args:
        dlt_type: dlt data type string

    Returns:
        Iceberg type
    """
    type_mapping = {
        "text": StringType(),
        "varchar": StringType(),
        "string": StringType(),
        "bigint": LongType(),
        "integer": IntegerType(),
        "int": IntegerType(),
        "smallint": IntegerType(),
        "double": DoubleType(),
        "float": FloatType(),
        "decimal": DecimalType(38, 9),
        "bool": BooleanType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestampType(),
        "date": DateType(),
        "time": TimeType(),
        "binary": BinaryType(),
        "json": StringType(),  # Store JSON as string
    }

    return type_mapping.get(dlt_type.lower(), StringType())
