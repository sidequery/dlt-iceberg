"""Sidequery dlt Iceberg REST Catalog Destination"""

# Class-based destination with atomic multi-file commits (RECOMMENDED)
from .destination_client import (
    iceberg_rest_class_based,
    IcebergRestClient,
    IcebergRestConfiguration,
)

# Function-based destination (legacy, per-file commits)
from .destination import iceberg_rest as iceberg_rest_function_based

# Export the class-based version as the primary destination
iceberg_rest = iceberg_rest_class_based

# Errors
from .schema_casting import CastingError
from .schema_evolution import SchemaEvolutionError

__all__ = [
    "iceberg_rest",
    "iceberg_rest_class_based",
    "iceberg_rest_function_based",
    "IcebergRestClient",
    "IcebergRestConfiguration",
    "CastingError",
    "SchemaEvolutionError",
]
