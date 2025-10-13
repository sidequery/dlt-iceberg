"""Sidequery dlt Iceberg REST Catalog Destination"""

from .destination import iceberg_rest
from .schema_casting import CastingError

__all__ = ["iceberg_rest", "CastingError"]
