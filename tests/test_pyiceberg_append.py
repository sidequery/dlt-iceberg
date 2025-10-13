"""
REAL TEST: Uses PyIceberg Table.append() API.
This is what our destination actually calls.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, TimestampType
from pyiceberg.table import TableProperties
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.catalog import load_catalog
import json


def test_pyiceberg_table_append():
    """
    REAL TEST: Creates a Table object and calls append().
    This is the exact API our destination uses.
    """
    temp_dir = tempfile.mkdtemp()
    warehouse_path = f"{temp_dir}/warehouse"

    print(f"\n🏗️  Warehouse path: {warehouse_path}")

    try:
        # Create Iceberg schema
        schema = Schema(
            NestedField(1, "event_id", LongType(), required=True),
            NestedField(2, "event_type", StringType(), required=False),
            NestedField(3, "event_timestamp", TimestampType(), required=True),
            NestedField(4, "value", LongType(), required=False),
        )

        print(f"✅ Created Iceberg schema")

        # Create a filesystem catalog
        catalog = load_catalog(
            "local",
            **{
                "type": "sql",
                "uri": f"sqlite:///{temp_dir}/catalog.db",
                "warehouse": f"file://{warehouse_path}",
            }
        )

        print(f"✅ Created catalog")

        # Create namespace
        catalog.create_namespace("test")
        print(f"✅ Created namespace 'test'")

        # Create table via catalog
        table = catalog.create_table(
            identifier="test.events",
            schema=schema,
        )

        print(f"✅ Created Table object via catalog")
        print(f"   Location: {table.location()}")

        # Create test data with explicit schema to match required fields
        base_time = datetime(2024, 1, 1)
        arrow_schema = pa.schema([
            pa.field("event_id", pa.int64(), nullable=False),
            pa.field("event_type", pa.string(), nullable=True),
            pa.field("event_timestamp", pa.timestamp("us"), nullable=False),
            pa.field("value", pa.int64(), nullable=True),
        ])
        data = {
            "event_id": list(range(1, 11)),
            "event_type": [f"type_{i % 3}" for i in range(10)],
            "event_timestamp": [base_time + timedelta(hours=i) for i in range(10)],
            "value": [i * 10 for i in range(10)],
        }
        arrow_table = pa.table(data, schema=arrow_schema)

        print(f"✅ Created test data: {len(arrow_table)} rows")

        # THE KEY OPERATION: Call table.append()
        print(f"\n✍️  Calling table.append()...")
        table.append(arrow_table)

        print(f"✅ APPEND SUCCEEDED!")

        # Read back the data
        print(f"\n🔍 Reading data back...")
        scan = table.scan()
        result = scan.to_arrow()

        print(f"✅ Read {len(result)} rows")

        # Verify
        assert len(result) == 10, f"Expected 10 rows, got {len(result)}"

        df = result.to_pandas()
        assert df["event_id"].min() == 1
        assert df["event_id"].max() == 10

        print(f"✅ Data verified!")
        print(f"\nSample data:")
        print(df.head())

        # Test incremental append
        print(f"\n📦 Testing incremental append...")

        more_data = {
            "event_id": list(range(11, 16)),
            "event_type": [f"type_{i % 3}" for i in range(5)],
            "event_timestamp": [base_time + timedelta(hours=10+i) for i in range(5)],
            "value": [(10+i) * 10 for i in range(5)],
        }
        arrow_table2 = pa.table(more_data, schema=arrow_schema)

        table.append(arrow_table2)

        print(f"✅ Second append succeeded")

        # Read all data
        result2 = table.scan().to_arrow()
        assert len(result2) == 15

        print(f"✅ Total rows: {len(result2)}")

        print(f"\n🎉 SUCCESS! PyIceberg Table.append() WORKS!")
        print(f"\n Summary:")
        print(f"   ✅ Created Table object with metadata")
        print(f"   ✅ Called table.append() with 10 rows")
        print(f"   ✅ Read and verified data")
        print(f"   ✅ Called table.append() again with 5 rows")
        print(f"   ✅ Read all 15 rows successfully")
        print(f"\n THIS IS THE EXACT API OUR DESTINATION USES!")

    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
