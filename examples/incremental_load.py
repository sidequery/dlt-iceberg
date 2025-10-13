"""
Incremental Load Example

Demonstrates loading CSV files incrementally into an Iceberg table.
Each batch is appended to the table, creating a new snapshot per load.
"""

import dlt
import csv
from pathlib import Path
from dlt_iceberg import iceberg_rest


def load_csv(file_path: str):
    """Load CSV file and yield rows as dictionaries."""
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric fields
            row["event_id"] = int(row["event_id"])
            row["user_id"] = int(row["user_id"])
            row["value"] = int(row["value"])
            yield row


def main():
    # Setup paths
    examples_dir = Path(__file__).parent
    data_dir = examples_dir / "data"

    # Create dlt pipeline with Nessie REST catalog
    pipeline = dlt.pipeline(
        pipeline_name="incremental_example",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="examples",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="example_data",
    )

    print("Loading batch 1...")
    batch1_file = data_dir / "events_batch1.csv"

    @dlt.resource(name="events", write_disposition="append")
    def batch1():
        return load_csv(str(batch1_file))

    load_info = pipeline.run(batch1())
    print(f"Batch 1 loaded: {load_info}")

    print("\nLoading batch 2...")
    batch2_file = data_dir / "events_batch2.csv"

    @dlt.resource(name="events", write_disposition="append")
    def batch2():
        return load_csv(str(batch2_file))

    load_info = pipeline.run(batch2())
    print(f"Batch 2 loaded: {load_info}")

    # Query the table to verify
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "query",
        type="rest",
        uri="http://localhost:19120/iceberg/main",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
        },
    )

    table = catalog.load_table("examples.events")
    result = table.scan().to_arrow()

    print(f"\nTotal rows in table: {len(result)}")
    print(f"Event IDs: {sorted(result['event_id'].to_pylist())}")

    # TODO: Snapshot counting doesn't work reliably with Nessie - it may compact snapshots
    # Consider using a different catalog implementation (Polaris, Glue) for examples
    # snapshots = list(table.snapshots())
    # print(f"\nSnapshots: {len(snapshots)} (should be 2, one per load)")

    print("\nIncremental load complete!")


if __name__ == "__main__":
    main()
