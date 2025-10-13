"""
Merge Load Example

Demonstrates loading two CSV files with overlapping customer_id keys using merge disposition.
The second file updates existing customers and adds new ones.
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
            row["customer_id"] = int(row["customer_id"])
            yield row


def main():
    # Setup paths
    examples_dir = Path(__file__).parent
    data_dir = examples_dir / "data"

    # Create dlt pipeline with Nessie REST catalog
    pipeline = dlt.pipeline(
        pipeline_name="merge_example",
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

    print("Loading initial customers...")
    initial_file = data_dir / "customers_initial.csv"

    @dlt.resource(name="customers", write_disposition="merge", primary_key="customer_id")
    def initial_customers():
        return load_csv(str(initial_file))

    load_info = pipeline.run(initial_customers())
    print(f"Initial load complete: {load_info}")

    # Query to see initial state
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

    table = catalog.load_table("examples.customers")
    result = table.scan().to_arrow()
    print(f"\nAfter initial load: {len(result)} customers")
    print(result.to_pandas()[["customer_id", "name", "status"]])

    print("\n" + "="*60)
    print("Loading customer updates...")
    print("This will:")
    print("  - Update customer_id 2 (Bob): email and status change")
    print("  - Update customer_id 3 (Carol): status change")
    print("  - Add customer_id 6 (Frank): new customer")
    print("  - Add customer_id 7 (Grace): new customer")
    print("="*60 + "\n")

    updates_file = data_dir / "customers_updates.csv"

    @dlt.resource(name="customers", write_disposition="merge", primary_key="customer_id")
    def updated_customers():
        return load_csv(str(updates_file))

    load_info = pipeline.run(updated_customers())
    print(f"Updates loaded: {load_info}")

    # Query to see merged state
    table = catalog.load_table("examples.customers")
    result = table.scan().to_arrow()
    print(f"\nAfter merge: {len(result)} customers")
    print(result.to_pandas()[["customer_id", "name", "email", "status"]].sort_values("customer_id"))

    # TODO: Snapshot counting doesn't work reliably with Nessie - it may compact snapshots
    # Consider using a different catalog implementation (Polaris, Glue) for examples
    # snapshots = list(table.snapshots())
    # print(f"\nSnapshots: {len(snapshots)} (should be 2, one per load)")

    print("\nMerge complete!")


if __name__ == "__main__":
    main()
