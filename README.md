# dlt-iceberg

A [dlt](https://dlthub.com/) destination for [Apache Iceberg](https://iceberg.apache.org/) tables using REST catalogs.

## Features

- **Atomic Multi-File Commits**: Multiple parquet files committed as single Iceberg snapshot per table
- **REST Catalog Support**: Works with Nessie, Polaris, AWS Glue, Unity Catalog
- **Credential Vending**: Most REST catalogs vend storage credentials automatically
- **Partitioning**: Full support for Iceberg partition transforms via `iceberg_adapter()`
- **Merge Strategies**: Delete-insert and upsert with hard delete support
- **DuckDB Integration**: Query loaded data via `pipeline.dataset()`
- **Schema Evolution**: Automatic schema updates when adding columns

## Installation

```bash
pip install dlt-iceberg
```

Or with uv:

```bash
uv add dlt-iceberg
```

## Quick Start

```python
import dlt
from dlt_iceberg import iceberg_rest

@dlt.resource(name="events", write_disposition="append")
def generate_events():
    yield {"event_id": 1, "value": 100}

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=iceberg_rest(
        catalog_uri="https://my-catalog.example.com/api/catalog",
        namespace="analytics",
        warehouse="my_warehouse",
        credential="client-id:client-secret",
        oauth2_server_uri="https://my-catalog.example.com/oauth/tokens",
    ),
)

pipeline.run(generate_events())
```

### Query Loaded Data

```python
# Query data via DuckDB
dataset = pipeline.dataset()

# Access as dataframe
df = dataset["events"].df()

# Run SQL queries
result = dataset.query("SELECT * FROM events WHERE value > 50").fetchall()

# Get Arrow table
arrow_table = dataset["events"].arrow()
```

### Merge/Upsert

```python
@dlt.resource(
    name="users",
    write_disposition="merge",
    primary_key="user_id"
)
def generate_users():
    yield {"user_id": 1, "name": "Alice", "status": "active"}

pipeline.run(generate_users())
```

## Configuration

### Required Options

```python
iceberg_rest(
    catalog_uri="...",    # REST catalog endpoint (or sqlite:// for local)
    namespace="...",      # Iceberg namespace (database)
)
```

### Authentication

Choose based on your catalog:

| Catalog | Auth Method |
|---------|-------------|
| Polaris, Lakekeeper | `credential` + `oauth2_server_uri` |
| Unity Catalog | `token` |
| AWS Glue | `sigv4_enabled` + `signing_region` |
| Local SQLite | None needed |

Most REST catalogs (Polaris, Lakekeeper, etc.) **vend storage credentials automatically** via the catalog API. You typically don't need to configure S3/GCS/Azure credentials manually.

<details>
<summary><b>Advanced Options</b></summary>

```python
iceberg_rest(
    # ... required options ...

    # Manual storage credentials (usually not needed with credential vending)
    s3_endpoint="...",
    s3_access_key_id="...",
    s3_secret_access_key="...",
    s3_region="...",

    # Performance tuning
    max_retries=5,               # Retry attempts for transient failures
    retry_backoff_base=2.0,      # Exponential backoff multiplier
    merge_batch_size=500000,     # Rows per batch for merge operations
    strict_casting=False,        # Fail on potential data loss

    # Table management
    table_location_layout=None,  # Custom table location pattern
    register_new_tables=False,   # Register tables found in storage
    hard_delete_column="_dlt_deleted_at",  # Column for hard deletes
)
```

</details>

## Catalog Examples

<details>
<summary><b>Lakekeeper (Docker)</b></summary>

```python
iceberg_rest(
    catalog_uri="http://localhost:8282/catalog/",
    warehouse="test-warehouse",
    namespace="my_namespace",
    s3_endpoint="http://localhost:9000",
    s3_access_key_id="minioadmin",
    s3_secret_access_key="minioadmin",
    s3_region="us-east-1",
)
```

Start Lakekeeper + MinIO with `docker compose up -d`. Lakekeeper supports credential vending in production.

</details>

<details>
<summary><b>Polaris</b></summary>

```python
iceberg_rest(
    catalog_uri="https://polaris.example.com/api/catalog",
    warehouse="my_warehouse",
    namespace="production",
    credential="client-id:client-secret",
    oauth2_server_uri="https://polaris.example.com/api/catalog/v1/oauth/tokens",
)
```

Storage credentials are vended automatically by the catalog.

</details>

<details>
<summary><b>Unity Catalog (Databricks)</b></summary>

```python
iceberg_rest(
    catalog_uri="https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
    warehouse="<catalog-name>",
    namespace="<schema-name>",
    token="<databricks-token>",
)
```

</details>

<details>
<summary><b>AWS Glue</b></summary>

```python
iceberg_rest(
    catalog_uri="https://glue.us-east-1.amazonaws.com/iceberg",
    warehouse="<account-id>:s3tablescatalog/<bucket>",
    namespace="my_database",
    sigv4_enabled=True,
    signing_region="us-east-1",
)
```

Requires AWS credentials in environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).

</details>

<details>
<summary><b>Local SQLite Catalog</b></summary>

```python
iceberg_rest(
    catalog_uri="sqlite:///catalog.db",
    warehouse="file:///path/to/warehouse",
    namespace="my_namespace",
)
```

Great for local development and testing.

</details>

<details>
<summary><b>Nessie (Docker)</b></summary>

```python
iceberg_rest(
    catalog_uri="http://localhost:19120/iceberg/main",
    namespace="my_namespace",
    s3_endpoint="http://localhost:9000",
    s3_access_key_id="minioadmin",
    s3_secret_access_key="minioadmin",
    s3_region="us-east-1",
)
```

Start Nessie + MinIO with `docker compose up -d` (see docker-compose.yml in repo).

</details>

## Partitioning

### Using iceberg_adapter (Recommended)

The `iceberg_adapter` function provides a clean API for configuring Iceberg partitioning:

```python
from dlt_iceberg import iceberg_adapter, iceberg_partition

@dlt.resource(name="events")
def events():
    yield {"event_date": "2024-01-01", "user_id": 123, "region": "US"}

# Single partition
adapted = iceberg_adapter(events, partition="region")

# Multiple partitions with transforms
adapted = iceberg_adapter(
    events,
    partition=[
        iceberg_partition.month("event_date"),
        iceberg_partition.bucket(10, "user_id"),
        "region",  # identity partition
    ]
)

pipeline.run(adapted)
```

### Partition Transforms

```python
# Temporal transforms (for timestamp/date columns)
iceberg_partition.year("created_at")
iceberg_partition.month("created_at")
iceberg_partition.day("created_at")
iceberg_partition.hour("created_at")

# Identity (no transformation)
iceberg_partition.identity("region")

# Bucket (hash into N buckets)
iceberg_partition.bucket(10, "user_id")

# Truncate (truncate to width)
iceberg_partition.truncate(4, "email")

# Custom partition field names
iceberg_partition.month("created_at", "event_month")
iceberg_partition.bucket(8, "user_id", "user_bucket")
```

### Using Column Hints

You can also use dlt column hints for partitioning:

```python
@dlt.resource(
    name="events",
    columns={
        "event_date": {
            "data_type": "date",
            "partition": True,
            "partition_transform": "day",
        },
        "user_id": {
            "data_type": "bigint",
            "partition": True,
            "partition_transform": "bucket[10]",
        }
    }
)
def events():
    ...
```

## Write Dispositions

### Append
```python
write_disposition="append"
```
Adds new data without modifying existing rows.

### Replace
```python
write_disposition="replace"
```
Truncates table and inserts new data.

### Merge

#### Delete-Insert Strategy (Default)
```python
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    primary_key="user_id"
)
```
Deletes matching rows then inserts new data. Single atomic transaction.

#### Upsert Strategy
```python
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "upsert"},
    primary_key="user_id"
)
```
Updates existing rows, inserts new rows.

#### Hard Deletes

Mark rows for deletion by setting the `_dlt_deleted_at` column:

```python
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    primary_key="user_id"
)
def users_with_deletes():
    from datetime import datetime
    yield {"user_id": 1, "name": "alice", "_dlt_deleted_at": None}  # Keep
    yield {"user_id": 2, "name": "bob", "_dlt_deleted_at": datetime.now()}  # Delete
```

## Development

### Run Tests

```bash
# Start Docker services (for Nessie tests)
docker compose up -d

# Run all tests
uv run pytest tests/ -v

# Run only unit tests (no Docker required)
uv run pytest tests/ --ignore=tests/nessie -v

# Run Nessie integration tests
uv run pytest tests/nessie/ -v
```

### Project Structure

```
dlt-iceberg/
├── src/dlt_iceberg/
│   ├── __init__.py           # Public API
│   ├── destination_client.py # Class-based destination (atomic commits)
│   ├── destination.py        # Function-based destination (legacy)
│   ├── adapter.py            # iceberg_adapter() for partitioning
│   ├── sql_client.py         # DuckDB integration for dataset()
│   ├── schema_converter.py   # dlt → Iceberg schema conversion
│   ├── schema_casting.py     # Arrow table casting
│   ├── schema_evolution.py   # Schema updates
│   ├── partition_builder.py  # Partition specs
│   └── error_handling.py     # Retry logic
├── tests/
│   ├── test_adapter.py       # iceberg_adapter tests
│   ├── test_capabilities.py  # Hard delete, partition names tests
│   ├── test_dataset.py       # DuckDB integration tests
│   ├── test_merge_disposition.py
│   ├── test_schema_evolution.py
│   └── ...
├── examples/
│   ├── incremental_load.py   # CSV incremental loading
│   ├── merge_load.py         # CSV merge/upsert
│   └── data/                 # Sample CSV files
└── docker-compose.yml        # Nessie + MinIO for testing
```

## How It Works

The class-based destination uses dlt's `JobClientBase` interface to accumulate parquet files during a load and commit them atomically in `complete_load()`:

1. dlt extracts data and writes parquet files
2. Each file is registered in module-level global state
3. After all files complete, `complete_load()` is called
4. All files for a table are combined and committed as single Iceberg snapshot
5. Each table gets one snapshot per load

This ensures atomic commits even though dlt creates multiple client instances.

## License

MIT License - see LICENSE file

## Resources

- [dlt Documentation](https://dlthub.com/docs)
- [Apache Iceberg](https://iceberg.apache.org/)
- [PyIceberg](https://py.iceberg.apache.org/)
- [Iceberg REST Spec](https://iceberg.apache.org/rest-catalog-spec/)
