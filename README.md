# dlt-iceberg

A [dlt](https://dlthub.com/) destination for [Apache Iceberg](https://iceberg.apache.org/) tables using REST catalogs.

## Features

- **Atomic Multi-File Commits**: Multiple parquet files committed as single Iceberg snapshot
- **REST Catalog Support**: Polaris, Lakekeeper, Unity Catalog, AWS Glue, Nessie
- **Credential Vending**: Works with catalogs that vend storage credentials automatically
- **Partitioning**: Full support for Iceberg partition transforms
- **Merge Strategies**: Delete-insert and upsert with hard delete support
- **DuckDB Integration**: Query loaded data via `pipeline.dataset()`

## Installation

```bash
pip install dlt-iceberg
```

## Quick Start

```python
import dlt
from dlt_iceberg import iceberg_rest

@dlt.resource(name="events")
def my_events():
    yield {"event_id": 1, "value": 100}

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=iceberg_rest(
        catalog_uri="https://my-catalog.example.com/api/catalog",
        warehouse="my_warehouse",
        namespace="analytics",
        credential="client-id:client-secret",
        oauth2_server_uri="https://my-catalog.example.com/oauth/tokens",
    ),
)

pipeline.run(my_events())
```

Most REST catalogs vend storage credentials automatically, so you typically only need catalog auth.

### Query Loaded Data

```python
dataset = pipeline.dataset()
df = dataset["events"].df()
result = dataset.query("SELECT * FROM events WHERE value > 50").fetchall()
```

## Catalog Examples

### Polaris / Lakekeeper

```python
iceberg_rest(
    catalog_uri="https://polaris.example.com/api/catalog",
    warehouse="my_warehouse",
    namespace="production",
    credential="client-id:client-secret",
    oauth2_server_uri="https://polaris.example.com/api/catalog/v1/oauth/tokens",
)
```

### Unity Catalog

```python
iceberg_rest(
    catalog_uri="https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
    warehouse="<catalog-name>",
    namespace="<schema-name>",
    token="<databricks-token>",
)
```

### AWS Glue

```python
iceberg_rest(
    catalog_uri="https://glue.us-east-1.amazonaws.com/iceberg",
    warehouse="<account-id>:s3tablescatalog/<bucket>",
    namespace="my_database",
    sigv4_enabled=True,
    signing_region="us-east-1",
)
```

Requires AWS credentials in environment.

### Local Development (SQLite)

```python
iceberg_rest(
    catalog_uri="sqlite:///catalog.db",
    warehouse="file:///path/to/warehouse",
    namespace="dev",
)
```

## Partitioning

Use `iceberg_adapter` to configure partitions:

```python
from dlt_iceberg import iceberg_adapter, iceberg_partition

@dlt.resource(name="events")
def events():
    yield {"event_date": "2024-01-01", "user_id": 123, "region": "US"}

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

### Available Transforms

```python
iceberg_partition.year("ts")           # Extract year
iceberg_partition.month("ts")          # Extract month
iceberg_partition.day("ts")            # Extract day
iceberg_partition.hour("ts")           # Extract hour
iceberg_partition.identity("region")   # Use value as-is
iceberg_partition.bucket(10, "id")     # Hash into N buckets
iceberg_partition.truncate(4, "str")   # Truncate to width
```

## Merge

```python
@dlt.resource(
    write_disposition="merge",
    primary_key="user_id"
)
def users():
    yield {"user_id": 1, "name": "Alice"}
```

### Strategies

- **delete-insert** (default): Deletes matching rows, then inserts. Single atomic transaction.
- **upsert**: Updates existing rows, inserts new ones.

```python
write_disposition={"disposition": "merge", "strategy": "delete-insert"}
write_disposition={"disposition": "merge", "strategy": "upsert"}
```

### Hard Deletes

Set `_dlt_deleted_at` to mark rows for deletion:

```python
yield {"user_id": 1, "name": "alice", "_dlt_deleted_at": None}        # Keep
yield {"user_id": 2, "name": "bob", "_dlt_deleted_at": datetime.now()} # Delete
```

## Configuration Reference

```python
iceberg_rest(
    # Required
    catalog_uri="...",              # Catalog REST endpoint or SQLite URI
    namespace="...",                # Iceberg namespace

    # Catalog auth (pick one)
    credential="...",               # OAuth2 client credentials
    oauth2_server_uri="...",        # OAuth2 token endpoint
    token="...",                    # Bearer token

    # Optional
    warehouse="...",                # Warehouse name/location

    # AWS SigV4 (for Glue)
    sigv4_enabled=True,
    signing_region="us-east-1",

    # Manual S3 credentials (usually not needed - catalogs vend these)
    s3_endpoint="...",
    s3_access_key_id="...",
    s3_secret_access_key="...",
    s3_region="...",

    # Advanced
    merge_batch_size=500000,        # Rows per upsert batch
    table_location_layout=None,     # Custom table paths
)
```

## Development

```bash
# Run tests (no Docker needed)
uv run pytest tests/ --ignore=tests/nessie -v

# Run with Nessie integration
docker compose up -d
uv run pytest tests/ -v
```

## License

MIT
