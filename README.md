# dlt-iceberg

A [dlt](https://dlthub.com/) destination for [Apache Iceberg](https://iceberg.apache.org/) tables using REST catalogs.

## Features

- **Atomic Multi-File Commits**: Multiple parquet files committed as single Iceberg snapshot per table
- **REST Catalog Support**: Works with Nessie, Polaris, AWS Glue, Unity Catalog
- **Partitioning**: Full support for Iceberg partition transforms (temporal, bucket, truncate, identity)
- **Authentication**: OAuth2, Bearer token, AWS SigV4
- **Write Dispositions**: Append, replace, merge (upsert)
- **Schema Evolution**: Automatic schema updates when adding columns
- **Retry Logic**: Exponential backoff for transient failures

## Installation

```bash
pip install dlt-iceberg
```

Or with uv:

```bash
uv add dlt-iceberg
```

## Quick Start

See [examples/](examples/) directory for working examples.

### Incremental Load

```python
import dlt
from dlt_iceberg import iceberg_rest

@dlt.resource(name="events", write_disposition="append")
def generate_events():
    yield {"event_id": 1, "value": 100}

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=iceberg_rest(
        catalog_uri="http://localhost:19120/iceberg/main",
        namespace="analytics",
        s3_endpoint="http://localhost:9000",
        s3_access_key_id="minioadmin",
        s3_secret_access_key="minioadmin",
        s3_region="us-east-1",
    ),
)

pipeline.run(generate_events())
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

## Configuration Options

All configuration options can be passed to `iceberg_rest()`:

```python
iceberg_rest(
    catalog_uri="...",           # Required: REST catalog URI
    namespace="...",             # Required: Iceberg namespace (database)
    warehouse="...",             # Optional: Warehouse location

    # Authentication
    credential="...",            # OAuth2 client credentials
    oauth2_server_uri="...",     # OAuth2 token endpoint
    token="...",                 # Bearer token

    # AWS SigV4
    sigv4_enabled=True,
    signing_region="us-east-1",

    # S3 configuration
    s3_endpoint="...",
    s3_access_key_id="...",
    s3_secret_access_key="...",
    s3_region="...",

    # Performance tuning
    max_retries=5,               # Retry attempts for transient failures
    retry_backoff_base=2.0,      # Exponential backoff multiplier
    merge_batch_size=500000,     # Rows per batch for merge operations
    strict_casting=False,        # Fail on potential data loss
)
```

### Nessie (Docker)

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

Start services: `docker compose up -d`

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

AWS credentials via environment variables.

### Polaris

```python
iceberg_rest(
    catalog_uri="https://polaris.example.com/api/catalog",
    warehouse="s3://bucket/warehouse",
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

## Partitioning

Mark columns for partitioning using dlt column hints:

```python
@dlt.resource(
    name="events",
    columns={
        "event_date": {
            "data_type": "date",
            "partition": True,
            "partition_transform": "day",  # Optional: year, month, day, hour
        },
        "region": {
            "data_type": "text",
            "partition": True,  # Uses identity transform for strings
        },
        "user_id": {
            "data_type": "bigint",
            "partition": True,
            "partition_transform": "bucket[10]",  # Hash into 10 buckets
        }
    }
)
def events():
    ...
```

### Available Transforms

- **Temporal**: `year`, `month`, `day`, `hour` (for timestamp/date columns)
- **Identity**: No transformation (default for string/integer)
- **Bucket**: `bucket[N]` - Hash-based partitioning into N buckets
- **Truncate**: `truncate[N]` - Truncate strings/integers to N width

### Default Behavior

If `partition_transform` is not specified:
- Timestamp/date columns default to `month`
- String/integer columns default to `identity`

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
```python
write_disposition="merge"
primary_key="user_id"
```
Updates existing rows by primary key, inserts new rows.

## Development

### Run Tests

```bash
# Start Docker services
docker compose up -d

# Run all tests
uv run pytest tests/ -v

# Run only unit tests
uv run pytest tests/ -v -m "not integration"

# Run only integration tests
uv run pytest tests/ -v -m integration
```

### Project Structure

```
dlt-iceberg/
├── src/dlt_iceberg/
│   ├── __init__.py              # Public API
│   ├── destination_client.py   # Class-based destination (atomic commits)
│   ├── destination.py           # Function-based destination (legacy)
│   ├── schema_converter.py     # dlt → Iceberg schema conversion
│   ├── schema_casting.py        # Arrow table casting
│   ├── schema_evolution.py     # Schema updates
│   ├── partition_builder.py    # Partition specs
│   └── error_handling.py       # Retry logic
├── tests/
│   ├── test_destination_rest_catalog.py  # Integration tests (Docker)
│   ├── test_class_based_atomic.py        # Atomic commit tests
│   ├── test_merge_disposition.py
│   ├── test_schema_evolution.py
│   └── ...
├── examples/
│   ├── incremental_load.py     # CSV incremental loading
│   ├── merge_load.py            # CSV merge/upsert
│   └── data/                    # Sample CSV files
└── docker-compose.yml           # Nessie + MinIO for testing
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
