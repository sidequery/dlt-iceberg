# dlt-iceberg - Iceberg REST Catalog Destination

A custom [dlt](https://dlthub.com/) destination for writing data to [Apache Iceberg](https://iceberg.apache.org/) tables using REST catalog implementations (Polaris, Unity Catalog, AWS Glue, Nessie, etc.).

## Features

- **REST Catalog Support**: Works with any Iceberg REST catalog (Polaris, Unity, Glue, Nessie, Lakekeeper)
- **Multiple Authentication Methods**: OAuth2, Bearer token, AWS SigV4
- **Automatic Schema Conversion**: dlt schema → Iceberg schema with proper type mapping
- **Partition Support**: Automatically creates partitioned tables from dlt hints
- **Write Dispositions**: Append, replace, and merge operations
- **Retry Logic**: Built-in exponential backoff for commit conflicts
- **Type Safety**: Full PyArrow and Iceberg type support including complex types

## Installation

```bash
# Clone and install
git clone <repository-url>
cd dlt-iceberg
uv sync
```

## Quick Start

### 1. Configure Secrets

Copy the example secrets file:

```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Edit `.dlt/secrets.toml` with your catalog credentials:

```toml
[destination.iceberg_rest]
catalog_uri = "https://polaris.example.com/api/catalog"
warehouse = "s3://my-bucket/warehouse"

# OAuth2 (recommended)
credential = "client-id:client-secret"
oauth2_server_uri = "https://polaris.example.com/api/catalog/v1/oauth/tokens"

# OR Bearer token
# token = "your-bearer-token"
```

### 2. Create a Pipeline

```python
import dlt
from datetime import datetime
from dlt_iceberg import iceberg_rest

@dlt.resource(
    name="events",
    write_disposition="append",
    columns={
        "event_timestamp": {
            "data_type": "timestamp",
            "partition": True,  # Partition by month
            "nullable": False,
        }
    }
)
def generate_events():
    yield {
        "event_id": 1,
        "user_id": 42,
        "event_type": "click",
        "event_timestamp": datetime.now(),
    }

# Create pipeline
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=iceberg_rest,
    dataset_name="analytics",  # Not used (namespace set in config)
)

# Run pipeline
load_info = pipeline.run(generate_events())
print(f"Loaded {len(load_info.loads_ids)} packages")
```

### 3. Run

```bash
uv run examples/basic_pipeline.py
```

## Configuration

### Catalog Settings

Configure in `.dlt/config.toml`:

```toml
[destination.iceberg_rest]
namespace = "production"  # Iceberg namespace (database)
max_retries = 5
retry_backoff_base = 2.0
```

### Supported Catalogs

#### Apache Polaris (Local)
```toml
[destination.iceberg_rest]
catalog_uri = "http://localhost:8181/api/catalog"
warehouse = "file:///tmp/warehouse"
token = "principal:root;realm:default-realm"
```

#### AWS Glue REST Catalog
```toml
[destination.iceberg_rest]
catalog_uri = "https://glue.us-east-1.amazonaws.com/iceberg"
warehouse = "<account-id>:s3tablescatalog/<bucket>"
sigv4_enabled = true  # Set in config.toml
signing_region = "us-east-1"
```

Then set AWS credentials via environment:
```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

#### Unity Catalog (Databricks)
```toml
[destination.iceberg_rest]
catalog_uri = "https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest"
warehouse = "<catalog-name>"
token = "<databricks-token>"
```

## Partitioning

Mark columns for partitioning in dlt resource hints:

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
            "partition": True,  # Identity transform for strings
        }
    }
)
def events():
    ...
```

**Transform Selection**:
- `timestamp`/`date` columns: Default to `month` transform (specify `day`, `hour`, `year` for others)
- `string`/`integer` columns: Use `identity` transform
- Custom: Set `partition_transform` in column hints

## Write Dispositions

### Append (Default)
```python
@dlt.resource(write_disposition="append")
def events():
    ...
```
Adds new data without modifying existing rows.

### Replace
```python
@dlt.resource(write_disposition="replace")
def snapshot_table():
    ...
```
Overwrites entire table.

### Merge/Upsert
```python
@dlt.resource(
    write_disposition="merge",
    primary_key="user_id"
)
def users():
    ...
```
Updates existing rows based on primary key, inserts new rows.

## Schema Conversion

Automatic type mapping from dlt/PyArrow to Iceberg:

| PyArrow Type | Iceberg Type |
|--------------|--------------|
| `bool_` | `BooleanType` |
| `int32`, `int64` | `IntegerType`, `LongType` |
| `float32`, `float64` | `FloatType`, `DoubleType` |
| `string` | `StringType` |
| `timestamp` | `TimestampType` |
| `date` | `DateType` |
| `list_` | `ListType` |
| `struct` | `StructType` |

Complex types (lists, structs) are fully supported.

## Development

### Run Tests

```bash
uv run pytest tests/ -v
```

### Project Structure

```
dlt-iceberg/
├── src/dlt_iceberg/
│   ├── __init__.py
│   ├── destination.py          # Main destination implementation
│   ├── schema_converter.py     # dlt → Iceberg schema conversion
│   └── partition_builder.py    # Partition spec builder
├── tests/
│   ├── test_schema_converter.py
│   └── test_partition_builder.py
├── examples/
│   └── basic_pipeline.py
├── .dlt/
│   ├── config.toml
│   └── secrets.toml.example
└── pyproject.toml
```

## Advanced Usage

### Custom Retry Configuration

```python
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=iceberg_rest(
        max_retries=10,
        retry_backoff_base=1.5,
    )
)
```

### Multiple Namespaces

```python
# Production namespace
pipeline_prod = dlt.pipeline(
    destination=iceberg_rest(namespace="production")
)

# Staging namespace
pipeline_staging = dlt.pipeline(
    destination=iceberg_rest(namespace="staging")
)
```

### Monitoring

The destination logs all operations at INFO level:

```python
import logging
logging.basicConfig(level=logging.INFO)
```

Output:
```
INFO:dlt_iceberg.destination:Connecting to REST catalog at https://...
INFO:dlt_iceberg.destination:Processing table production.events with disposition append
INFO:dlt_iceberg.destination:Read 1000 rows from /path/to/file.parquet
INFO:dlt_iceberg.destination:Appending to table production.events
INFO:dlt_iceberg.destination:Successfully wrote 1000 rows to production.events
```

## How It Works

1. **dlt Extract & Normalize**: dlt extracts data from sources and normalizes to Parquet
2. **Custom Destination**: Destination receives Parquet file paths
3. **Schema Conversion**: Converts dlt schema to Iceberg schema
4. **Table Operations**: Uses PyIceberg to create/update tables via REST catalog
5. **Data Write**: Writes PyArrow data to Iceberg with retry logic

```
┌─────────────────┐
│  dlt pipeline   │
│ (extract/norm)  │
└────────┬────────┘
         │ Parquet files
         ▼
┌─────────────────┐
│ Custom          │
│ Destination     │
│ @dlt.destination│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PyIceberg     │
│  REST Catalog   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Iceberg Tables  │
│ (S3/GCS/Azure)  │
└─────────────────┘
```

## Troubleshooting

### Commit Failures

If you see `CommitFailedException`, the destination will automatically retry with exponential backoff. Check:
- Concurrent writes to the same table
- Network connectivity
- Catalog service health

### Schema Evolution

Iceberg supports schema evolution. New columns are automatically added. To modify existing columns, use Iceberg's schema evolution APIs separately.

### Authentication Errors

- **OAuth2**: Verify `client_id`, `client_secret`, and `oauth2_server_uri`
- **Bearer Token**: Ensure token hasn't expired
- **AWS SigV4**: Check AWS credentials and IAM permissions

### Table Not Visible

Tables are only registered in the REST catalog. To query:
- Use tools that support REST catalogs (Spark with REST catalog config, PyIceberg, Trino, etc.)
- Tables won't appear in AWS Glue Data Catalog unless using Glue REST API

## Limitations

- **Merge/Upsert**: Simplified implementation (delete+insert). For complex merge logic, use Iceberg merge APIs directly
- **Concurrent Writes**: Multiple writers to same table can cause commit conflicts (retries help but not guaranteed)
- **Large Transactions**: Very large appends may be slow (batch your data appropriately)

## Contributing

Contributions welcome! Areas for improvement:
- Advanced merge strategies
- Table evolution operations
- Performance optimizations
- Additional catalog implementations

## License

MIT License - see LICENSE file

## Resources

- [dlt Documentation](https://dlthub.com/docs)
- [Apache Iceberg](https://iceberg.apache.org/)
- [PyIceberg](https://py.iceberg.apache.org/)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/rest-catalog-spec/)
- [Apache Polaris](https://polaris.apache.org/)
