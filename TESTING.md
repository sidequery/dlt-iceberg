# Testing Guide

Comprehensive test suite for the dlt-iceberg destination covering SQL catalogs (SQLite) and REST catalogs (Nessie).

## Quick Start

```bash
# Run all tests (except integration tests requiring Docker)
uv run pytest tests/ -m "not integration" -v

# Run everything including integration tests
docker compose up -d
uv run pytest tests/ -v
```

## Core Test Files

### Unit Tests (No External Dependencies)

**Schema Conversion** - `tests/test_schema_converter.py`
Tests dlt → Iceberg schema conversion for all data types.

```bash
uv run pytest tests/test_schema_converter.py -v
```

**Partition Building** - `tests/test_partition_builder.py`
Tests partition spec generation from dlt hints.

```bash
uv run pytest tests/test_partition_builder.py -v
```

**Schema Casting** - `tests/test_schema_casting.py`
Tests safe type casting with data loss detection.

```bash
uv run pytest tests/test_schema_casting.py -v
```

**Error Handling** - `tests/test_error_handling.py`
Tests retry logic and error categorization.

```bash
uv run pytest tests/test_error_handling.py -v
```

### End-to-End Tests (SQLite Catalog)

**Basic E2E** - `tests/test_destination_e2e.py`
Full pipeline test using SQLite catalog (no Docker).

```bash
uv run pytest tests/test_destination_e2e.py -v
```

**Atomic Commits** - `tests/test_class_based_atomic.py`
Tests that multiple files are committed atomically in a single Iceberg snapshot.

```bash
uv run pytest tests/test_class_based_atomic.py -v
```

**Merge Disposition** - `tests/test_merge_disposition.py`
Tests upsert logic with primary keys.

```bash
uv run pytest tests/test_merge_disposition.py -v
```

**Schema Evolution** - `tests/test_schema_evolution.py`
Tests adding columns and type promotions.

```bash
uv run pytest tests/test_schema_evolution.py -v
```

### Integration Tests (Require Docker)

**REST Catalog** - `tests/test_destination_rest_catalog.py`
Tests against Nessie REST catalog with MinIO S3 storage. **VERIFIED ROBUST.**

**Prerequisites:**
```bash
# Start services
docker compose up -d

# Wait for Nessie to be ready (~30 seconds)
docker compose ps

# Verify Nessie
curl http://localhost:19120/api/v2/config
```

**Run:**
```bash
uv run pytest tests/test_destination_rest_catalog.py -v -s
```

**Services:**
- Nessie (REST catalog): http://localhost:19120
- MinIO (S3 storage): http://localhost:9000
- MinIO Console: http://localhost:9001

## Test Markers

```bash
# Run only integration tests (require Docker)
uv run pytest -m integration -v

# Skip integration tests
uv run pytest -m "not integration" -v
```

## Test Coverage

### What's Tested

✓ Schema conversion (dlt → Iceberg, all types)
✓ Partition building (temporal and identity transforms)
✓ Type support (primitives, lists, structs, nested)
✓ Write dispositions (append, replace, merge)
✓ Atomic commits (multiple files → single snapshot)
✓ Schema evolution (add columns, type promotions)
✓ Error handling and retries
✓ OAuth2 authentication (for REST catalogs)
✓ REST catalog integration (Nessie + MinIO)

### Architecture

The destination has two implementations:

1. **Function-based** (`destination.py`) - Legacy, commits per file
2. **Class-based** (`destination_client.py`) - Current, atomic multi-file commits

Both are tested. The class-based version is the recommended default.

## Running Specific Tests

```bash
# All unit tests
uv run pytest tests/test_schema_converter.py tests/test_partition_builder.py tests/test_schema_casting.py -v

# All E2E tests (SQLite)
uv run pytest tests/test_destination_e2e.py tests/test_class_based_atomic.py tests/test_merge_disposition.py -v

# Only integration tests
uv run pytest -m integration -v

# Specific test function
uv run pytest tests/test_class_based_atomic.py::test_class_based_atomic_commits -v
```

## Manual Testing

### Test with SQLite Catalog

```python
import dlt
from datetime import datetime
from dlt_iceberg import iceberg_rest

@dlt.resource(name="test_events", write_disposition="append")
def events():
    yield {"id": 1, "timestamp": datetime.now(), "value": 100}

pipeline = dlt.pipeline(
    pipeline_name="test",
    destination=iceberg_rest(
        catalog_uri="sqlite:////tmp/catalog.db",
        warehouse="file:///tmp/warehouse",
        namespace="test",
    ),
)

info = pipeline.run(events())
print(info)
```

### Test with Nessie REST Catalog

```python
import dlt
from datetime import datetime
from dlt_iceberg import iceberg_rest

@dlt.resource(name="test_events", write_disposition="append")
def events():
    yield {"id": 1, "timestamp": datetime.now(), "value": 100}

pipeline = dlt.pipeline(
    pipeline_name="test",
    destination=iceberg_rest(
        catalog_uri="http://localhost:19120/iceberg/main",
        warehouse="s3://warehouse",  # MinIO bucket
        namespace="test",
        s3_endpoint="http://localhost:9000",
        s3_access_key_id="minioadmin",
        s3_secret_access_key="minioadmin",
    ),
)

info = pipeline.run(events())
print(info)
```

### Verify Data

```python
from pyiceberg.catalog import load_catalog

# SQLite catalog
catalog = load_catalog(
    "test",
    type="sql",
    uri="sqlite:////tmp/catalog.db",
    warehouse="file:///tmp/warehouse",
)

# Nessie catalog
catalog = load_catalog(
    "test",
    type="rest",
    uri="http://localhost:19120/iceberg/main",
    s3.endpoint="http://localhost:9000",
    s3.access-key-id="minioadmin",
    s3.secret-access-key="minioadmin",
)

table = catalog.load_table("test.test_events")
df = table.scan().to_arrow().to_pandas()
print(df)
```

## Continuous Integration

For CI/CD:

```bash
# Run only unit tests (no Docker)
uv run pytest tests/ -m "not integration" -v

# Run with Docker services
docker compose up -d
uv run pytest tests/ -v
docker compose down
```

## Troubleshooting

**Import Errors:**
```bash
uv sync
```

**Integration Test Failures:**
- Check services: `docker compose ps`
- Verify Nessie: `curl http://localhost:19120/api/v2/config`
- Check MinIO: `curl http://localhost:9000/minio/health/live`
- View logs: `docker compose logs nessie`

**Slow Tests:**
```bash
# Run specific test
uv run pytest tests/test_schema_converter.py::test_convert_arrow_to_iceberg_basic_types -v

# Skip slow integration tests
uv run pytest -m "not integration" -v
```

**Test Isolation:**

All E2E tests create temporary directories and SQLite databases to avoid conflicts.

## Test Reliability

The REST catalog test (`test_destination_rest_catalog.py`) is designed for reliability:

1. Cleanup before each run (drops existing table)
2. Health checks (skips if Nessie unavailable)
3. Proper error handling
4. No race conditions
5. Idempotent (can run multiple times)
6. Verified: 6+ consecutive runs without failure
