# Examples

This directory contains example scripts demonstrating how to use dlt-iceberg with REST catalogs.

## Prerequisites

Start the Docker services (Nessie REST catalog and MinIO):

```bash
docker compose up -d
```

Wait for services to be ready:

```bash
docker compose ps
```

## Examples

### Incremental Load (`incremental_load.py`)

Demonstrates loading CSV files incrementally into an Iceberg table using append disposition.

- Loads `events_batch1.csv` (5 events)
- Loads `events_batch2.csv` (5 more events)
- Each batch creates a new Iceberg snapshot
- Final table contains 10 events

**Run:**

```bash
uv run examples/incremental_load.py
```

### Merge Load (`merge_load.py`)

Demonstrates merging two CSV files with overlapping customer IDs using merge disposition (upsert).

- Loads `customers_initial.csv` (5 customers)
- Loads `customers_updates.csv` (2 updates + 2 new customers)
- Updates are merged based on `customer_id` primary key
- Final table contains 7 customers with updated records

**Run:**

```bash
uv run examples/merge_load.py
```

## Data Files

Sample CSV files are in the `data/` directory:

- `events_batch1.csv` - First batch of events
- `events_batch2.csv` - Second batch of events
- `customers_initial.csv` - Initial customer data
- `customers_updates.csv` - Customer updates with overlapping IDs

## Key Features Demonstrated

- **Atomic commits**: Multiple files committed as single Iceberg snapshot
- **Incremental loads**: Append new data to existing tables
- **Merge/Upsert**: Update existing records and insert new ones based on primary key
- **REST catalog**: All examples use Nessie REST catalog with MinIO storage
- **Querying**: Direct PyIceberg queries to verify loaded data
