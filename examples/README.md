# Examples

This directory contains example scripts demonstrating how to use dlt-iceberg with REST catalogs.

All examples use [PEP 723](https://peps.python.org/pep-0723/) inline script metadata, allowing them to be run directly with `uv run` without installing dependencies separately.

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

### USGS Earthquake Data (`usgs_earthquakes.py`)

Demonstrates loading real-world GeoJSON data from the USGS Earthquake API from 2010 through current date.

- Fetches earthquake data from USGS API (2010-present, ~190 months)
- Loads data in monthly batches with automatic weekly splitting for high-volume months
- Handles API's 20,000 result limit by automatically splitting large months into weekly chunks
- Uses partitioning by month on timestamp column
- Transforms GeoJSON features into flat records
- Loads ~2.5 million earthquakes with complete metadata

**Features demonstrated:**
- Dynamic date range generation using `date.today()`
- Automatic handling of API result limits with recursive splitting
- Retry logic with exponential backoff
- Rate limiting between API requests
- Large-scale data ingestion (190+ API calls)

**Run:**

```bash
uv run examples/usgs_earthquakes.py
```

**Note:** This script takes approximately 15-20 minutes to complete as it makes 190+ API calls with rate limiting.

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
- **Partitioning**: Partition tables by timestamp (month transform)
- **API integration**: Fetch and transform data from external APIs
- **Querying**: Direct PyIceberg queries to verify loaded data
