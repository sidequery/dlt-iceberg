#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#     "dlt",
#     "dlt-iceberg",
#     "pyiceberg",
#     "requests",
#     "python-dateutil",
# ]
# ///
"""
USGS Earthquake Data Example

Loads earthquake data from USGS GeoJSON API from 2010 through current date
into an Iceberg table.
"""

import dlt
import requests
import time
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from dlt_iceberg import iceberg_rest


def fetch_earthquakes(start_date: str, end_date: str, split_on_error: bool = True):
    """
    Fetch earthquake data from USGS API for a date range.

    If a 400 error occurs (likely due to >20k result limit), automatically
    splits the range into smaller chunks and retries.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        split_on_error: If True, split the date range on 400 errors
    """
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_date,
        "endtime": end_date,
    }

    print(f"Fetching earthquakes from {start_date} to {end_date}...")

    # Retry logic for rate limiting
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and split_on_error:
                # Split the date range and retry with smaller chunks
                print(f"  Too many results, splitting range into weekly chunks...")
                from datetime import timedelta

                start_dt = datetime.fromisoformat(start_date)
                end_dt = datetime.fromisoformat(end_date)
                current = start_dt

                while current < end_dt:
                    next_week = min(current + timedelta(days=7), end_dt)
                    # Recursively fetch with split_on_error=False to avoid infinite recursion
                    yield from fetch_earthquakes(
                        current.date().isoformat(),
                        next_week.date().isoformat(),
                        split_on_error=False
                    )
                    current = next_week
                return
            elif e.response.status_code == 400:
                print(f"  Warning: API returned 400 error for {start_date} to {end_date}, skipping...")
                return
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"  Request failed, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"  Request failed, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise

    data = response.json()
    features = data.get("features", [])
    print(f"Retrieved {len(features)} earthquakes")

    # Rate limiting: sleep briefly between requests
    time.sleep(0.5)

    # Transform GeoJSON features into flat records
    for feature in features:
        props = feature["properties"]
        geom = feature["geometry"]

        yield {
            "earthquake_id": feature["id"],
            "magnitude": props.get("mag"),
            "place": props.get("place"),
            "time": datetime.fromtimestamp(props["time"] / 1000) if props.get("time") else None,
            "updated": datetime.fromtimestamp(props["updated"] / 1000) if props.get("updated") else None,
            "url": props.get("url"),
            "detail": props.get("detail"),
            "felt": props.get("felt"),
            "cdi": props.get("cdi"),
            "mmi": props.get("mmi"),
            "alert": props.get("alert"),
            "status": props.get("status"),
            "tsunami": props.get("tsunami"),
            "sig": props.get("sig"),
            "net": props.get("net"),
            "code": props.get("code"),
            "ids": props.get("ids"),
            "sources": props.get("sources"),
            "types": props.get("types"),
            "nst": props.get("nst"),
            "dmin": props.get("dmin"),
            "rms": props.get("rms"),
            "gap": props.get("gap"),
            "magType": props.get("magType"),
            "type": props.get("type"),
            "title": props.get("title"),
            "longitude": geom["coordinates"][0] if geom and geom.get("coordinates") else None,
            "latitude": geom["coordinates"][1] if geom and geom.get("coordinates") else None,
            "depth": geom["coordinates"][2] if geom and geom.get("coordinates") and len(geom["coordinates"]) > 2 else None,
        }


def main():
    # Create dlt pipeline with Nessie REST catalog
    pipeline = dlt.pipeline(
        pipeline_name="usgs_earthquakes",
        destination=iceberg_rest(
            catalog_uri="http://localhost:19120/iceberg/main",
            namespace="examples",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
        ),
        dataset_name="usgs_data",
    )

    # Load earthquakes from 2010 through current date
    # Breaking into monthly batches to avoid overwhelming the API
    # Note: USGS endtime is exclusive, so we use the first day of next month

    start_date = date(2010, 1, 1)
    end_date = date.today()

    date_ranges = []
    current = start_date
    while current <= end_date:
        next_month = current + relativedelta(months=1)
        date_ranges.append((current.isoformat(), next_month.isoformat()))
        current = next_month

    print(f"Loading {len(date_ranges)} months of earthquake data from {start_date} to {end_date}...")
    print()

    for i, (start, end) in enumerate(date_ranges, 1):
        @dlt.resource(
            name="earthquakes",
            write_disposition="append",
            columns={
                "time": {
                    "data_type": "timestamp",
                    "x-partition": True,
                    "x-partition-transform": "month",
                }
            }
        )
        def earthquakes_batch():
            return fetch_earthquakes(start, end)

        load_info = pipeline.run(earthquakes_batch())
        print(f"[{i}/{len(date_ranges)}] Loaded {start} to {end}")
        print()

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

    table = catalog.load_table("examples.earthquakes")
    result = table.scan().to_arrow()

    print(f"\n{'='*60}")
    print(f"Total earthquakes loaded: {len(result)}")

    import pyarrow.compute as pc
    print(f"Date range: {pc.min(result['time']).as_py()} to {pc.max(result['time']).as_py()}")
    print(f"Magnitude range: {pc.min(result['magnitude']).as_py()} to {pc.max(result['magnitude']).as_py()}")

    # Show some sample records
    import pandas as pd
    df = result.to_pandas()
    print(f"\nSample earthquakes:")
    print(df[["time", "magnitude", "place", "depth"]].head(10).to_string(index=False))

    # Show distribution by month
    df["month"] = pd.to_datetime(df["time"]).dt.to_period("M")
    monthly_counts = df.groupby("month").size().sort_index()
    print(f"\nEarthquakes by month:")
    for month, count in monthly_counts.items():
        print(f"  {month}: {count:,}")

    # Show magnitude distribution
    print(f"\nMagnitude distribution:")
    print(df["magnitude"].describe())

    print(f"\n{'='*60}")
    print("USGS earthquake data load complete!")


if __name__ == "__main__":
    main()
