# Local Iceberg + NOAA GSOD Example

This folder is a minimal example of a **local Iceberg table** using an SQLite catalog and **PyIceberg** that pulls NOAA GSOD data, downloads yearly CSVs, and inserts the data into an Iceberg table stored on the local filesystem.

This repo is initended to be used as an example and for local expirmentation.

## What it does

- Downloads NOAA GSOD CSVs for a year range and one or more site IDs.
- Inserts records into a local Iceberg table via PyIceberg.
- Provides a small read utility to summarize the table contents.

## Setup

From this directory:

```bash
uv venv
uv pip install -r requirements.txt
```

## Run

### Initialize the warehouse

This creates the local warehouse directory, ensures the namespace exists, and creates the Iceberg table if it is missing.

```bash
uv run python warehouse.py init
```

### Reset the table

This drops and recreates the Iceberg table while keeping the warehouse directory structure in place.

```bash
uv run python warehouse.py reset
```

### Clean the warehouse

This drops the Iceberg table and deletes both the warehouse directory and the downloads cache.

```bash
uv run python warehouse.py clean
```

### Download only

Fetches NOAA GSOD CSVs for the requested years and site IDs into `downloads/` without inserting into Iceberg.

```bash
uv run python noaa_data.py download 1980 1990 --site-id 72406093721
```

### Download and insert

Downloads the CSVs (if missing locally) and appends them into the Iceberg table.

```bash
uv run python noaa_data.py insert 1980 1990 --site-id 72406093721
```

### Multiple site IDs

Pass multiple station IDs in one run to download/insert them together.

```bash
uv run python noaa_data.py insert 1980 1990 --site-id 72406093721 72681024131
```

### Site IDs from a file

Use `file:PATH` to load a list of IDs from a file (space- or newline-separated).

```bash
uv run python noaa_data.py insert 1980 1990 --site-id file:site_ids.txt
```

### Control write batch size

The default batch size is 1000 rows. For large backfills, increasing this value is significantly more efficient because it reduces the number of Iceberg append operations. 

```bash
uv run python noaa_data.py insert 1980 1990 --site-id 72406093721 --min-rows 5000
```

### Read a summary

Prints the table name, total row count, and one example row for a quick sanity check.

```bash
uv run python read_data.py
```

## Notes

- Data downloads go to `downloads/`.
- The Iceberg warehouse lives under `warehouse/`.
- Missing site/year combinations are recorded in `downloads/missing_site_years.txt` and skipped on future runs.
- `noaa_data.py` uses async downloads by default for better throughput.
- The NOAA GSOD schema is defined in `noaa_schema.py` and used both when creating the Iceberg table and when parsing CSVs with PyArrow, ensuring consistent column types across ingestion and storage.

## Project layout

- `noaa_data.py`: CLI for downloading NOAA GSOD CSVs and inserting into the Iceberg table (async by default).
- `write_data.py`: CSV → Arrow conversion and Arrow → Iceberg write helpers.
- `read_data.py`: Prints a short summary of the table (name, row count, sample row).
- `noaa_schema.py`: Arrow and Iceberg schemas for the GSOD dataset.
- `settings.py`: Central configuration for paths, table name, and catalog settings.
- `downloads/`: Cached CSV files by site/year.
- `warehouse/`: Local Iceberg warehouse directory (SQLite catalog + table data/metadata).

## Settings

The `settings.py` file centralizes configuration:

- `warehouse_dir`: Absolute path to the local Iceberg warehouse directory.
- `downloads_dir`: Absolute path where NOAA CSVs are stored.
- `missing_site_years_path`: Text file recording missing `{site_id}:{year}` combinations.
- `catalog_db_path`: SQLite path used by the PyIceberg SQL catalog.
- `namespace`: Iceberg namespace (defaults to `weather`).
- `table_name`: Iceberg table name (defaults to `noaa_gsod`).
- `identifier`: Fully qualified table identifier (e.g., `weather.noaa_gsod`).
- `catalog`: PyIceberg catalog configuration used by `load_catalog` (local SQL + warehouse path).

## Snapshot notes

This example can create **many snapshots**, which may cause Iceberg metadata to grow larger than the data itself. That behavior is mostly a result of how frequently data is appended in small batches. If you backfill large ranges, consider using larger write batches and follow up with Iceberg maintenance like `expire_snapshots` to prune old snapshots.
