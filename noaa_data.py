from __future__ import annotations

import argparse
import asyncio
from datetime import date
import os
from typing import Iterable, Sequence

import aiohttp
import pyarrow as pa
import requests
from tqdm import tqdm

import settings
from write_data import csv_path_to_arrow_table, write_arrow_table


BASE_URL = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{year}/{site_id}.csv"
DEFAULT_SITE_ID = "72406093721"


def iter_years(start_year: int, end_year: int) -> Iterable[int]:
    """Yield years from start_year to end_year inclusive."""
    if start_year > end_year:
        raise ValueError("start_year must be <= end_year")
    return range(start_year, end_year + 1)


def fetch_year_csv(year: int, site_id: str, target_dir: str) -> tuple[str | None, bool]:
    """Download a single year CSV (sync), returning (path or None, was_cached)."""
    target_path = os.path.join(target_dir, f"{site_id}_{year}.csv")
    if os.path.exists(target_path):
        return target_path, True

    url = BASE_URL.format(year=year, site_id=site_id)
    with requests.get(url, stream=True, timeout=60) as resp:
        if resp.status_code == 404:
            return None, False
        resp.raise_for_status()
        with open(target_path, "wb") as handle:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)

    return target_path, False


def fetch_range(
    start_year: int,
    end_year: int,
    site_id: str,
    target_dir: str,
    missing_entries: set[str] | None = None,
) -> dict[str, list[str]]:
    """Download a year range (sync); prefer fetch_range_async for throughput."""
    if not os.path.isdir(target_dir):
        raise FileNotFoundError(f"Download directory does not exist: {target_dir}")

    downloaded: list[str] = []
    cached: list[str] = []
    missing: list[str] = []
    missing_entries = missing_entries or set()

    for year in tqdm(iter_years(start_year, end_year), desc="Fetching NOAA"):
        if f"{site_id}:{year}" in missing_entries:
            continue
        result, was_cached = fetch_year_csv(year, site_id, target_dir)
        if result is None:
            missing.append(f"{site_id}:{year}")
        elif was_cached:
            cached.append(result)
        else:
            downloaded.append(result)

    return {"downloaded": downloaded, "cached": cached, "missing": missing}


async def fetch_year_csv_async(
    session: aiohttp.ClientSession,
    year: int,
    site_id: str,
    target_dir: str,
    semaphore: asyncio.Semaphore,
) -> tuple[str, int, str | None, bool, bool]:
    """Download a single year CSV (async), returning (site_id, year, path, cached, missing)."""
    target_path = os.path.join(target_dir, f"{site_id}_{year}.csv")
    if os.path.exists(target_path):
        return site_id, year, target_path, True, False

    url = BASE_URL.format(year=year, site_id=site_id)
    async with semaphore:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status == 404:
                return site_id, year, None, False, True
            resp.raise_for_status()
            with open(target_path, "wb") as handle:
                async for chunk in resp.content.iter_chunked(1024 * 1024):
                    if chunk:
                        handle.write(chunk)

    return site_id, year, target_path, False, False


async def fetch_range_async(
    start_year: int,
    end_year: int,
    site_ids: Sequence[str],
    target_dir: str,
    missing_entries: set[str] | None = None,
    max_concurrency: int = 8,
) -> dict[str, list[str]]:
    """Download year ranges for multiple site IDs concurrently."""
    if not os.path.isdir(target_dir):
        raise FileNotFoundError(f"Download directory does not exist: {target_dir}")

    downloaded: list[str] = []
    cached: list[str] = []
    missing: list[str] = []
    missing_entries = missing_entries or set()

    semaphore = asyncio.Semaphore(max_concurrency)
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_year_csv_async(session, year, site_id, target_dir, semaphore)
            for site_id in site_ids
            for year in iter_years(start_year, end_year)
            if f"{site_id}:{year}" not in missing_entries
        ]
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Fetching NOAA"):
            site_id, year, result, was_cached, was_missing = await task
            if was_missing:
                missing.append(f"{site_id}:{year}")
            elif result is None:
                missing.append(f"{site_id}:{year}")
            elif was_cached:
                cached.append(result)
            else:
                downloaded.append(result)

    return {"downloaded": downloaded, "cached": cached, "missing": missing}


def insert_csvs(csv_paths: Iterable[str], min_rows_per_write: int = 1000) -> dict[str, int]:
    """Insert CSVs into the table, buffering writes to reach min_rows_per_write."""
    rows_written = 0
    files_written = 0
    buffered_tables: list[pa.Table] = []
    buffered_rows = 0

    for csv_path in tqdm(list(csv_paths), desc="Inserting NOAA"):
        arrow_table = csv_path_to_arrow_table(csv_path)
        buffered_tables.append(arrow_table)
        buffered_rows += arrow_table.num_rows
        files_written += 1

        if buffered_rows >= min_rows_per_write:
            combined = pa.concat_tables(buffered_tables)
            write_arrow_table(combined)
            rows_written += combined.num_rows
            buffered_tables = []
            buffered_rows = 0

    if buffered_tables:
        combined = pa.concat_tables(buffered_tables)
        write_arrow_table(combined)
        rows_written += combined.num_rows

    return {"rows_written": rows_written, "files_written": files_written}


def resolve_site_ids(site_ids: Sequence[str]) -> list[str]:
    """Resolve site IDs, expanding any file:PATH entries into IDs from file."""
    resolved: list[str] = []
    for entry in site_ids:
        if entry.startswith("file:"):
            path = entry[5:]
            with open(path, "r", encoding="utf-8") as handle:
                content = handle.read()
            for token in content.split():
                if token:
                    resolved.append(token)
        else:
            resolved.append(entry)
    return resolved


def load_missing_entries(missing_path: str) -> set[str]:
    """Load missing site/year entries from a text file."""
    if not os.path.exists(missing_path):
        return set()
    with open(missing_path, "r", encoding="utf-8") as handle:
        return {line.strip() for line in handle if line.strip()}


def write_missing_entries(missing_path: str, entries: set[str]) -> None:
    """Write missing site/year entries to a text file (sorted)."""
    with open(missing_path, "w", encoding="utf-8") as handle:
        for entry in sorted(entries):
            handle.write(f"{entry}\n")


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for download/insert commands."""
    parser = argparse.ArgumentParser(
        description="Download and insert NOAA GSOD CSVs for a year range."
    )

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "start_year",
        type=int,
        help="Start year (inclusive).",
    )
    common.add_argument(
        "end_year",
        type=int,
        nargs="?",
        default=date.today().year,
        help="End year (inclusive). Defaults to the current year.",
    )
    common.add_argument(
        "--site-id",
        nargs="+",
        default=[DEFAULT_SITE_ID],
        help=(
            "One or more NOAA site ids. Supports file:PATH entries to load ids "
            f"from a file (default: {DEFAULT_SITE_ID})."
        ),
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("download", parents=[common], help="Download CSVs only.")
    insert_parser = subparsers.add_parser("insert", parents=[common], help="Download and insert CSVs.")
    insert_parser.add_argument(
        "--min-rows",
        type=int,
        default=1000,
        help="Minimum rows per write batch (default: 1000).",
    )

    return parser


def main_cli() -> None:
    """Run the CLI entry point."""
    args = build_parser().parse_args()
    site_ids = resolve_site_ids(args.site_id)
    missing_entries = load_missing_entries(settings.missing_site_years_path)
    results = asyncio.run(
        fetch_range_async(
            args.start_year,
            args.end_year,
            site_ids,
            settings.downloads_dir,
            missing_entries=missing_entries,
        )
    )

    print(f"Downloaded: {len(results['downloaded'])}")
    print(f"Cached: {len(results['cached'])}")
    print(f"Missing: {len(results['missing'])}")
    if results["missing"]:
        print("Missing entries:")
        for entry in results["missing"]:
            print(f"- {entry}")
        missing_entries.update(results["missing"])
        write_missing_entries(settings.missing_site_years_path, missing_entries)

    if args.command == "insert":
        csv_paths = results["downloaded"] + results["cached"]
        insert_results = insert_csvs(csv_paths, args.min_rows)
        print(f"Wrote to {settings.identifier}")
        print(f"Files written: {insert_results['files_written']}")
        print(f"Rows written: {insert_results['rows_written']}")


if __name__ == "__main__":
    main_cli()
