from __future__ import annotations

import argparse
import os
import shutil

from pyiceberg.catalog import load_catalog

import settings
from noaa_schema import iceberg_schema, partition_spec


def ensure_warehouse_dir() -> str:
    """Create the warehouse directory if needed and return its path."""
    os.makedirs(settings.warehouse_dir, exist_ok=True)
    return settings.warehouse_dir


def ensure_downloads_dir() -> str:
    """Create the downloads directory if needed and return its path."""
    os.makedirs(settings.downloads_dir, exist_ok=True)
    return settings.downloads_dir


def ensure_namespace(catalog) -> None:
    """Ensure the Iceberg namespace exists."""
    try:
        catalog.create_namespace(settings.namespace)
    except Exception:
        pass


def table_exists(catalog) -> bool:
    """Return True if the Iceberg table exists."""
    try:
        catalog.load_table(settings.identifier)
        return True
    except Exception:
        return False


def init_catalog() -> None:
    """Initialize the local Iceberg catalog and table if missing."""
    warehouse_dir = ensure_warehouse_dir()
    ensure_downloads_dir()
    catalog = load_catalog(**settings.catalog)
    ensure_namespace(catalog)

    if not table_exists(catalog):
        catalog.create_table(
            settings.identifier,
            schema=iceberg_schema(),
            partition_spec=partition_spec(),
        )
        print(f"Created {settings.identifier} in {warehouse_dir}")
    else:
        print(f"{settings.identifier} already exists in {warehouse_dir}")


def reset_catalog(hard: bool = False) -> None:
    """Drop and recreate the Iceberg table, optionally wiping the warehouse directory."""
    warehouse_dir = ensure_warehouse_dir()
    catalog = load_catalog(**settings.catalog)
    ensure_namespace(catalog)

    if hard:
        if os.path.exists(settings.warehouse_dir):
            shutil.rmtree(settings.warehouse_dir)
        os.makedirs(settings.warehouse_dir, exist_ok=True)
    else:
        try:
            catalog.drop_table(settings.identifier)
        except Exception:
            pass

    print(f"Reset {settings.identifier} in {warehouse_dir}")
    init_catalog()


def clean_catalog() -> None:
    """Drop the table and remove warehouse/download directories."""
    catalog = load_catalog(**settings.catalog)

    try:
        catalog.drop_table(settings.identifier)
    except Exception:
        pass

    if os.path.exists(settings.warehouse_dir):
        shutil.rmtree(settings.warehouse_dir)
        print(f"Removed {settings.warehouse_dir}")
    else:
        print(f"{settings.warehouse_dir} does not exist")

    if os.path.exists(settings.downloads_dir):
        shutil.rmtree(settings.downloads_dir)
        print(f"Removed {settings.downloads_dir}")
    else:
        print(f"{settings.downloads_dir} does not exist")


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for warehouse management."""
    parser = argparse.ArgumentParser(description="Manage Iceberg warehouse.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Create the namespace/table if missing.")
    reset_parser = subparsers.add_parser("reset", help="Drop and recreate the table.")
    reset_parser.add_argument(
        "--hard",
        action="store_true",
        help="Wipe the warehouse directory before recreating the table.",
    )
    subparsers.add_parser("clean", help="Drop the table and remove the warehouse directory.")

    return parser


def main() -> None:
    """Run the CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "init":
        init_catalog()
    elif args.command == "reset":
        reset_catalog(hard=args.hard)
    elif args.command == "clean":
        clean_catalog()


if __name__ == "__main__":
    main()
