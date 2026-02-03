from __future__ import annotations

from pathlib import Path
from typing import Union

import pyarrow as pa
import pyarrow.csv as pacsv
from pyiceberg.catalog import load_catalog

import settings
from noaa_schema import arrow_schema


PathLike = Union[str, Path]


def csv_path_to_arrow_table(csv_path: PathLike) -> pa.Table:
    """Read a NOAA GSOD CSV file into a PyArrow table using the fixed schema."""
    csv_path = Path(csv_path)
    return pacsv.read_csv(
        csv_path,
        convert_options=pacsv.ConvertOptions(
            column_types=arrow_schema(),
            strings_can_be_null=True,
        ),
    )


def write_arrow_table(arrow_table: pa.Table) -> None:
    """Append a PyArrow table to the configured Iceberg table."""
    catalog = load_catalog(**settings.catalog)
    table = catalog.load_table(settings.identifier)
    table.append(arrow_table)
