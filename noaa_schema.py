"""Schemas for the NOAA GSOD dataset used by PyIceberg and PyArrow."""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, YearTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    NestedField,
    StringType,
)


def iceberg_schema() -> Schema:
    fields = [
        ("STATION", StringType()),
        ("DATE", DateType()),
        ("LATITUDE", DoubleType()),
        ("LONGITUDE", DoubleType()),
        ("ELEVATION", DoubleType()),
        ("NAME", StringType()),
        ("TEMP", DoubleType()),
        ("TEMP_ATTRIBUTES", StringType()),
        ("DEWP", DoubleType()),
        ("DEWP_ATTRIBUTES", StringType()),
        ("SLP", DoubleType()),
        ("SLP_ATTRIBUTES", StringType()),
        ("STP", DoubleType()),
        ("STP_ATTRIBUTES", StringType()),
        ("VISIB", DoubleType()),
        ("VISIB_ATTRIBUTES", StringType()),
        ("WDSP", DoubleType()),
        ("WDSP_ATTRIBUTES", StringType()),
        ("MXSPD", DoubleType()),
        ("GUST", DoubleType()),
        ("MAX", DoubleType()),
        ("MAX_ATTRIBUTES", StringType()),
        ("MIN", DoubleType()),
        ("MIN_ATTRIBUTES", StringType()),
        ("PRCP", DoubleType()),
        ("PRCP_ATTRIBUTES", StringType()),
        ("SNDP", DoubleType()),
        ("FRSHTT", StringType()),
    ]

    nested_fields = []
    field_id = 1
    for name, field_type in fields:
        nested_fields.append(
            NestedField(
                field_id,
                name,
                field_type,
                required=False,
            )
        )
        field_id += 1

    return Schema(*nested_fields)


def arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            ("STATION", pa.string()),
            ("DATE", pa.date32()),
            ("LATITUDE", pa.float64()),
            ("LONGITUDE", pa.float64()),
            ("ELEVATION", pa.float64()),
            ("NAME", pa.string()),
            ("TEMP", pa.float64()),
            ("TEMP_ATTRIBUTES", pa.string()),
            ("DEWP", pa.float64()),
            ("DEWP_ATTRIBUTES", pa.string()),
            ("SLP", pa.float64()),
            ("SLP_ATTRIBUTES", pa.string()),
            ("STP", pa.float64()),
            ("STP_ATTRIBUTES", pa.string()),
            ("VISIB", pa.float64()),
            ("VISIB_ATTRIBUTES", pa.string()),
            ("WDSP", pa.float64()),
            ("WDSP_ATTRIBUTES", pa.string()),
            ("MXSPD", pa.float64()),
            ("GUST", pa.float64()),
            ("MAX", pa.float64()),
            ("MAX_ATTRIBUTES", pa.string()),
            ("MIN", pa.float64()),
            ("MIN_ATTRIBUTES", pa.string()),
            ("PRCP", pa.float64()),
            ("PRCP_ATTRIBUTES", pa.string()),
            ("SNDP", pa.float64()),
            ("FRSHTT", pa.string()),
        ]
    )


def partition_spec() -> PartitionSpec:
    return PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1000,
            transform=BucketTransform(32),
            name="station_bucket",
        ),
        PartitionField(
            source_id=2,
            field_id=1001,
            transform=YearTransform(),
            name="date_year",
        ),
    )
