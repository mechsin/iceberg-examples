from __future__ import annotations

from pathlib import Path
from pprint import pformat

from pyiceberg.catalog import load_catalog

import settings


def main() -> None:
    warehouse_dir = Path(settings.warehouse_dir)

    catalog = load_catalog(**settings.catalog)

    table = catalog.load_table(settings.identifier)

    print(f"Loaded {settings.identifier} from {warehouse_dir}")
    print(f"Table name: {settings.identifier}")

    total_rows = table.scan().count()
    print(f"Total rows: {total_rows}")

    sample = table.scan(limit=1).to_arrow()
    if sample.num_rows:
        row = sample.to_pylist()[0]
        print("Example row:")
        print(pformat(row, sort_dicts=True))
    else:
        print("Example row: <empty>")


if __name__ == "__main__":
    main()
