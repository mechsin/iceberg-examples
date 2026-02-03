from __future__ import annotations

import os

here = os.path.dirname(__file__) if os.path.dirname(__file__) else os.getcwd()
warehouse_dir = os.path.abspath(os.path.join(here, "warehouse"))
downloads_dir = os.path.abspath(os.path.join(here, "downloads"))
missing_site_years_path = os.path.join(downloads_dir, "missing_site_years.txt")
catalog_db_path = os.path.join(warehouse_dir, "iceberg_catalog.db")

namespace = "weather"
table_name = "noaa_gsod"
identifier = f"{namespace}.{table_name}"

catalog = {
    "name": "local",
    "type": "sql",
    "uri": f"sqlite:///{catalog_db_path}",
    "warehouse": warehouse_dir,
}
