"""Render an escaped SQLAlchemy URL for Airflow's *_CMD config support."""

from __future__ import annotations

import os
from urllib.parse import quote_plus


def main() -> int:
    user = quote_plus(os.environ["AIRFLOW_DB_USER"])
    password = quote_plus(os.environ["AIRFLOW_DB_PASSWORD"])
    database = quote_plus(os.environ["AIRFLOW_DB_NAME"])
    print(f"postgresql+psycopg2://{user}:{password}@postgres:5432/{database}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
