"""Idempotently create the isolated Airflow role and database."""

from __future__ import annotations

import argparse
import os

import psycopg
from psycopg import sql


def bootstrap_airflow_database(
    *, admin_database_url: str, database_name: str, user_name: str, password: str
) -> None:
    if not all(value and value.replace("_", "").isalnum() for value in (database_name, user_name)):
        raise ValueError("Airflow database and user names must be alphanumeric identifiers")
    if not password:
        raise ValueError("Airflow database password must not be empty")
    with psycopg.connect(admin_database_url, autocommit=True) as connection:
        role_exists = connection.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = %s", (user_name,)
        ).fetchone()
        if role_exists is None:
            connection.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(user_name), sql.Literal(password)
                )
            )
        else:
            connection.execute(
                sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                    sql.Identifier(user_name), sql.Literal(password)
                )
            )
        database_exists = connection.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (database_name,)
        ).fetchone()
        if database_exists is None:
            connection.execute(
                sql.SQL("CREATE DATABASE {} OWNER {}").format(
                    sql.Identifier(database_name), sql.Identifier(user_name)
                )
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-database-url", required=True)
    parser.add_argument("--database-name", required=True)
    parser.add_argument("--user-name", required=True)
    parser.add_argument("--password", default=os.getenv("AIRFLOW_DB_PASSWORD"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.password:
        raise ValueError("--password or AIRFLOW_DB_PASSWORD is required")
    bootstrap_airflow_database(
        admin_database_url=args.admin_database_url,
        database_name=args.database_name,
        user_name=args.user_name,
        password=args.password,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
