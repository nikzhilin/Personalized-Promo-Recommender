"""Apply immutable numbered SQL migrations with checksums and an advisory lock."""

from __future__ import annotations

import argparse
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

import psycopg

MIGRATION_PATTERN = re.compile(r"(?P<version>[0-9]{3,})_[a-z0-9_]+\.sql")
LOCK_NAME = "personalized_promo_recommender_migrations"


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    sql: str
    checksum: str


def discover_migrations(directory: Path) -> list[Migration]:
    migrations: list[Migration] = []
    for path in sorted(directory.glob("*.sql")):
        match = MIGRATION_PATTERN.fullmatch(path.name)
        if match is None:
            raise ValueError(f"invalid migration filename: {path.name}")
        sql = path.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=match.group("version"),
                name=path.name,
                sql=sql,
                checksum=hashlib.sha256(sql.encode()).hexdigest(),
            )
        )
    versions = [migration.version for migration in migrations]
    if not migrations or len(versions) != len(set(versions)):
        raise ValueError("migrations must contain unique numbered versions")
    return migrations


def apply_migrations(database_url: str, directory: Path) -> list[str]:
    migrations = discover_migrations(directory)
    applied: list[str] = []
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute("SELECT pg_advisory_lock(hashtext(%s))", (LOCK_NAME,))
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version    TEXT PRIMARY KEY,
                    name       TEXT NOT NULL,
                    checksum   CHAR(64) NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            existing = {
                row[0]: (row[1], row[2])
                for row in connection.execute(
                    "SELECT version, name, checksum FROM schema_migrations"
                ).fetchall()
            }
            for migration in migrations:
                recorded = existing.get(migration.version)
                if recorded is not None:
                    if recorded != (migration.name, migration.checksum):
                        raise ValueError(
                            f"migration {migration.version} checksum/name mismatch"
                        )
                    continue
                with connection.transaction():
                    connection.execute(migration.sql)
                    connection.execute(
                        """
                        INSERT INTO schema_migrations(version, name, checksum)
                        VALUES (%s, %s, %s)
                        """,
                        (migration.version, migration.name, migration.checksum),
                    )
                applied.append(migration.version)
        finally:
            connection.execute("SELECT pg_advisory_unlock(hashtext(%s))", (LOCK_NAME,))
    return applied


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        default=Path(__file__).with_name("migrations"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    applied = apply_migrations(args.database_url, args.migrations_dir)
    print(f"applied migrations: {', '.join(applied) if applied else 'none'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
