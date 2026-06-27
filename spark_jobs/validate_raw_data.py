"""Stream raw CSV files and validate their ingestion contracts."""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from spark_jobs.data_contracts import DATA_CONTRACTS, FieldContract, FieldType, FileContract
from spark_jobs.time_compat import UTC

MAX_EXAMPLES = 5


@dataclass
class ValidationIssue:
    level: str
    code: str
    file: str
    message: str
    count: int = 0
    examples: list[str] = field(default_factory=list)


@dataclass
class FileProfile:
    rows: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)
    datetime_ranges: dict[str, dict[str, str | None]] = field(default_factory=dict)
    rows_before_cutoff: int | None = None
    rows_on_or_after_cutoff: int | None = None


@dataclass
class ValidationReport:
    generated_at: str
    data_dir: str
    feature_cutoff: str
    valid: bool
    files: dict[str, FileProfile]
    issues: list[ValidationIssue]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class IssueCollector:
    def __init__(self) -> None:
        self._issues: dict[tuple[str, str, str, str], ValidationIssue] = {}

    def add(
        self,
        *,
        level: str,
        code: str,
        file: str,
        message: str,
        example: str | None = None,
    ) -> None:
        key = (level, code, file, message)
        issue = self._issues.setdefault(key, ValidationIssue(level, code, file, message))
        issue.count += 1
        if example is not None and len(issue.examples) < MAX_EXAMPLES:
            issue.examples.append(example)

    def values(self) -> list[ValidationIssue]:
        return sorted(self._issues.values(), key=lambda issue: (issue.file, issue.code))


def _parse_value(value: str, contract: FieldContract) -> object:
    if contract.field_type == FieldType.STRING:
        parsed: object = value
    elif contract.field_type == FieldType.INTEGER:
        parsed = int(value)
    elif contract.field_type == FieldType.FLOAT:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("non-finite number")
    elif contract.field_type == FieldType.DATETIME:
        parsed = datetime.fromisoformat(value)
    else:  # pragma: no cover - protects future enum additions
        raise ValueError(f"unsupported field type: {contract.field_type}")

    if contract.allowed_values is not None and parsed not in contract.allowed_values:
        raise ValueError(f"expected one of {sorted(contract.allowed_values)}")
    return parsed


def _format_key(row: dict[str, str], fields: tuple[str, ...]) -> str:
    return "|".join(row.get(name, "") for name in fields)


class RawDataValidator:
    def __init__(
        self,
        data_dir: Path,
        feature_cutoff: datetime,
        *,
        max_purchase_rows: int | None = None,
        enforce_feature_cutoff: bool = False,
    ) -> None:
        if feature_cutoff.tzinfo is not None:
            raise ValueError("feature_cutoff must not contain a timezone")
        if max_purchase_rows is not None and max_purchase_rows <= 0:
            raise ValueError("max_purchase_rows must be greater than zero")
        self.data_dir = data_dir
        self.feature_cutoff = feature_cutoff
        self.max_purchase_rows = max_purchase_rows
        self.enforce_feature_cutoff = enforce_feature_cutoff
        self.issues = IssueCollector()
        self.profiles: dict[str, FileProfile] = {}
        self.keys: dict[str, set[str]] = {}

    def validate(self) -> ValidationReport:
        for file_name, contract in DATA_CONTRACTS.items():
            limit = self.max_purchase_rows if file_name == "purchases.csv" else None
            self._validate_file(file_name, contract, limit)

        issues = self.issues.values()
        return ValidationReport(
            generated_at=datetime.now(UTC).isoformat(),
            data_dir=str(self.data_dir),
            feature_cutoff=self.feature_cutoff.isoformat(),
            valid=not any(issue.level == "error" for issue in issues),
            files=self.profiles,
            issues=issues,
        )

    def _validate_file(
        self, file_name: str, contract: FileContract, row_limit: int | None
    ) -> None:
        path = self.data_dir / file_name
        profile = FileProfile(null_counts={name: 0 for name in contract.fields})
        if file_name == "purchases.csv":
            profile.rows_before_cutoff = 0
            profile.rows_on_or_after_cutoff = 0
        self.profiles[file_name] = profile

        if not path.is_file():
            self.issues.add(
                level="error",
                code="missing_file",
                file=file_name,
                message=f"Required file does not exist: {path}",
            )
            return

        seen_keys: set[str] = set()
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            actual_fields = set(reader.fieldnames or [])
            missing_fields = set(contract.fields) - actual_fields
            for field_name in sorted(missing_fields):
                self.issues.add(
                    level="error",
                    code="missing_column",
                    file=file_name,
                    message=f"Required column is missing: {field_name}",
                )
            if missing_fields:
                return

            for line_number, row in enumerate(reader, start=2):
                if row_limit is not None and profile.rows >= row_limit:
                    break
                profile.rows += 1
                if None in row:
                    self.issues.add(
                        level="error",
                        code="malformed_row",
                        file=file_name,
                        message="Row has more values than the header",
                        example=str(line_number),
                    )

                parsed_values: dict[str, object] = {}
                for field_name, field_contract in contract.fields.items():
                    raw_value = (row.get(field_name) or "").strip()
                    if raw_value == "":
                        profile.null_counts[field_name] += 1
                        if not field_contract.nullable:
                            self.issues.add(
                                level="error",
                                code="null_required_value",
                                file=file_name,
                                message=f"Required value is empty: {field_name}",
                                example=str(line_number),
                            )
                        continue
                    try:
                        parsed = _parse_value(raw_value, field_contract)
                    except (TypeError, ValueError) as error:
                        self.issues.add(
                            level="error",
                            code="invalid_value",
                            file=file_name,
                            message=f"Invalid {field_name}: {error}",
                            example=f"line {line_number}: {raw_value}",
                        )
                        continue
                    parsed_values[field_name] = parsed
                    if isinstance(parsed, datetime):
                        self._update_datetime_range(profile, field_name, parsed)

                if contract.primary_key:
                    key = _format_key(row, contract.primary_key)
                    if key in seen_keys:
                        self.issues.add(
                            level="error",
                            code="duplicate_primary_key",
                            file=file_name,
                            message=f"Duplicate primary key: {','.join(contract.primary_key)}",
                            example=key,
                        )
                    else:
                        seen_keys.add(key)

                self._validate_relations(file_name, row, line_number)
                if file_name == "purchases.csv":
                    self._classify_purchase_cutoff(parsed_values, line_number)

        if contract.primary_key:
            self.keys[file_name] = seen_keys
        if profile.rows == 0:
            self.issues.add(
                level="error",
                code="empty_file",
                file=file_name,
                message="File has no data rows",
            )

    @staticmethod
    def _update_datetime_range(
        profile: FileProfile, field_name: str, value: datetime
    ) -> None:
        date_range = profile.datetime_ranges.setdefault(field_name, {"min": None, "max": None})
        serialized = value.isoformat()
        if date_range["min"] is None or serialized < date_range["min"]:
            date_range["min"] = serialized
        if date_range["max"] is None or serialized > date_range["max"]:
            date_range["max"] = serialized

    def _validate_relations(
        self, file_name: str, row: dict[str, str | list[str] | None], line_number: int
    ) -> None:
        relations: list[tuple[str, str, str]] = []
        if file_name == "purchases.csv":
            relations = [
                ("client_id", "clients.csv", "purchase_client_not_found"),
                ("product_id", "products.csv", "purchase_product_not_found"),
            ]
        elif file_name in {"uplift_train.csv", "uplift_test.csv"}:
            relations = [("client_id", "clients.csv", "uplift_client_not_found")]

        for field_name, parent_file, code in relations:
            raw_value = row.get(field_name)
            value = raw_value.strip() if isinstance(raw_value, str) else ""
            parent_keys = self.keys.get(parent_file)
            if value and parent_keys is not None and value not in parent_keys:
                self.issues.add(
                    level="error",
                    code=code,
                    file=file_name,
                    message=f"{field_name} is absent from {parent_file}",
                    example=f"line {line_number}: {value}",
                )

    def _classify_purchase_cutoff(
        self, parsed_values: dict[str, object], line_number: int
    ) -> None:
        profile = self.profiles["purchases.csv"]
        transaction_datetime = parsed_values.get("transaction_datetime")
        if not isinstance(transaction_datetime, datetime):
            return
        if transaction_datetime < self.feature_cutoff:
            assert profile.rows_before_cutoff is not None
            profile.rows_before_cutoff += 1
            return

        assert profile.rows_on_or_after_cutoff is not None
        profile.rows_on_or_after_cutoff += 1
        if self.enforce_feature_cutoff:
            self.issues.add(
                level="error",
                code="feature_leakage",
                file="purchases.csv",
                message="Feature input contains an event on or after feature_cutoff",
                example=f"line {line_number}: {transaction_datetime.isoformat()}",
            )


def validate_raw_data(
    data_dir: Path,
    feature_cutoff: datetime,
    *,
    max_purchase_rows: int | None = None,
    enforce_feature_cutoff: bool = False,
) -> ValidationReport:
    return RawDataValidator(
        data_dir,
        feature_cutoff,
        max_purchase_rows=max_purchase_rows,
        enforce_feature_cutoff=enforce_feature_cutoff,
    ).validate()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    parser.add_argument(
        "--feature-cutoff",
        type=datetime.fromisoformat,
        required=True,
        help="Timezone-free ISO timestamp; only earlier purchases may become features",
    )
    parser.add_argument(
        "--max-purchase-rows",
        type=int,
        help="Validate only the first N purchase rows; dimension files are always read fully",
    )
    parser.add_argument(
        "--enforce-feature-cutoff",
        action="store_true",
        help="Fail if purchase input contains events on or after the cutoff",
    )
    parser.add_argument("--report", type=Path, help="Write the JSON report to this path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = validate_raw_data(
            args.data_dir,
            args.feature_cutoff,
            max_purchase_rows=args.max_purchase_rows,
            enforce_feature_cutoff=args.enforce_feature_cutoff,
        )
    except ValueError as error:
        raise SystemExit(str(error)) from error

    rendered = json.dumps(report.to_dict(), indent=2, ensure_ascii=False)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(f"{rendered}\n", encoding="utf-8")
    print(rendered)
    return 0 if report.valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
