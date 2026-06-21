from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from spark_jobs.data_contracts import DATA_CONTRACTS
from spark_jobs.validate_raw_data import validate_raw_data


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _raw_data_dir(tmp_path: Path, purchase_datetime: str = "2019-01-01 12:00:00") -> Path:
    _write_csv(
        tmp_path / "clients.csv",
        [
            {
                "client_id": "client-1",
                "first_issue_date": "2018-01-01 10:00:00",
                "first_redeem_date": "",
                "age": "35",
                "gender": "F",
            }
        ],
    )
    _write_csv(
        tmp_path / "products.csv",
        [
            {
                "product_id": "product-1",
                "level_1": "l1",
                "level_2": "l2",
                "level_3": "l3",
                "level_4": "l4",
                "segment_id": "1",
                "brand_id": "brand-1",
                "vendor_id": "vendor-1",
                "netto": "0.5",
                "is_own_trademark": "0",
                "is_alcohol": "0",
            }
        ],
    )
    _write_csv(
        tmp_path / "purchases.csv",
        [
            {
                "client_id": "client-1",
                "transaction_id": "transaction-1",
                "transaction_datetime": purchase_datetime,
                "regular_points_received": "1",
                "express_points_received": "0",
                "regular_points_spent": "0",
                "express_points_spent": "0",
                "purchase_sum": "100",
                "store_id": "store-1",
                "product_id": "product-1",
                "product_quantity": "1",
                "trn_sum_from_iss": "50",
                "trn_sum_from_red": "",
            }
        ],
    )
    _write_csv(
        tmp_path / "uplift_train.csv",
        [{"client_id": "client-1", "treatment_flg": "1", "target": "0"}],
    )
    _write_csv(tmp_path / "uplift_test.csv", [{"client_id": "client-1"}])
    return tmp_path


def test_contract_covers_all_expected_source_files() -> None:
    assert set(DATA_CONTRACTS) == {
        "clients.csv",
        "products.csv",
        "purchases.csv",
        "uplift_train.csv",
        "uplift_test.csv",
    }


def test_valid_data_produces_profiles_and_no_errors(tmp_path: Path) -> None:
    data_dir = _raw_data_dir(tmp_path)

    report = validate_raw_data(data_dir, datetime(2019, 3, 1))

    assert report.valid
    assert report.issues == []
    assert report.files["purchases.csv"].rows == 1
    assert report.files["purchases.csv"].rows_before_cutoff == 1
    assert report.files["purchases.csv"].rows_on_or_after_cutoff == 0
    assert report.files["clients.csv"].null_counts["first_redeem_date"] == 1


def test_unknown_foreign_key_is_rejected(tmp_path: Path) -> None:
    data_dir = _raw_data_dir(tmp_path)
    purchase_path = data_dir / "purchases.csv"
    contents = purchase_path.read_text(encoding="utf-8")
    purchase_path.write_text(contents.replace("product-1", "unknown-product"), encoding="utf-8")

    report = validate_raw_data(data_dir, datetime(2019, 3, 1))

    assert not report.valid
    assert any(issue.code == "purchase_product_not_found" for issue in report.issues)


def test_post_cutoff_purchase_is_reported_but_raw_data_remains_valid(tmp_path: Path) -> None:
    data_dir = _raw_data_dir(tmp_path, purchase_datetime="2019-03-01 00:00:00")

    report = validate_raw_data(data_dir, datetime(2019, 3, 1))

    assert report.valid
    assert report.files["purchases.csv"].rows_before_cutoff == 0
    assert report.files["purchases.csv"].rows_on_or_after_cutoff == 1


def test_strict_cutoff_rejects_feature_leakage(tmp_path: Path) -> None:
    data_dir = _raw_data_dir(tmp_path, purchase_datetime="2019-03-01 00:00:00")

    report = validate_raw_data(
        data_dir,
        datetime(2019, 3, 1),
        enforce_feature_cutoff=True,
    )

    assert not report.valid
    leakage = next(issue for issue in report.issues if issue.code == "feature_leakage")
    assert leakage.count == 1


def test_duplicate_dimension_key_is_rejected(tmp_path: Path) -> None:
    data_dir = _raw_data_dir(tmp_path)
    clients_path = data_dir / "clients.csv"
    contents = clients_path.read_text(encoding="utf-8")
    _, row = contents.splitlines()
    clients_path.write_text(f"{contents}{row}\n", encoding="utf-8")

    report = validate_raw_data(data_dir, datetime(2019, 3, 1))

    assert not report.valid
    assert any(issue.code == "duplicate_primary_key" for issue in report.issues)
