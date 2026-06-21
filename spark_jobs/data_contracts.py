"""Raw CSV contracts shared by ingestion and validation jobs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class FieldType(StrEnum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DATETIME = "datetime"


@dataclass(frozen=True)
class FieldContract:
    field_type: FieldType
    nullable: bool = False
    allowed_values: frozenset[object] | None = None


@dataclass(frozen=True)
class FileContract:
    fields: dict[str, FieldContract]
    primary_key: tuple[str, ...] = ()


STRING = FieldContract(FieldType.STRING)
NULLABLE_STRING = FieldContract(FieldType.STRING, nullable=True)
FLOAT = FieldContract(FieldType.FLOAT)
NULLABLE_FLOAT = FieldContract(FieldType.FLOAT, nullable=True)
DATETIME = FieldContract(FieldType.DATETIME)
NULLABLE_DATETIME = FieldContract(FieldType.DATETIME, nullable=True)
BINARY = FieldContract(FieldType.INTEGER, allowed_values=frozenset({0, 1}))


DATA_CONTRACTS: dict[str, FileContract] = {
    "clients.csv": FileContract(
        fields={
            "client_id": STRING,
            "first_issue_date": DATETIME,
            "first_redeem_date": NULLABLE_DATETIME,
            "age": NULLABLE_FLOAT,
            "gender": NULLABLE_STRING,
        },
        primary_key=("client_id",),
    ),
    "products.csv": FileContract(
        fields={
            "product_id": STRING,
            "level_1": NULLABLE_STRING,
            "level_2": NULLABLE_STRING,
            "level_3": NULLABLE_STRING,
            "level_4": NULLABLE_STRING,
            "segment_id": NULLABLE_FLOAT,
            "brand_id": NULLABLE_STRING,
            "vendor_id": NULLABLE_STRING,
            "netto": NULLABLE_FLOAT,
            "is_own_trademark": BINARY,
            "is_alcohol": BINARY,
        },
        primary_key=("product_id",),
    ),
    "purchases.csv": FileContract(
        fields={
            "client_id": STRING,
            "transaction_id": STRING,
            "transaction_datetime": DATETIME,
            "regular_points_received": FLOAT,
            "express_points_received": FLOAT,
            "regular_points_spent": FLOAT,
            "express_points_spent": FLOAT,
            "purchase_sum": FLOAT,
            "store_id": STRING,
            "product_id": STRING,
            "product_quantity": FLOAT,
            "trn_sum_from_iss": NULLABLE_FLOAT,
            "trn_sum_from_red": NULLABLE_FLOAT,
        }
    ),
    "uplift_train.csv": FileContract(
        fields={
            "client_id": STRING,
            "treatment_flg": BINARY,
            "target": BINARY,
        },
        primary_key=("client_id",),
    ),
    "uplift_test.csv": FileContract(
        fields={"client_id": STRING},
        primary_key=("client_id",),
    ),
}

