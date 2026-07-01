from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from services.feedback.export_feedback import (
    FEEDBACK_SCHEMA,
    Watermark,
    deterministic_batch_id,
    event_date_utc,
    load_feedback_export_config,
    merge_feedback_rows,
    replace_hdfs_paths,
)


def feedback_row(event_id: str, fingerprint: str = "a" * 64) -> dict[str, object]:
    return {
        "event_id": event_id,
        "request_id": str(uuid.uuid4()),
        "client_id": "client-1",
        "product_id": "product-1",
        "event_type": "purchase",
        "shown_discount": Decimal("0.1000"),
        "purchase_value": Decimal("80.00"),
        "discount_cost": Decimal("8.00"),
        "created_at": datetime(2026, 7, 3, 12, tzinfo=UTC),
        "received_at": datetime(2026, 7, 3, 12, 1, tzinfo=UTC),
        "verification_status": "VERIFIED",
        "event_fingerprint": fingerprint,
        "event_date": datetime(2026, 7, 3, tzinfo=UTC).date(),
    }


def test_feedback_export_config_and_arrow_schema() -> None:
    config = load_feedback_export_config("configs/feedback_export.yaml")
    assert config.batch_size == 100_000
    assert config.required_replication == 2
    assert FEEDBACK_SCHEMA.field("shown_discount").type.precision == 5
    assert FEEDBACK_SCHEMA.field("created_at").type.tz == "UTC"


def test_watermark_order_and_batch_id_are_deterministic() -> None:
    received = datetime(2026, 7, 3, 12, tzinfo=UTC)
    lower = Watermark(received, uuid.UUID(int=1))
    upper = Watermark(received, uuid.UUID(int=2))
    assert lower < upper
    assert deterministic_batch_id(lower, upper) == deterministic_batch_id(lower, upper)
    assert len(deterministic_batch_id(lower, upper)) == 32


def test_event_date_uses_utc_business_time() -> None:
    plus_three = timezone(timedelta(hours=3))
    assert event_date_utc(datetime(2026, 7, 4, 1, tzinfo=plus_three)).isoformat() == (
        "2026-07-03"
    )


def test_merge_is_idempotent_and_rejects_fingerprint_conflicts() -> None:
    event_id = str(uuid.uuid4())
    row = feedback_row(event_id)
    assert merge_feedback_rows([row], [row]) == [row]
    with pytest.raises(ValueError, match="fingerprint conflict"):
        merge_feedback_rows([row], [feedback_row(event_id, "b" * 64)])


class FakeHdfs:
    def __init__(self) -> None:
        self.paths: dict[str, str] = {
            "/stage/a": "new-a",
            "/stage/b": "new-b",
            "/target/a": "old-a",
            "/target/b": "old-b",
        }
        self.fail_source = "/stage/b"

    def status(self, path: str, strict: bool = True) -> dict[str, str] | None:
        value = self.paths.get(path)
        if value is None and strict:
            raise FileNotFoundError(path)
        return None if value is None else {"value": value}

    def makedirs(self, path: str) -> None:
        del path

    def rename(self, source: str, target: str) -> None:
        if source == self.fail_source:
            raise RuntimeError("injected rename failure")
        self.paths[target] = self.paths.pop(source)

    def delete(self, path: str, recursive: bool = False) -> None:
        del recursive
        for key in [key for key in self.paths if key == path or key.startswith(f"{path}/")]:
            del self.paths[key]


def test_multi_partition_publication_rolls_back_on_rename_failure() -> None:
    hdfs = FakeHdfs()
    with pytest.raises(RuntimeError, match="injected"):
        replace_hdfs_paths(
            hdfs,  # type: ignore[arg-type]
            [("/stage/a", "/target/a"), ("/stage/b", "/target/b")],
            "/backup",
        )
    assert hdfs.paths["/target/a"] == "old-a"
    assert hdfs.paths["/target/b"] == "old-b"
