from pathlib import Path

import pytest

from services.pipeline.state import config_fingerprint, update_run
from training.evaluate_offline import safe_ratio


def test_config_fingerprint_is_order_independent(tmp_path: Path) -> None:
    first = tmp_path / "a.yaml"
    second = tmp_path / "b.yaml"
    first.write_text("version: 1\n", encoding="utf-8")
    second.write_text("budget: 10\n", encoding="utf-8")
    assert config_fingerprint([first, second]) == config_fingerprint([second, first])


def test_safe_ratio_handles_empty_population() -> None:
    assert safe_ratio(4, 2) == 2
    assert safe_ratio(0, 0) == 0


def test_pipeline_status_is_validated_before_database_access() -> None:
    with pytest.raises(ValueError, match="unsupported pipeline status"):
        update_run("unused", run_id=__import__("uuid").uuid4(), status="UNKNOWN")
