from __future__ import annotations

from datetime import datetime

import pytest

from training.build_propensity_dataset import label_purchase_months, validate_cutoffs


def test_label_month_selection_uses_half_open_future_window() -> None:
    assert label_purchase_months(
        datetime(2019, 3, 1), 30, ["2019-02", "2019-03", "2019-04"]
    ) == ["2019-03"]


def test_label_month_selection_crosses_month_boundary() -> None:
    assert label_purchase_months(
        datetime(2019, 3, 20), 30, ["2019-02", "2019-03", "2019-04", "2019-05"]
    ) == ["2019-03", "2019-04"]


def test_temporal_cutoffs_must_be_unique_and_increasing() -> None:
    cutoffs = [datetime(2019, 2, 1), datetime(2019, 3, 1)]
    assert validate_cutoffs(cutoffs) == cutoffs

    with pytest.raises(ValueError, match="strictly increasing"):
        validate_cutoffs(list(reversed(cutoffs)))
    with pytest.raises(ValueError, match="at least two"):
        validate_cutoffs([cutoffs[0]])
    with pytest.raises(ValueError, match="distinct snapshot dates"):
        validate_cutoffs([datetime(2019, 2, 1), datetime(2019, 2, 1, 12)])
