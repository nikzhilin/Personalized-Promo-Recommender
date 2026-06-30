CREATE TABLE fallback_recommendations (
    snapshot_id     TEXT NOT NULL,
    product_id      TEXT NOT NULL,
    rank            INTEGER NOT NULL CHECK (rank > 0 AND rank <= 100),
    discount        NUMERIC(5,4) NOT NULL CHECK (discount >= 0 AND discount < 1),
    expected_profit DOUBLE PRECISION,
    recsys_score    DOUBLE PRECISION NOT NULL CHECK (recsys_score >= 0),
    reason_code     TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_id, rank),
    UNIQUE (snapshot_id, product_id)
);

CREATE TABLE fallback_snapshot_state (
    singleton       BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    snapshot_id     TEXT NOT NULL,
    activated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_fallback_recommendations_snapshot
    ON fallback_recommendations(snapshot_id, rank);
