CREATE TABLE prediction_events (
    event_id       UUID PRIMARY KEY,
    request_id     UUID NOT NULL UNIQUE,
    client_id      TEXT NOT NULL,
    snapshot_id    TEXT NOT NULL,
    payload         JSONB NOT NULL,
    is_fallback    BOOLEAN NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX ix_prediction_events_created_at ON prediction_events(created_at);
CREATE INDEX ix_prediction_events_client_id ON prediction_events(client_id);

CREATE TABLE feedback_events (
    event_id            UUID PRIMARY KEY,
    request_id          UUID NOT NULL,
    client_id           TEXT NOT NULL,
    product_id          TEXT NOT NULL,
    event_type          TEXT NOT NULL CHECK (event_type IN ('click', 'cart', 'purchase')),
    shown_discount      NUMERIC(5,4) NOT NULL CHECK (
        shown_discount >= 0 AND shown_discount < 1
    ),
    purchase_value      NUMERIC(12,2) CHECK (purchase_value >= 0),
    discount_cost       NUMERIC(12,2) CHECK (discount_cost >= 0),
    created_at          TIMESTAMPTZ NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    verification_status TEXT NOT NULL CHECK (
        verification_status IN ('VERIFIED', 'UNVERIFIED_MISSING_REQUEST')
    ),
    event_fingerprint   CHAR(64) NOT NULL
);

CREATE INDEX ix_feedback_events_created_at ON feedback_events(created_at);
CREATE INDEX ix_feedback_events_received_at ON feedback_events(received_at);
CREATE INDEX ix_feedback_events_request_id ON feedback_events(request_id);
