CREATE TABLE feedback_export_state (
    export_name            TEXT PRIMARY KEY,
    watermark_received_at  TIMESTAMPTZ NOT NULL,
    watermark_event_id     UUID NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO feedback_export_state(
    export_name, watermark_received_at, watermark_event_id
) VALUES (
    'hdfs_feedback',
    TIMESTAMPTZ '1970-01-01 00:00:00+00',
    UUID '00000000-0000-0000-0000-000000000000'
);
