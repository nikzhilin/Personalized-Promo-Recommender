CREATE TABLE pipeline_runs (
    run_id              UUID PRIMARY KEY,
    dag_id              TEXT NOT NULL,
    airflow_run_id      TEXT,
    status              TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    feature_cutoff      TIMESTAMPTZ NOT NULL,
    config_fingerprint CHAR(64) NOT NULL,
    current_task        TEXT,
    snapshot_id         TEXT,
    propensity_run_id   TEXT,
    uplift_run_id       TEXT,
    metrics             JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_summary       TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX pipeline_runs_status_started_idx
    ON pipeline_runs(status, started_at DESC);

