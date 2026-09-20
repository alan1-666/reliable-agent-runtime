BEGIN;

CREATE TABLE runs (
    id                text PRIMARY KEY,
    tenant_id         text NOT NULL,
    idempotency_key   text NOT NULL,
    request_hash      text NOT NULL,
    input             text NOT NULL,
    execution_mode    text NOT NULL CHECK (execution_mode IN ('LIVE', 'SHADOW', 'EVAL', 'REPLAY')),
    manifest          jsonb NOT NULL,
    status            text NOT NULL CHECK (status IN (
                          'QUEUED', 'RUNNING', 'WAITING_APPROVAL', 'RETRY_WAIT',
                          'SUCCEEDED', 'FAILED', 'CANCELLED', 'INCONCLUSIVE', 'TIMED_OUT'
                      )),
    deadline_at       timestamptz,
    next_attempt_at   timestamptz,
    lease_owner       text,
    lease_until       timestamptz,
    fence_token       bigint NOT NULL DEFAULT 0 CHECK (fence_token >= 0),
    attempt_no        integer NOT NULL DEFAULT 0 CHECK (attempt_no >= 0),
    last_event_seq    bigint NOT NULL DEFAULT 0 CHECK (last_event_seq >= 0),
    final_result      jsonb,
    cancel_requested_at timestamptz,
    created_at        timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL,
    UNIQUE (tenant_id, idempotency_key),
    CHECK (
        (status = 'RUNNING' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
        OR status <> 'RUNNING'
    )
);

CREATE INDEX runs_lease_candidates_idx
    ON runs (COALESCE(next_attempt_at, created_at), created_at, id)
    WHERE status IN ('QUEUED', 'RETRY_WAIT', 'RUNNING');

CREATE TABLE run_attempts (
    id             bigserial PRIMARY KEY,
    run_id         text NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    attempt_no     integer NOT NULL CHECK (attempt_no > 0),
    worker_id      text NOT NULL,
    fence_token    bigint NOT NULL CHECK (fence_token > 0),
    status         text NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'LEASE_EXPIRED', 'CANCELLED')),
    started_at     timestamptz NOT NULL,
    finished_at    timestamptz,
    error          text,
    UNIQUE (run_id, attempt_no),
    UNIQUE (run_id, fence_token)
);

CREATE TABLE run_events (
    run_id       text NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    seq          bigint NOT NULL CHECK (seq > 0),
    event_type   text NOT NULL,
    payload      jsonb,
    occurred_at  timestamptz NOT NULL,
    PRIMARY KEY (run_id, seq)
);

CREATE TABLE checkpoints (
    run_id       text PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    fence_token  bigint NOT NULL CHECK (fence_token > 0),
    event_seq    bigint NOT NULL CHECK (event_seq >= 0),
    state        jsonb NOT NULL,
    created_at   timestamptz NOT NULL
);

CREATE TABLE outbox_events (
    id               bigserial PRIMARY KEY,
    aggregate_type   text NOT NULL,
    aggregate_id     text NOT NULL,
    event_type       text NOT NULL,
    payload          jsonb NOT NULL,
    attempts         integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at  timestamptz NOT NULL,
    published_at     timestamptz,
    created_at       timestamptz NOT NULL
);

CREATE INDEX outbox_events_unpublished_idx
    ON outbox_events (next_attempt_at, id)
    WHERE published_at IS NULL;

COMMIT;
