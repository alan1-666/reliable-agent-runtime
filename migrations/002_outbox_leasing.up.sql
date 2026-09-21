BEGIN;

ALTER TABLE outbox_events
    ADD COLUMN lease_owner text,
    ADD COLUMN lease_until timestamptz,
    ADD COLUMN lease_token bigint NOT NULL DEFAULT 0 CHECK (lease_token >= 0),
    ADD COLUMN last_error text;

CREATE INDEX outbox_events_lease_idx
    ON outbox_events (next_attempt_at, lease_until, id)
    WHERE published_at IS NULL;

COMMIT;
