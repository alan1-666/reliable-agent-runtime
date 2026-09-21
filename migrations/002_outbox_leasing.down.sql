BEGIN;

DROP INDEX IF EXISTS outbox_events_lease_idx;

ALTER TABLE outbox_events
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS lease_token,
    DROP COLUMN IF EXISTS lease_until,
    DROP COLUMN IF EXISTS lease_owner;

COMMIT;
