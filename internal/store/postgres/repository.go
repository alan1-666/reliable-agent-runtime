package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/store"
)

type Repository struct {
	pool *pgxpool.Pool
}

var _ store.Repository = (*Repository)(nil)

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) CreateRun(ctx context.Context, request domain.RunRequest, now time.Time) (store.RunRecord, bool, error) {
	hash, err := store.RequestHash(request)
	if err != nil {
		return store.RunRecord{}, false, err
	}
	manifest, err := json.Marshal(request.Manifest)
	if err != nil {
		return store.RunRecord{}, false, err
	}
	createdPayload, err := json.Marshal(struct {
		Mode     domain.ExecutionMode   `json:"mode"`
		Manifest domain.ReleaseManifest `json:"manifest"`
	}{Mode: request.Mode, Manifest: request.Manifest})
	if err != nil {
		return store.RunRecord{}, false, err
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return store.RunRecord{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx, `
		INSERT INTO runs (
			id, tenant_id, idempotency_key, request_hash, input, execution_mode,
			manifest, status, deadline_at, last_event_seq, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'QUEUED', $8, 1, $9, $9)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
		RETURNING id, tenant_id, idempotency_key, request_hash, input, execution_mode,
		          manifest, status, deadline_at, lease_owner, lease_until, fence_token,
		          attempt_no, last_event_seq, final_result, created_at, updated_at`,
		request.RunID, request.TenantID, request.IdempotencyKey, hash, request.Input,
		request.Mode, manifest, nullableTime(request.Deadline), now,
	)
	record, scanErr := scanRun(row)
	created := scanErr == nil
	if scanErr != nil && !errors.Is(scanErr, pgx.ErrNoRows) {
		return store.RunRecord{}, false, scanErr
	}
	if !created {
		record, err = getRunByIdempotency(ctx, tx, request.TenantID, request.IdempotencyKey)
		if err != nil {
			return store.RunRecord{}, false, err
		}
		if record.RequestHash != hash {
			return store.RunRecord{}, false, store.ErrIdempotencyConflict
		}
	} else {
		if err := insertEventAndOutbox(ctx, tx, domain.RunEvent{
			RunID:      record.ID,
			Sequence:   1,
			Type:       domain.EventRunCreated,
			OccurredAt: now,
			Payload:    createdPayload,
		}); err != nil {
			return store.RunRecord{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return store.RunRecord{}, false, err
	}
	return record, created, nil
}

func (r *Repository) GetRun(ctx context.Context, runID string) (store.RunRecord, error) {
	record, err := scanRun(r.pool.QueryRow(ctx, runSelect+` WHERE id = $1`, runID))
	return record, mapNotFound(err)
}

func (r *Repository) LeaseNext(ctx context.Context, workerID string, now time.Time, ttl time.Duration) (store.Lease, error) {
	if workerID == "" || ttl <= 0 {
		return store.Lease{}, store.ErrInvalidTransition
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return store.Lease{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var runID string
	var previousStatus domain.RunStatus
	err = tx.QueryRow(ctx, `
		SELECT id, status
		FROM runs
		WHERE (
			(status IN ('QUEUED', 'RETRY_WAIT') AND (next_attempt_at IS NULL OR next_attempt_at <= $1))
			OR (status = 'RUNNING' AND lease_until <= $1)
		)
		AND (deadline_at IS NULL OR deadline_at > $1)
		ORDER BY COALESCE(next_attempt_at, created_at), created_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`, now).Scan(&runID, &previousStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Lease{}, store.ErrNoRunAvailable
	}
	if err != nil {
		return store.Lease{}, err
	}
	if previousStatus == domain.RunStatusRunning {
		if _, err := tx.Exec(ctx, `
			UPDATE run_attempts
			SET status = 'LEASE_EXPIRED', finished_at = $2, error = 'lease expired'
			WHERE run_id = $1 AND status = 'RUNNING'`, runID, now); err != nil {
			return store.Lease{}, err
		}
	}

	leaseUntil := now.Add(ttl)
	record, err := scanRun(tx.QueryRow(ctx, `
		UPDATE runs
		SET status = 'RUNNING', lease_owner = $2, lease_until = $3,
		    fence_token = fence_token + 1, attempt_no = attempt_no + 1,
		    next_attempt_at = NULL, updated_at = $4
		WHERE id = $1
		RETURNING id, tenant_id, idempotency_key, request_hash, input, execution_mode,
		          manifest, status, deadline_at, lease_owner, lease_until, fence_token,
		          attempt_no, last_event_seq, final_result, created_at, updated_at`,
		runID, workerID, leaseUntil, now))
	if err != nil {
		return store.Lease{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO run_attempts (
			run_id, attempt_no, worker_id, fence_token, status, started_at
		) VALUES ($1, $2, $3, $4, 'RUNNING', $5)`,
		record.ID, record.Attempt, workerID, record.FenceToken, now); err != nil {
		return store.Lease{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.Lease{}, err
	}
	return store.Lease{
		Run:        record,
		WorkerID:   workerID,
		FenceToken: record.FenceToken,
		Attempt:    record.Attempt,
		LeaseUntil: record.LeaseUntil,
	}, nil
}

func (r *Repository) RenewLease(ctx context.Context, runID, workerID string, fence uint64, now time.Time, ttl time.Duration) (time.Time, error) {
	if workerID == "" || ttl <= 0 {
		return time.Time{}, store.ErrInvalidTransition
	}
	leaseUntil := now.Add(ttl)
	command, err := r.pool.Exec(ctx, `
		UPDATE runs
		SET lease_until = $5, updated_at = $4
		WHERE id = $1 AND status = 'RUNNING' AND lease_owner = $2
		  AND fence_token = $3 AND lease_until > $4`,
		runID, workerID, fence, now, leaseUntil)
	if err != nil {
		return time.Time{}, err
	}
	if command.RowsAffected() != 1 {
		return time.Time{}, store.ErrLeaseLost
	}
	return leaseUntil, nil
}

func (r *Repository) AppendEvent(
	ctx context.Context,
	runID, workerID string,
	fence uint64,
	eventType domain.EventType,
	payload json.RawMessage,
	now time.Time,
) (domain.RunEvent, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.RunEvent{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	sequence, err := advanceSequence(ctx, tx, runID, workerID, fence, now)
	if err != nil {
		return domain.RunEvent{}, err
	}
	event := domain.RunEvent{RunID: runID, Sequence: sequence, Type: eventType, OccurredAt: now, Payload: payload}
	if err := insertEventAndOutbox(ctx, tx, event); err != nil {
		return domain.RunEvent{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.RunEvent{}, err
	}
	return event, nil
}

func (r *Repository) SaveCheckpoint(ctx context.Context, workerID string, checkpoint store.Checkpoint) error {
	if workerID == "" {
		return store.ErrLeaseLost
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var lastEventSeq uint64
	err = tx.QueryRow(ctx, `
		SELECT last_event_seq
		FROM runs
		WHERE id = $1 AND status = 'RUNNING' AND lease_owner = $2
		  AND fence_token = $3 AND lease_until > $4
		FOR UPDATE`, checkpoint.RunID, workerID, checkpoint.FenceToken, checkpoint.CreatedAt).Scan(&lastEventSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if checkpoint.EventSequence > lastEventSeq {
		return store.ErrCheckpointAhead
	}
	var existingSequence uint64
	err = tx.QueryRow(ctx, `SELECT event_seq FROM checkpoints WHERE run_id = $1`, checkpoint.RunID).Scan(&existingSequence)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil && existingSequence > checkpoint.EventSequence {
		return store.ErrStaleCheckpoint
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO checkpoints (run_id, fence_token, event_seq, state, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (run_id) DO UPDATE
		SET fence_token = EXCLUDED.fence_token,
		    event_seq = EXCLUDED.event_seq,
		    state = EXCLUDED.state,
		    created_at = EXCLUDED.created_at`,
		checkpoint.RunID, checkpoint.FenceToken, checkpoint.EventSequence, checkpoint.State, checkpoint.CreatedAt)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) GetCheckpoint(ctx context.Context, runID string) (store.Checkpoint, error) {
	var checkpoint store.Checkpoint
	err := r.pool.QueryRow(ctx, `
		SELECT run_id, fence_token, event_seq, state, created_at
		FROM checkpoints WHERE run_id = $1`, runID).Scan(
		&checkpoint.RunID, &checkpoint.FenceToken, &checkpoint.EventSequence,
		&checkpoint.State, &checkpoint.CreatedAt,
	)
	return checkpoint, mapNotFound(err)
}

func (r *Repository) FinishRun(
	ctx context.Context,
	runID, workerID string,
	fence uint64,
	result domain.FinalResult,
	eventType domain.EventType,
	now time.Time,
) (domain.RunEvent, error) {
	if !store.IsTerminal(result.Status) {
		return domain.RunEvent{}, store.ErrInvalidTransition
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return domain.RunEvent{}, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.RunEvent{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var sequence uint64
	var attempt int
	err = tx.QueryRow(ctx, `
		UPDATE runs
		SET status = $5, final_result = $6, last_event_seq = last_event_seq + 1,
		    lease_owner = NULL, lease_until = NULL, updated_at = $4
		WHERE id = $1 AND status = 'RUNNING' AND lease_owner = $2
		  AND fence_token = $3 AND lease_until > $4
		RETURNING last_event_seq, attempt_no`,
		runID, workerID, fence, now, result.Status, resultJSON).Scan(&sequence, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RunEvent{}, store.ErrLeaseLost
	}
	if err != nil {
		return domain.RunEvent{}, err
	}
	attemptStatus := attemptStatusFor(result.Status)
	if _, err := tx.Exec(ctx, `
		UPDATE run_attempts
		SET status = $3, finished_at = $4, error = $5
		WHERE run_id = $1 AND attempt_no = $2 AND status = 'RUNNING'`,
		runID, attempt, attemptStatus, now, nullableString(result.Error)); err != nil {
		return domain.RunEvent{}, err
	}
	event := domain.RunEvent{RunID: runID, Sequence: sequence, Type: eventType, OccurredAt: now, Payload: resultJSON}
	if err := insertEventAndOutbox(ctx, tx, event); err != nil {
		return domain.RunEvent{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.RunEvent{}, err
	}
	return event, nil
}

func (r *Repository) ListEvents(ctx context.Context, runID string, afterSequence uint64) ([]domain.RunEvent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT run_id, seq, event_type, occurred_at, payload
		FROM run_events
		WHERE run_id = $1 AND seq > $2
		ORDER BY seq`, runID, afterSequence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]domain.RunEvent, 0)
	for rows.Next() {
		var event domain.RunEvent
		if err := rows.Scan(&event.RunID, &event.Sequence, &event.Type, &event.OccurredAt, &event.Payload); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		var exists bool
		if err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runs WHERE id = $1)`, runID).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			return nil, store.ErrNotFound
		}
	}
	return events, nil
}

const runSelect = `
	SELECT id, tenant_id, idempotency_key, request_hash, input, execution_mode,
	       manifest, status, deadline_at, lease_owner, lease_until, fence_token,
	       attempt_no, last_event_seq, final_result, created_at, updated_at
	FROM runs`

type rowScanner interface {
	Scan(...any) error
}

func scanRun(row rowScanner) (store.RunRecord, error) {
	var record store.RunRecord
	var manifestJSON []byte
	var finalJSON []byte
	var deadline *time.Time
	var leaseOwner *string
	var leaseUntil *time.Time
	err := row.Scan(
		&record.ID, &record.TenantID, &record.IdempotencyKey, &record.RequestHash,
		&record.Input, &record.Mode, &manifestJSON, &record.Status, &deadline,
		&leaseOwner, &leaseUntil, &record.FenceToken, &record.Attempt,
		&record.LastEventSeq, &finalJSON, &record.CreatedAt, &record.UpdatedAt,
	)
	if err != nil {
		return store.RunRecord{}, err
	}
	if err := json.Unmarshal(manifestJSON, &record.Manifest); err != nil {
		return store.RunRecord{}, fmt.Errorf("decode run manifest: %w", err)
	}
	if len(finalJSON) > 0 {
		var result domain.FinalResult
		if err := json.Unmarshal(finalJSON, &result); err != nil {
			return store.RunRecord{}, fmt.Errorf("decode final result: %w", err)
		}
		record.FinalResult = &result
	}
	if deadline != nil {
		record.Deadline = *deadline
	}
	if leaseOwner != nil {
		record.LeaseOwner = *leaseOwner
	}
	if leaseUntil != nil {
		record.LeaseUntil = *leaseUntil
	}
	return record, nil
}

func getRunByIdempotency(ctx context.Context, tx pgx.Tx, tenantID, key string) (store.RunRecord, error) {
	record, err := scanRun(tx.QueryRow(ctx, runSelect+` WHERE tenant_id = $1 AND idempotency_key = $2`, tenantID, key))
	return record, mapNotFound(err)
}

func advanceSequence(ctx context.Context, tx pgx.Tx, runID, workerID string, fence uint64, now time.Time) (uint64, error) {
	if workerID == "" {
		return 0, store.ErrLeaseLost
	}
	var sequence uint64
	err := tx.QueryRow(ctx, `
		UPDATE runs
		SET last_event_seq = last_event_seq + 1, updated_at = $4
		WHERE id = $1 AND status = 'RUNNING' AND lease_owner = $2
		  AND fence_token = $3 AND lease_until > $4
		RETURNING last_event_seq`, runID, workerID, fence, now).Scan(&sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, store.ErrLeaseLost
	}
	return sequence, err
}

func insertEventAndOutbox(ctx context.Context, tx pgx.Tx, event domain.RunEvent) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO run_events (run_id, seq, event_type, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5)`,
		event.RunID, event.Sequence, event.Type, nullableJSON(event.Payload), event.OccurredAt); err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (
			aggregate_type, aggregate_id, event_type, payload, next_attempt_at, created_at
		) VALUES ('RUN', $1, $2, $3, $4, $4)`,
		event.RunID, event.Type, payload, event.OccurredAt)
	return err
}

func mapNotFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	}
	return err
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableJSON(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func attemptStatusFor(status domain.RunStatus) string {
	switch status {
	case domain.RunStatusSucceeded:
		return "SUCCEEDED"
	case domain.RunStatusCancelled:
		return "CANCELLED"
	default:
		return "FAILED"
	}
}
