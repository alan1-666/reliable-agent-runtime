package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"safemarket/agent-runtime/internal/outbox"
)

type Repository struct {
	pool *pgxpool.Pool
}

var _ outbox.Repository = (*Repository)(nil)

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) LeaseBatch(
	ctx context.Context,
	publisherID string,
	now time.Time,
	ttl time.Duration,
	limit int,
) ([]outbox.Message, error) {
	if publisherID == "" || ttl <= 0 || limit <= 0 {
		return nil, outbox.ErrInvalidConfig
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM outbox_events
			WHERE published_at IS NULL
			  AND next_attempt_at <= $1
			  AND (lease_until IS NULL OR lease_until <= $1)
			ORDER BY next_attempt_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE outbox_events AS events
		SET lease_owner = $3,
		    lease_until = $4,
		    lease_token = events.lease_token + 1,
		    attempts = events.attempts + 1,
		    last_error = NULL
		FROM candidates
		WHERE events.id = candidates.id
		RETURNING events.id, events.aggregate_type, events.aggregate_id,
		          events.event_type, events.payload, events.attempts,
		          events.created_at, events.lease_owner, events.lease_until,
		          events.lease_token`,
		now, limit, publisherID, now.Add(ttl))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := make([]outbox.Message, 0, limit)
	for rows.Next() {
		var message outbox.Message
		if err := rows.Scan(
			&message.ID,
			&message.AggregateType,
			&message.AggregateID,
			&message.EventType,
			&message.Payload,
			&message.Attempts,
			&message.CreatedAt,
			&message.LeaseOwner,
			&message.LeaseUntil,
			&message.LeaseToken,
		); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return nil, outbox.ErrNoMessages
	}
	return messages, nil
}

func (r *Repository) MarkPublished(ctx context.Context, messageID int64, publisherID string, leaseToken uint64, now time.Time) error {
	command, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = $4, lease_owner = NULL, lease_until = NULL, last_error = NULL
		WHERE id = $1 AND published_at IS NULL AND lease_owner = $2
		  AND lease_token = $3 AND lease_until > $4`,
		messageID, publisherID, leaseToken, now)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return outbox.ErrLeaseLost
	}
	return nil
}

func (r *Repository) MarkRetry(
	ctx context.Context,
	messageID int64,
	publisherID string,
	leaseToken uint64,
	nextAttempt time.Time,
	message string,
) error {
	command, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET next_attempt_at = $4, lease_owner = NULL, lease_until = NULL, last_error = $5
		WHERE id = $1 AND published_at IS NULL AND lease_owner = $2 AND lease_token = $3`,
		messageID, publisherID, leaseToken, nextAttempt, message)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return outbox.ErrLeaseLost
	}
	return nil
}
