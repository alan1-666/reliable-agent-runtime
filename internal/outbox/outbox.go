package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNoMessages    = errors.New("no outbox messages available")
	ErrLeaseLost     = errors.New("outbox lease lost")
	ErrInvalidConfig = errors.New("invalid outbox publisher configuration")
)

type Message struct {
	ID            int64
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       json.RawMessage
	Attempts      int
	CreatedAt     time.Time
	LeaseOwner    string
	LeaseUntil    time.Time
	LeaseToken    uint64
}

type Repository interface {
	LeaseBatch(context.Context, string, time.Time, time.Duration, int) ([]Message, error)
	MarkPublished(context.Context, int64, string, uint64, time.Time) error
	MarkRetry(context.Context, int64, string, uint64, time.Time, string) error
}

type Sink interface {
	Publish(context.Context, Message) error
}
