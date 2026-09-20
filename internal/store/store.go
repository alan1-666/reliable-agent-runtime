package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"safemarket/agent-runtime/internal/domain"
)

var (
	ErrNotFound            = errors.New("run not found")
	ErrNoRunAvailable      = errors.New("no run available")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different request")
	ErrLeaseLost           = errors.New("run lease lost")
	ErrInvalidTransition   = errors.New("invalid run state transition")
	ErrStaleCheckpoint     = errors.New("checkpoint is older than current checkpoint")
	ErrCheckpointAhead     = errors.New("checkpoint references an event that is not committed")
)

type RunRecord struct {
	ID             string
	TenantID       string
	IdempotencyKey string
	RequestHash    string
	Input          string
	Mode           domain.ExecutionMode
	Manifest       domain.ReleaseManifest
	Status         domain.RunStatus
	Deadline       time.Time
	LeaseOwner     string
	LeaseUntil     time.Time
	FenceToken     uint64
	Attempt        int
	LastEventSeq   uint64
	FinalResult    *domain.FinalResult
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Lease struct {
	Run        RunRecord
	WorkerID   string
	FenceToken uint64
	Attempt    int
	LeaseUntil time.Time
}

type Checkpoint struct {
	RunID         string
	FenceToken    uint64
	EventSequence uint64
	State         json.RawMessage
	CreatedAt     time.Time
}

type Repository interface {
	CreateRun(context.Context, domain.RunRequest, time.Time) (RunRecord, bool, error)
	GetRun(context.Context, string) (RunRecord, error)
	LeaseNext(context.Context, string, time.Time, time.Duration) (Lease, error)
	RenewLease(context.Context, string, string, uint64, time.Time, time.Duration) (time.Time, error)
	AppendEvent(context.Context, string, string, uint64, domain.EventType, json.RawMessage, time.Time) (domain.RunEvent, error)
	SaveCheckpoint(context.Context, string, Checkpoint) error
	GetCheckpoint(context.Context, string) (Checkpoint, error)
	FinishRun(context.Context, string, string, uint64, domain.FinalResult, domain.EventType, time.Time) (domain.RunEvent, error)
	ListEvents(context.Context, string, uint64) ([]domain.RunEvent, error)
}

func IsTerminal(status domain.RunStatus) bool {
	switch status {
	case domain.RunStatusSucceeded,
		domain.RunStatusFailed,
		domain.RunStatusCancelled,
		domain.RunStatusInconclusive,
		domain.RunStatusTimedOut:
		return true
	default:
		return false
	}
}
