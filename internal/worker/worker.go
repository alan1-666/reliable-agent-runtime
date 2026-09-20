package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/store"
)

var (
	ErrInvalidConfig   = errors.New("invalid worker configuration")
	ErrMissingTerminal = errors.New("engine event stream closed without terminal result")
	ErrInvalidTerminal = errors.New("engine emitted an invalid terminal event")
)

type Engine interface {
	Run(context.Context, domain.RunRequest) <-chan domain.RunEvent
}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type Config struct {
	WorkerID     string
	LeaseTTL     time.Duration
	RenewEvery   time.Duration
	PollInterval time.Duration
}

func DefaultConfig(workerID string) Config {
	return Config{
		WorkerID:     workerID,
		LeaseTTL:     30 * time.Second,
		RenewEvery:   10 * time.Second,
		PollInterval: 500 * time.Millisecond,
	}
}

type Worker struct {
	repository store.Repository
	engine     Engine
	clock      Clock
	config     Config
}

type Option func(*Worker)

func WithClock(clock Clock) Option {
	return func(worker *Worker) { worker.clock = clock }
}

func New(repository store.Repository, engine Engine, config Config, options ...Option) (*Worker, error) {
	worker := &Worker{
		repository: repository,
		engine:     engine,
		clock:      realClock{},
		config:     config,
	}
	for _, option := range options {
		option(worker)
	}
	if repository == nil || engine == nil || worker.clock == nil || config.WorkerID == "" ||
		config.LeaseTTL <= 0 || config.RenewEvery <= 0 || config.RenewEvery >= config.LeaseTTL ||
		config.PollInterval <= 0 {
		return nil, ErrInvalidConfig
	}
	return worker, nil
}

// RunOnce leases and executes at most one Run. The bool is false when the queue is empty.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	lease, err := w.repository.LeaseNext(ctx, w.config.WorkerID, w.clock.Now(), w.config.LeaseTTL)
	if errors.Is(err, store.ErrNoRunAvailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, w.executeLease(ctx, lease)
}

// Run polls until its context is cancelled. Run-level failures do not stop later queue items.
func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()
	for {
		executed, err := w.RunOnce(ctx)
		if err != nil && !errors.Is(err, store.ErrLeaseLost) {
			return err
		}
		if executed {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) executeLease(ctx context.Context, lease store.Lease) error {
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	heartbeatCtx, stopHeartbeat := context.WithCancel(runCtx)
	defer stopHeartbeat()
	heartbeatErrors := make(chan error, 1)
	go w.renewLease(heartbeatCtx, lease, heartbeatErrors)

	request := requestFrom(lease.Run)
	events := w.engine.Run(runCtx, request)
	for events != nil {
		select {
		case err := <-heartbeatErrors:
			cancelRun()
			if err == nil {
				return store.ErrLeaseLost
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			if event.Type == domain.EventRunCreated {
				continue
			}
			if terminalEvent(event.Type) {
				stopHeartbeat()
				return w.finish(runCtx, lease, event)
			}
			persisted, err := w.repository.AppendEvent(
				runCtx,
				lease.Run.ID,
				w.config.WorkerID,
				lease.FenceToken,
				event.Type,
				event.Payload,
				w.clock.Now(),
			)
			if err != nil {
				cancelRun()
				return err
			}
			if shouldCheckpoint(event.Type) {
				if err := w.saveCheckpoint(runCtx, lease, event, persisted.Sequence); err != nil {
					cancelRun()
					return err
				}
			}
		}
	}
	return ErrMissingTerminal
}

func (w *Worker) renewLease(ctx context.Context, lease store.Lease, failures chan<- error) {
	ticker := time.NewTicker(w.config.RenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := w.repository.RenewLease(
				ctx,
				lease.Run.ID,
				w.config.WorkerID,
				lease.FenceToken,
				w.clock.Now(),
				w.config.LeaseTTL,
			)
			if err != nil {
				select {
				case failures <- fmt.Errorf("renew lease: %w", err):
				default:
				}
				return
			}
		}
	}
}

func (w *Worker) finish(ctx context.Context, lease store.Lease, event domain.RunEvent) error {
	var result domain.FinalResult
	if err := json.Unmarshal(event.Payload, &result); err != nil {
		return fmt.Errorf("%w: decode result: %v", ErrInvalidTerminal, err)
	}
	if err := store.ValidateTerminalEvent(result, event.Type); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTerminal, err)
	}
	_, err := w.repository.FinishRun(
		ctx,
		lease.Run.ID,
		w.config.WorkerID,
		lease.FenceToken,
		result,
		event.Type,
		w.clock.Now(),
	)
	return err
}

func (w *Worker) saveCheckpoint(
	ctx context.Context,
	lease store.Lease,
	event domain.RunEvent,
	persistedSequence uint64,
) error {
	state, err := json.Marshal(struct {
		Attempt        int              `json:"attempt"`
		EngineSequence uint64           `json:"engine_sequence"`
		LastEvent      domain.EventType `json:"last_event"`
	}{
		Attempt:        lease.Attempt,
		EngineSequence: event.Sequence,
		LastEvent:      event.Type,
	})
	if err != nil {
		return err
	}
	return w.repository.SaveCheckpoint(ctx, w.config.WorkerID, store.Checkpoint{
		RunID:         lease.Run.ID,
		FenceToken:    lease.FenceToken,
		EventSequence: persistedSequence,
		State:         state,
		CreatedAt:     w.clock.Now(),
	})
}

func requestFrom(record store.RunRecord) domain.RunRequest {
	return domain.RunRequest{
		RunID:          record.ID,
		TenantID:       record.TenantID,
		IdempotencyKey: record.IdempotencyKey,
		Input:          record.Input,
		Mode:           record.Mode,
		Deadline:       record.Deadline,
		Manifest:       record.Manifest,
	}
}

func terminalEvent(eventType domain.EventType) bool {
	switch eventType {
	case domain.EventRunSucceeded,
		domain.EventRunFailed,
		domain.EventRunCancelled,
		domain.EventRunInconclusive,
		domain.EventRunTimedOut:
		return true
	default:
		return false
	}
}

func shouldCheckpoint(eventType domain.EventType) bool {
	switch eventType {
	case domain.EventModelStarted,
		domain.EventUsageRecorded,
		domain.EventToolRequested,
		domain.EventToolCompleted,
		domain.EventToolFailed:
		return true
	default:
		return false
	}
}
