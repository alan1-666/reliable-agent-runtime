package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"safemarket/agent-runtime/internal/domain"
)

type MemoryRepository struct {
	mu            sync.Mutex
	runs          map[string]RunRecord
	byIdempotency map[string]string
	events        map[string][]domain.RunEvent
	checkpoints   map[string]Checkpoint
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		runs:          make(map[string]RunRecord),
		byIdempotency: make(map[string]string),
		events:        make(map[string][]domain.RunEvent),
		checkpoints:   make(map[string]Checkpoint),
	}
}

func (r *MemoryRepository) CreateRun(_ context.Context, request domain.RunRequest, now time.Time) (RunRecord, bool, error) {
	hash, err := RequestHash(request)
	if err != nil {
		return RunRecord{}, false, err
	}
	createdPayload, err := json.Marshal(struct {
		Mode     domain.ExecutionMode   `json:"mode"`
		Manifest domain.ReleaseManifest `json:"manifest"`
	}{Mode: request.Mode, Manifest: request.Manifest})
	if err != nil {
		return RunRecord{}, false, err
	}
	key := request.TenantID + "\x00" + request.IdempotencyKey

	r.mu.Lock()
	defer r.mu.Unlock()
	if runID, exists := r.byIdempotency[key]; exists {
		existing := r.runs[runID]
		if existing.RequestHash != hash {
			return RunRecord{}, false, ErrIdempotencyConflict
		}
		return cloneRun(existing), false, nil
	}
	if _, exists := r.runs[request.RunID]; exists {
		return RunRecord{}, false, fmt.Errorf("run id already exists: %s", request.RunID)
	}

	record := RunRecord{
		ID:             request.RunID,
		TenantID:       request.TenantID,
		IdempotencyKey: request.IdempotencyKey,
		RequestHash:    hash,
		Input:          request.Input,
		Mode:           request.Mode,
		Manifest:       cloneManifest(request.Manifest),
		Status:         domain.RunStatusQueued,
		Deadline:       request.Deadline,
		LastEventSeq:   1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	r.runs[record.ID] = record
	r.byIdempotency[key] = record.ID
	r.events[record.ID] = []domain.RunEvent{{
		RunID:      record.ID,
		Sequence:   1,
		Type:       domain.EventRunCreated,
		OccurredAt: now,
		Payload:    createdPayload,
	}}
	return cloneRun(record), true, nil
}

func (r *MemoryRepository) GetRun(_ context.Context, runID string) (RunRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, exists := r.runs[runID]
	if !exists {
		return RunRecord{}, ErrNotFound
	}
	return cloneRun(record), nil
}

func (r *MemoryRepository) LeaseNext(_ context.Context, workerID string, now time.Time, ttl time.Duration) (Lease, error) {
	if workerID == "" || ttl <= 0 {
		return Lease{}, ErrInvalidTransition
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	candidates := make([]RunRecord, 0)
	for _, record := range r.runs {
		available := record.Status == domain.RunStatusQueued || record.Status == domain.RunStatusRetryWait
		expired := record.Status == domain.RunStatusRunning && !record.LeaseUntil.After(now)
		if (available || expired) && (record.Deadline.IsZero() || record.Deadline.After(now)) {
			candidates = append(candidates, record)
		}
	}
	if len(candidates) == 0 {
		return Lease{}, ErrNoRunAvailable
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].ID < candidates[j].ID
		}
		return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
	})

	record := r.runs[candidates[0].ID]
	record.Status = domain.RunStatusRunning
	record.LeaseOwner = workerID
	record.LeaseUntil = now.Add(ttl)
	record.FenceToken++
	record.Attempt++
	record.UpdatedAt = now
	r.runs[record.ID] = record
	return Lease{
		Run:        cloneRun(record),
		WorkerID:   workerID,
		FenceToken: record.FenceToken,
		Attempt:    record.Attempt,
		LeaseUntil: record.LeaseUntil,
	}, nil
}

func (r *MemoryRepository) RenewLease(_ context.Context, runID, workerID string, fence uint64, now time.Time, ttl time.Duration) (time.Time, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.validLease(runID, workerID, fence, now)
	if err != nil {
		return time.Time{}, err
	}
	if ttl <= 0 {
		return time.Time{}, ErrInvalidTransition
	}
	record.LeaseUntil = now.Add(ttl)
	record.UpdatedAt = now
	r.runs[runID] = record
	return record.LeaseUntil, nil
}

func (r *MemoryRepository) AppendEvent(
	_ context.Context,
	runID, workerID string,
	fence uint64,
	eventType domain.EventType,
	payload json.RawMessage,
	now time.Time,
) (domain.RunEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.validLease(runID, workerID, fence, now)
	if err != nil {
		return domain.RunEvent{}, err
	}
	event := appendEvent(record, eventType, payload, now)
	record.LastEventSeq = event.Sequence
	record.UpdatedAt = now
	r.runs[runID] = record
	r.events[runID] = append(r.events[runID], event)
	return cloneEvent(event), nil
}

func (r *MemoryRepository) SaveCheckpoint(_ context.Context, workerID string, checkpoint Checkpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.validLease(checkpoint.RunID, workerID, checkpoint.FenceToken, checkpoint.CreatedAt)
	if err != nil {
		return err
	}
	if checkpoint.EventSequence > record.LastEventSeq {
		return ErrCheckpointAhead
	}
	if existing, exists := r.checkpoints[checkpoint.RunID]; exists && existing.EventSequence > checkpoint.EventSequence {
		return ErrStaleCheckpoint
	}
	checkpoint.State = append(json.RawMessage(nil), checkpoint.State...)
	r.checkpoints[checkpoint.RunID] = checkpoint
	return nil
}

func (r *MemoryRepository) GetCheckpoint(_ context.Context, runID string) (Checkpoint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	checkpoint, exists := r.checkpoints[runID]
	if !exists {
		return Checkpoint{}, ErrNotFound
	}
	checkpoint.State = append(json.RawMessage(nil), checkpoint.State...)
	return checkpoint, nil
}

func (r *MemoryRepository) FinishRun(
	_ context.Context,
	runID, workerID string,
	fence uint64,
	result domain.FinalResult,
	eventType domain.EventType,
	now time.Time,
) (domain.RunEvent, error) {
	if err := ValidateTerminalEvent(result, eventType); err != nil {
		return domain.RunEvent{}, err
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return domain.RunEvent{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.validLease(runID, workerID, fence, now)
	if err != nil {
		return domain.RunEvent{}, err
	}
	event := appendEvent(record, eventType, payload, now)
	record.Status = result.Status
	record.LastEventSeq = event.Sequence
	record.FinalResult = &result
	record.LeaseOwner = ""
	record.LeaseUntil = time.Time{}
	record.UpdatedAt = now
	r.runs[runID] = record
	r.events[runID] = append(r.events[runID], event)
	return cloneEvent(event), nil
}

func (r *MemoryRepository) ListEvents(_ context.Context, runID string, afterSequence uint64) ([]domain.RunEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.runs[runID]; !exists {
		return nil, ErrNotFound
	}
	var result []domain.RunEvent
	for _, event := range r.events[runID] {
		if event.Sequence > afterSequence {
			result = append(result, cloneEvent(event))
		}
	}
	return result, nil
}

func (r *MemoryRepository) validLease(runID, workerID string, fence uint64, now time.Time) (RunRecord, error) {
	record, exists := r.runs[runID]
	if !exists {
		return RunRecord{}, ErrNotFound
	}
	if workerID == "" || record.Status != domain.RunStatusRunning || record.LeaseOwner != workerID || record.FenceToken != fence || !record.LeaseUntil.After(now) {
		return RunRecord{}, ErrLeaseLost
	}
	return record, nil
}

func appendEvent(record RunRecord, eventType domain.EventType, payload json.RawMessage, now time.Time) domain.RunEvent {
	return domain.RunEvent{
		RunID:      record.ID,
		Sequence:   record.LastEventSeq + 1,
		Type:       eventType,
		OccurredAt: now,
		Payload:    append(json.RawMessage(nil), payload...),
	}
}

func cloneRun(record RunRecord) RunRecord {
	record.Manifest = cloneManifest(record.Manifest)
	if record.FinalResult != nil {
		result := *record.FinalResult
		record.FinalResult = &result
	}
	return record
}

func cloneManifest(manifest domain.ReleaseManifest) domain.ReleaseManifest {
	manifest.SkillVersions = append([]string(nil), manifest.SkillVersions...)
	return manifest
}

func cloneEvent(event domain.RunEvent) domain.RunEvent {
	event.Payload = append(json.RawMessage(nil), event.Payload...)
	return event
}
