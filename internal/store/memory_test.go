package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"safemarket/agent-runtime/internal/domain"
)

func TestCreateRunIsIdempotentAndDetectsConflict(t *testing.T) {
	repository := NewMemoryRepository()
	now := time.Unix(100, 0).UTC()
	request := storeRequest("run-1", "key-1")

	first, created, err := repository.CreateRun(context.Background(), request, now)
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}
	events, err := repository.ListEvents(context.Background(), first.ID, 0)
	if err != nil || len(events) != 1 || events[0].Type != domain.EventRunCreated || events[0].Sequence != 1 {
		t.Fatalf("created events = %+v, err=%v", events, err)
	}
	second, created, err := repository.CreateRun(context.Background(), request, now.Add(time.Second))
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("idempotent create: record=%+v created=%v err=%v", second, created, err)
	}
	events, err = repository.ListEvents(context.Background(), first.ID, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("idempotent create duplicated event: events=%+v err=%v", events, err)
	}

	request.RunID = "run-2"
	request.Input = "different"
	if _, _, err := repository.CreateRun(context.Background(), request, now); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want idempotency conflict", err)
	}
}

func TestExpiredLeaseIsFencedFromWriting(t *testing.T) {
	repository := NewMemoryRepository()
	now := time.Unix(100, 0).UTC()
	mustCreate(t, repository, storeRequest("run-1", "key-1"), now)

	first, err := repository.LeaseNext(context.Background(), "worker-a", now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.LeaseNext(context.Background(), "worker-b", now.Add(2*time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.FenceToken <= first.FenceToken || second.Attempt != first.Attempt+1 {
		t.Fatalf("lease did not advance: first=%+v second=%+v", first, second)
	}

	if _, err := repository.AppendEvent(
		context.Background(), "run-1", "worker-a", first.FenceToken,
		domain.EventModelStarted, json.RawMessage(`{}`), now.Add(2*time.Second),
	); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old worker error = %v, want lease lost", err)
	}
	if _, err := repository.AppendEvent(
		context.Background(), "run-1", "worker-b", second.FenceToken,
		domain.EventModelStarted, json.RawMessage(`{}`), now.Add(2500*time.Millisecond),
	); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointAndFinishAreFencedAndMonotonic(t *testing.T) {
	repository := NewMemoryRepository()
	now := time.Unix(100, 0).UTC()
	mustCreate(t, repository, storeRequest("run-1", "key-1"), now)
	lease, err := repository.LeaseNext(context.Background(), "worker-a", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	event, err := repository.AppendEvent(
		context.Background(), "run-1", "worker-a", lease.FenceToken,
		domain.EventModelStarted, json.RawMessage(`{"turn":1}`), now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := Checkpoint{
		RunID:         "run-1",
		FenceToken:    lease.FenceToken,
		EventSequence: event.Sequence,
		State:         json.RawMessage(`{"turn":1}`),
		CreatedAt:     now.Add(2 * time.Second),
	}
	if err := repository.SaveCheckpoint(context.Background(), "worker-a", checkpoint); err != nil {
		t.Fatal(err)
	}
	stale := checkpoint
	stale.EventSequence = 0
	if err := repository.SaveCheckpoint(context.Background(), "worker-a", stale); !errors.Is(err, ErrStaleCheckpoint) {
		t.Fatalf("error = %v, want stale checkpoint", err)
	}
	ahead := checkpoint
	ahead.EventSequence = event.Sequence + 1
	if err := repository.SaveCheckpoint(context.Background(), "worker-a", ahead); !errors.Is(err, ErrCheckpointAhead) {
		t.Fatalf("error = %v, want checkpoint ahead", err)
	}
	if _, err := repository.AppendEvent(
		context.Background(), "run-1", "", lease.FenceToken,
		domain.EventModelDelta, json.RawMessage(`{}`), now.Add(2500*time.Millisecond),
	); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("empty worker error = %v, want lease lost", err)
	}

	result := domain.FinalResult{Status: domain.RunStatusSucceeded, Output: "done"}
	finalEvent, err := repository.FinishRun(
		context.Background(), "run-1", "worker-a", lease.FenceToken,
		result, domain.EventRunSucceeded, now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if finalEvent.Sequence != event.Sequence+1 {
		t.Fatalf("final sequence = %d, want %d", finalEvent.Sequence, event.Sequence+1)
	}
	if _, err := repository.AppendEvent(
		context.Background(), "run-1", "worker-a", lease.FenceToken,
		domain.EventModelDelta, json.RawMessage(`{}`), now.Add(4*time.Second),
	); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("terminal append error = %v, want lease lost", err)
	}
	record, err := repository.GetRun(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != domain.RunStatusSucceeded || record.FinalResult == nil || record.FinalResult.Output != "done" {
		t.Fatalf("unexpected final record: %+v", record)
	}
}

func TestOnlyOneWorkerCanLeaseQueuedRun(t *testing.T) {
	repository := NewMemoryRepository()
	now := time.Unix(100, 0).UTC()
	mustCreate(t, repository, storeRequest("run-1", "key-1"), now)

	const workers = 16
	var wait sync.WaitGroup
	wait.Add(workers)
	winners := make(chan Lease, workers)
	for i := 0; i < workers; i++ {
		go func(index int) {
			defer wait.Done()
			lease, err := repository.LeaseNext(context.Background(), string(rune('a'+index)), now, time.Minute)
			if err == nil {
				winners <- lease
				return
			}
			if !errors.Is(err, ErrNoRunAvailable) {
				t.Errorf("unexpected lease error: %v", err)
			}
		}(i)
	}
	wait.Wait()
	close(winners)

	count := 0
	for range winners {
		count++
	}
	if count != 1 {
		t.Fatalf("lease winners = %d, want 1", count)
	}
}

func storeRequest(runID, key string) domain.RunRequest {
	return domain.RunRequest{
		RunID:          runID,
		TenantID:       "tenant-1",
		IdempotencyKey: key,
		Input:          "hello",
		Mode:           domain.ExecutionModeEval,
		Manifest: domain.ReleaseManifest{
			ReleaseID: "release-1",
			Checksum:  "sha256:test",
		},
	}
}

func mustCreate(t *testing.T, repository Repository, request domain.RunRequest, now time.Time) RunRecord {
	t.Helper()
	record, created, err := repository.CreateRun(context.Background(), request, now)
	if err != nil || !created {
		t.Fatalf("create: record=%+v created=%v err=%v", record, created, err)
	}
	return record
}
