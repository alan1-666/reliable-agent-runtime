package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/engine"
	"safemarket/agent-runtime/internal/model/fake"
	"safemarket/agent-runtime/internal/store"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

func TestRunOncePersistsEngineEventsCheckpointAndTerminal(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	repository := store.NewMemoryRepository()
	request := runRequest("run-1")
	if _, created, err := repository.CreateRun(context.Background(), request, now); err != nil || !created {
		t.Fatalf("create run: created=%v err=%v", created, err)
	}
	runtime := engine.NewWithClock(fake.Successful("done"), fixedClock{now: now.Add(time.Second)})
	worker, err := New(repository, runtime, testConfig("worker-a"), WithClock(fixedClock{now: now.Add(time.Second)}))
	if err != nil {
		t.Fatal(err)
	}

	executed, err := worker.RunOnce(context.Background())
	if err != nil || !executed {
		t.Fatalf("run once: executed=%v err=%v", executed, err)
	}
	record, err := repository.GetRun(context.Background(), request.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != domain.RunStatusSucceeded || record.FinalResult == nil || record.FinalResult.Output != "done" {
		t.Fatalf("unexpected final record: %+v", record)
	}
	events, err := repository.ListEvents(context.Background(), request.RunID, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []domain.EventType{
		domain.EventRunCreated,
		domain.EventModelStarted,
		domain.EventModelDelta,
		domain.EventRunSucceeded,
	}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", eventTypes(events), want)
	}
	for index := range want {
		if events[index].Type != want[index] || events[index].Sequence != uint64(index+1) {
			t.Fatalf("events = %+v", events)
		}
	}
	checkpoint, err := repository.GetCheckpoint(context.Background(), request.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.EventSequence != 2 {
		t.Fatalf("checkpoint sequence = %d, want 2", checkpoint.EventSequence)
	}
	var state struct {
		LastEvent domain.EventType `json:"last_event"`
	}
	if err := json.Unmarshal(checkpoint.State, &state); err != nil {
		t.Fatal(err)
	}
	if state.LastEvent != domain.EventModelStarted {
		t.Fatalf("checkpoint last event = %s", state.LastEvent)
	}

	executed, err = worker.RunOnce(context.Background())
	if err != nil || executed {
		t.Fatalf("empty queue: executed=%v err=%v", executed, err)
	}
}

func TestRunOnceStopsWhenLeaseIsStolen(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	memory := store.NewMemoryRepository()
	request := runRequest("run-1")
	if _, _, err := memory.CreateRun(context.Background(), request, now); err != nil {
		t.Fatal(err)
	}
	repository := &stealingRepository{
		Repository: memory,
		memory:     memory,
		stealAt:    now.Add(2 * time.Minute),
	}
	runtime := engine.NewWithClock(fake.Successful("unused"), fixedClock{now: now.Add(time.Second)})
	worker, err := New(repository, runtime, testConfig("worker-a"), WithClock(fixedClock{now: now}))
	if err != nil {
		t.Fatal(err)
	}

	executed, err := worker.RunOnce(context.Background())
	if !executed || !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("executed=%v err=%v, want lease lost", executed, err)
	}
	record, err := memory.GetRun(context.Background(), request.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.LeaseOwner != "worker-b" || record.FenceToken != 2 {
		t.Fatalf("run was not stolen: %+v", record)
	}
}

func TestNewRejectsInvalidLeaseTiming(t *testing.T) {
	config := testConfig("worker-a")
	config.RenewEvery = config.LeaseTTL
	_, err := New(store.NewMemoryRepository(), engine.New(fake.Successful("done")), config)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v", err)
	}
}

type stealingRepository struct {
	store.Repository
	memory  *store.MemoryRepository
	stealAt time.Time
	stolen  bool
}

func (repository *stealingRepository) AppendEvent(
	ctx context.Context,
	runID, workerID string,
	fence uint64,
	eventType domain.EventType,
	payload json.RawMessage,
	now time.Time,
) (domain.RunEvent, error) {
	if !repository.stolen {
		repository.stolen = true
		if _, err := repository.memory.LeaseNext(ctx, "worker-b", repository.stealAt, time.Minute); err != nil {
			return domain.RunEvent{}, err
		}
	}
	return repository.Repository.AppendEvent(ctx, runID, workerID, fence, eventType, payload, now)
}

func runRequest(runID string) domain.RunRequest {
	return domain.RunRequest{
		RunID:          runID,
		TenantID:       "tenant-1",
		IdempotencyKey: "key-" + runID,
		Input:          "hello",
		Mode:           domain.ExecutionModeEval,
		Manifest: domain.ReleaseManifest{
			ReleaseID: "release-1",
			Checksum:  "sha256:test",
		},
	}
}

func testConfig(workerID string) Config {
	return Config{
		WorkerID:     workerID,
		LeaseTTL:     time.Minute,
		RenewEvery:   20 * time.Second,
		PollInterval: time.Millisecond,
	}
}

func eventTypes(events []domain.RunEvent) []domain.EventType {
	result := make([]domain.EventType, 0, len(events))
	for _, event := range events {
		result = append(result, event.Type)
	}
	return result
}
