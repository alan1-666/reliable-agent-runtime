package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/engine"
	"safemarket/agent-runtime/internal/model"
	"safemarket/agent-runtime/internal/model/fake"
	"safemarket/agent-runtime/internal/store"
	"safemarket/agent-runtime/internal/tool"
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
	state, err := engine.DecodeState(checkpoint.State)
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != engine.PhaseModel || state.Turn != 1 || len(state.Messages) != 1 {
		t.Fatalf("unexpected checkpoint state: %+v", state)
	}

	executed, err = worker.RunOnce(context.Background())
	if err != nil || executed {
		t.Fatalf("empty queue: executed=%v err=%v", executed, err)
	}
}

func TestNewAttemptResumesAfterCompletedToolWithoutRepeatingIt(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	memory := store.NewMemoryRepository()
	request := runRequest("run-resume")
	if _, _, err := memory.CreateRun(context.Background(), request, now); err != nil {
		t.Fatal(err)
	}
	toolExecutions := 0
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Function{
		Def: tool.Definition{
			Name:        "echo",
			InputSchema: json.RawMessage(`{"type":"object"}`),
			ReadOnly:    true,
		},
		Run: func(context.Context, json.RawMessage) (tool.Result, error) {
			toolExecutions++
			return tool.Result{Content: `{"ok":true}`}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	firstAdapter := fake.Scripted(
		[]model.Event{{Type: model.EventToolCall, ToolCall: model.ToolCall{
			ID: "call-1", Name: "echo", Arguments: json.RawMessage(`{}`),
		}}},
		[]model.Event{{Type: model.EventCompleted, Output: "should not persist"}},
	)
	injected := errors.New("simulated persistence outage")
	failingRepository := &failModelStartRepository{Repository: memory, failure: injected}
	firstRuntime := engine.New(firstAdapter, engine.WithTools(registry))
	firstWorker, err := New(failingRepository, firstRuntime, testConfig("worker-a"), WithClock(fixedClock{now: now}))
	if err != nil {
		t.Fatal(err)
	}
	if executed, err := firstWorker.RunOnce(context.Background()); !executed || !errors.Is(err, injected) {
		t.Fatalf("first attempt: executed=%v err=%v", executed, err)
	}
	if toolExecutions != 1 {
		t.Fatalf("tool executions after first attempt = %d", toolExecutions)
	}
	checkpoint, err := memory.GetCheckpoint(context.Background(), request.RunID)
	if err != nil {
		t.Fatal(err)
	}
	state, err := engine.DecodeState(checkpoint.State)
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != engine.PhaseTools || state.NextToolIndex != 1 {
		t.Fatalf("unexpected recovery state: %+v", state)
	}

	secondAdapter := fake.Successful("recovered")
	secondRuntime := engine.New(secondAdapter, engine.WithTools(registry))
	secondWorker, err := New(memory, secondRuntime, testConfig("worker-b"), WithClock(fixedClock{now: now.Add(2 * time.Minute)}))
	if err != nil {
		t.Fatal(err)
	}
	if executed, err := secondWorker.RunOnce(context.Background()); !executed || err != nil {
		t.Fatalf("second attempt: executed=%v err=%v", executed, err)
	}
	if toolExecutions != 1 {
		t.Fatalf("completed tool was repeated: executions=%d", toolExecutions)
	}
	requests := secondAdapter.Requests()
	if len(requests) != 1 || requests[0].Turn != 2 {
		t.Fatalf("resume model requests: %+v", requests)
	}
	record, err := memory.GetRun(context.Background(), request.RunID)
	if err != nil || record.FinalResult == nil || record.FinalResult.Output != "recovered" || record.Attempt != 2 {
		t.Fatalf("final record=%+v err=%v", record, err)
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

type failModelStartRepository struct {
	store.Repository
	modelStarts int
	failure     error
}

func (repository *failModelStartRepository) AppendEvent(
	ctx context.Context,
	runID, workerID string,
	fence uint64,
	eventType domain.EventType,
	payload json.RawMessage,
	now time.Time,
) (domain.RunEvent, error) {
	if eventType == domain.EventModelStarted {
		repository.modelStarts++
		if repository.modelStarts == 2 {
			return domain.RunEvent{}, fmt.Errorf("persist model start: %w", repository.failure)
		}
	}
	return repository.Repository.AppendEvent(ctx, runID, workerID, fence, eventType, payload, now)
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
