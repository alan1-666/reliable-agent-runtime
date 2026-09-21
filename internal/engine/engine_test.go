package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/model"
	"safemarket/agent-runtime/internal/model/fake"
	"safemarket/agent-runtime/internal/tool"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestRunCompletesWithOrderedEvents(t *testing.T) {
	adapter := fake.Successful("done")
	engine := NewWithClock(adapter, fixedClock{now: time.Unix(1, 0).UTC()})

	events := collect(engine.Run(context.Background(), validRequest()))
	wantTypes := []domain.EventType{
		domain.EventRunCreated,
		domain.EventModelStarted,
		domain.EventModelDelta,
		domain.EventRunSucceeded,
	}

	if len(events) != len(wantTypes) {
		t.Fatalf("got %d events, want %d", len(events), len(wantTypes))
	}
	for i, want := range wantTypes {
		if events[i].Type != want {
			t.Fatalf("event %d type = %s, want %s", i, events[i].Type, want)
		}
		if events[i].Sequence != uint64(i+1) {
			t.Fatalf("event %d sequence = %d, want %d", i, events[i].Sequence, i+1)
		}
	}

	var result domain.FinalResult
	if err := json.Unmarshal(events[len(events)-1].Payload, &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != domain.RunStatusSucceeded || result.Output != "done" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRunExecutesToolAndFeedsResultIntoNextTurn(t *testing.T) {
	adapter := fake.Scripted(
		[]model.Event{{
			Type: model.EventToolCall,
			ToolCall: model.ToolCall{
				ID:        "call-1",
				Name:      "echo",
				Arguments: json.RawMessage(`{"message":"hello"}`),
			},
		}},
		[]model.Event{{Type: model.EventCompleted, Output: "tool said hello"}},
	)
	registry := tool.NewRegistry()
	if err := registry.Register(echoTool()); err != nil {
		t.Fatal(err)
	}
	runtime := New(adapter, WithTools(registry))

	events := collect(runtime.Run(context.Background(), validRequest()))
	wantTypes := []domain.EventType{
		domain.EventRunCreated,
		domain.EventModelStarted,
		domain.EventToolRequested,
		domain.EventToolCompleted,
		domain.EventModelStarted,
		domain.EventRunSucceeded,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("got event types %v, want %v", eventTypes(events), wantTypes)
	}
	for i, want := range wantTypes {
		if events[i].Type != want {
			t.Fatalf("event %d type = %s, want %s", i, events[i].Type, want)
		}
	}

	requests := adapter.Requests()
	if len(requests) != 2 {
		t.Fatalf("model requests = %d, want 2", len(requests))
	}
	secondMessages := requests[1].Messages
	last := secondMessages[len(secondMessages)-1]
	if last.Role != "tool" || last.ToolCallID != "call-1" || !strings.Contains(last.Content, "hello") {
		t.Fatalf("unexpected tool result message: %+v", last)
	}
}

func TestRunFailsForUnknownTool(t *testing.T) {
	adapter := fake.Scripted([]model.Event{{
		Type: model.EventToolCall,
		ToolCall: model.ToolCall{
			ID:        "call-1",
			Name:      "missing",
			Arguments: json.RawMessage(`{}`),
		},
	}})
	runtime := New(adapter)

	events := collect(runtime.Run(context.Background(), validRequest()))
	if len(events) < 2 || events[len(events)-2].Type != domain.EventToolFailed || events[len(events)-1].Type != domain.EventRunFailed {
		t.Fatalf("unexpected event types: %v", eventTypes(events))
	}
}

func TestRunEnforcesToolCallBudget(t *testing.T) {
	adapter := fake.Scripted([]model.Event{
		{Type: model.EventToolCall, ToolCall: model.ToolCall{ID: "call-1", Name: "echo", Arguments: json.RawMessage(`{}`)}},
		{Type: model.EventToolCall, ToolCall: model.ToolCall{ID: "call-2", Name: "echo", Arguments: json.RawMessage(`{}`)}},
	})
	runtime := New(adapter, WithLimits(Limits{MaxTurns: 2, MaxToolCalls: 1}))

	events := collect(runtime.Run(context.Background(), validRequest()))
	if events[len(events)-1].Type != domain.EventRunFailed {
		t.Fatalf("unexpected event types: %v", eventTypes(events))
	}
	if !strings.Contains(string(events[len(events)-1].Payload), ErrMaxToolCalls.Error()) {
		t.Fatalf("unexpected failure payload: %s", events[len(events)-1].Payload)
	}
}

func TestRunRecordsUsageAndStopsWhenCostBudgetExceeded(t *testing.T) {
	adapter := fake.Scripted([]model.Event{
		{Type: model.EventUsage, Usage: model.Usage{InputTokens: 100, OutputTokens: 20, CostMicros: 200}},
		{Type: model.EventCompleted, Output: "too expensive"},
	})
	runtime := New(adapter, WithLimits(Limits{MaxTurns: 2, MaxToolCalls: 1, MaxCostMicros: 100}))

	events := collect(runtime.Run(context.Background(), validRequest()))
	wantTail := []domain.EventType{domain.EventUsageRecorded, domain.EventRunInconclusive}
	if len(events) < 2 || events[len(events)-2].Type != wantTail[0] || events[len(events)-1].Type != wantTail[1] {
		t.Fatalf("unexpected event types: %v", eventTypes(events))
	}
	if !strings.Contains(string(events[len(events)-1].Payload), ErrCostBudget.Error()) {
		t.Fatalf("unexpected failure payload: %s", events[len(events)-1].Payload)
	}
}

func TestRunHonorsDeadline(t *testing.T) {
	request := validRequest()
	request.Deadline = time.Now().Add(20 * time.Millisecond)
	runtime := New(&fake.Adapter{Block: true})

	events := collect(runtime.Run(context.Background(), request))
	if events[len(events)-1].Type != domain.EventRunTimedOut {
		t.Fatalf("unexpected event types: %v", eventTypes(events))
	}
}

func TestRunRejectsInvalidRequest(t *testing.T) {
	engine := New(fake.Successful("unused"))
	events := collect(engine.Run(context.Background(), domain.RunRequest{}))

	if len(events) != 1 || events[0].Type != domain.EventRunFailed {
		t.Fatalf("unexpected events: %+v", events)
	}
}

func TestRunPropagatesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	engine := New(&fake.Adapter{Block: true})
	eventStream := engine.Run(ctx, validRequest())

	first := <-eventStream
	if first.Type != domain.EventRunCreated {
		t.Fatalf("first event = %s", first.Type)
	}
	second := <-eventStream
	if second.Type != domain.EventModelStarted {
		t.Fatalf("second event = %s", second.Type)
	}
	cancel()

	remaining := collect(eventStream)
	if len(remaining) != 1 {
		t.Fatalf("unexpected remaining events: %+v", remaining)
	}
	if remaining[0].Type != domain.EventRunCancelled {
		t.Fatalf("last event = %s", remaining[0].Type)
	}
}

func TestResumeExecutesOnlyRemainingReadOnlyTools(t *testing.T) {
	adapter := fake.Successful("recovered")
	registry := tool.NewRegistry()
	executions := 0
	if err := registry.Register(tool.Function{
		Def: tool.Definition{Name: "echo", InputSchema: json.RawMessage(`{"type":"object"}`), ReadOnly: true},
		Run: func(context.Context, json.RawMessage) (tool.Result, error) {
			executions++
			return tool.Result{Content: "ok"}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	request := validRequest()
	state := ExecutionState{
		Version: StateVersion, RunID: request.RunID, ManifestChecksum: request.Manifest.Checksum,
		Phase: PhaseTools, Turn: 1, ToolCallsUsed: 2, NextToolIndex: 1,
		Messages: []model.Message{{Role: "user", Content: request.Input}},
		PendingToolCalls: []model.ToolCall{
			{ID: "done", Name: "echo", Arguments: json.RawMessage(`{}`)},
			{ID: "remaining", Name: "echo", Arguments: json.RawMessage(`{}`)},
		},
	}
	runtime := New(adapter, WithTools(registry))
	outputs := collectOutputs(runtime.Execute(context.Background(), request, &state))
	if executions != 1 {
		t.Fatalf("remaining tool executions = %d, want 1", executions)
	}
	if len(outputs) < 4 || outputs[0].Event.Type != domain.EventToolRequested ||
		outputs[1].Event.Type != domain.EventToolCompleted ||
		outputs[len(outputs)-1].Event.Type != domain.EventRunSucceeded {
		t.Fatalf("unexpected output types: %v", outputTypes(outputs))
	}
	requests := adapter.Requests()
	if len(requests) != 1 || requests[0].Turn != 2 {
		t.Fatalf("model requests = %+v", requests)
	}
}

func TestResumeDoesNotReplayWriteTool(t *testing.T) {
	registry := tool.NewRegistry()
	executions := 0
	if err := registry.Register(tool.Function{
		Def: tool.Definition{Name: "write", InputSchema: json.RawMessage(`{"type":"object"}`), ReadOnly: false},
		Run: func(context.Context, json.RawMessage) (tool.Result, error) {
			executions++
			return tool.Result{}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	request := validRequest()
	state := ExecutionState{
		Version: StateVersion, RunID: request.RunID, ManifestChecksum: request.Manifest.Checksum,
		Phase: PhaseTools, Turn: 1, ToolCallsUsed: 1,
		Messages:         []model.Message{{Role: "user", Content: request.Input}},
		PendingToolCalls: []model.ToolCall{{ID: "write-1", Name: "write", Arguments: json.RawMessage(`{}`)}},
	}
	outputs := collectOutputs(New(fake.Successful("unused"), WithTools(registry)).Execute(context.Background(), request, &state))
	if executions != 0 || len(outputs) != 1 || outputs[0].Event.Type != domain.EventRunInconclusive {
		t.Fatalf("executions=%d output types=%v", executions, outputTypes(outputs))
	}
	if !strings.Contains(string(outputs[0].Event.Payload), ErrUnsafeToolRecovery.Error()) {
		t.Fatalf("unexpected payload: %s", outputs[0].Event.Payload)
	}
}

func validRequest() domain.RunRequest {
	return domain.RunRequest{
		RunID:          "run-1",
		TenantID:       "tenant-1",
		IdempotencyKey: "request-1",
		Input:          "hello",
		Mode:           domain.ExecutionModeEval,
		Manifest: domain.ReleaseManifest{
			ReleaseID: "release-1",
			Checksum:  "sha256:test",
		},
	}
}

func collect(stream <-chan domain.RunEvent) []domain.RunEvent {
	var events []domain.RunEvent
	for event := range stream {
		events = append(events, event)
	}
	return events
}

func collectOutputs(stream <-chan Output) []Output {
	var outputs []Output
	for output := range stream {
		outputs = append(outputs, output)
	}
	return outputs
}

func outputTypes(outputs []Output) []domain.EventType {
	result := make([]domain.EventType, 0, len(outputs))
	for _, output := range outputs {
		result = append(result, output.Event.Type)
	}
	return result
}

func eventTypes(events []domain.RunEvent) []domain.EventType {
	types := make([]domain.EventType, 0, len(events))
	for _, event := range events {
		types = append(types, event.Type)
	}
	return types
}

func echoTool() tool.Function {
	return tool.Function{
		Def: tool.Definition{
			Name:        "echo",
			Description: "returns the supplied JSON",
			InputSchema: json.RawMessage(`{"type":"object"}`),
			ReadOnly:    true,
		},
		Run: func(_ context.Context, arguments json.RawMessage) (tool.Result, error) {
			return tool.Result{Content: string(arguments)}, nil
		},
	}
}
