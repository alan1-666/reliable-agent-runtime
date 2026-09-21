package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/model"
	"safemarket/agent-runtime/internal/tool"
)

var (
	ErrInvalidRequest     = errors.New("invalid run request")
	ErrMaxTurns           = errors.New("maximum turns exceeded")
	ErrMaxToolCalls       = errors.New("maximum tool calls exceeded")
	ErrInputTokens        = errors.New("input token budget exceeded")
	ErrOutputTokens       = errors.New("output token budget exceeded")
	ErrCostBudget         = errors.New("cost budget exceeded")
	ErrEmptyModelTurn     = errors.New("model turn ended without final result or tool call")
	ErrUnsafeToolRecovery = errors.New("cannot automatically replay a non-read-only tool after recovery")
)

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type Limits struct {
	MaxTurns        int
	MaxToolCalls    int
	MaxInputTokens  int
	MaxOutputTokens int
	MaxCostMicros   int64
}

func DefaultLimits() Limits {
	return Limits{MaxTurns: 8, MaxToolCalls: 32}
}

type Option func(*Engine)

func WithClock(clock Clock) Option {
	return func(engine *Engine) { engine.clock = clock }
}

func WithTools(registry *tool.Registry) Option {
	return func(engine *Engine) { engine.tools = registry }
}

func WithLimits(limits Limits) Option {
	return func(engine *Engine) { engine.limits = limits }
}

type Engine struct {
	model  model.Adapter
	tools  *tool.Registry
	clock  Clock
	limits Limits
}

func New(adapter model.Adapter, options ...Option) *Engine {
	engine := &Engine{
		model:  adapter,
		tools:  tool.NewRegistry(),
		clock:  realClock{},
		limits: DefaultLimits(),
	}
	for _, option := range options {
		option(engine)
	}
	return engine
}

func NewWithClock(adapter model.Adapter, clock Clock) *Engine {
	return New(adapter, WithClock(clock))
}

func (e *Engine) Run(ctx context.Context, request domain.RunRequest) <-chan domain.RunEvent {
	output := make(chan domain.RunEvent, 32)
	execution := e.Execute(ctx, request, nil)
	go func() {
		defer close(output)
		for item := range execution {
			output <- item.Event
		}
	}()
	return output
}

func (e *Engine) Execute(ctx context.Context, request domain.RunRequest, checkpoint *ExecutionState) <-chan Output {
	output := make(chan Output, 32)
	runCtx := ctx
	cancel := func() {}
	if !request.Deadline.IsZero() {
		runCtx, cancel = context.WithDeadline(ctx, request.Deadline)
	}

	go func() {
		defer cancel()
		defer close(output)

		sequence := uint64(0)
		state := InitialState(request)
		resumed := checkpoint != nil
		if resumed {
			state = cloneState(*checkpoint)
		}
		emit := func(eventType domain.EventType, payload any, checkpointState *ExecutionState) bool {
			sequence++
			encoded, _ := json.Marshal(payload)
			item := Output{
				Event: domain.RunEvent{
					RunID:      request.RunID,
					Sequence:   sequence,
					Type:       eventType,
					OccurredAt: e.clock.Now(),
					Payload:    encoded,
				},
			}
			if checkpointState != nil {
				cloned := cloneState(*checkpointState)
				item.Checkpoint = &cloned
			}
			select {
			case <-runCtx.Done():
				return false
			case output <- item:
				return true
			}
		}
		emitTerminal := func(eventType domain.EventType, payload any) {
			sequence++
			encoded, _ := json.Marshal(payload)
			output <- Output{Event: domain.RunEvent{
				RunID: request.RunID, Sequence: sequence, Type: eventType,
				OccurredAt: e.clock.Now(), Payload: encoded,
			}}
		}
		plainEmit := func(eventType domain.EventType, payload any) bool {
			return emit(eventType, payload, nil)
		}
		fail := func(err error) {
			plainEmit(domain.EventRunFailed, domain.FinalResult{Status: domain.RunStatusFailed, Error: err.Error()})
		}
		inconclusive := func(err error) {
			plainEmit(domain.EventRunInconclusive, domain.FinalResult{Status: domain.RunStatusInconclusive, Error: err.Error()})
		}
		terminateContext := func(err error) {
			if errors.Is(err, context.DeadlineExceeded) {
				emitTerminal(domain.EventRunTimedOut, domain.FinalResult{Status: domain.RunStatusTimedOut, Error: err.Error()})
				return
			}
			emitTerminal(domain.EventRunCancelled, domain.FinalResult{Status: domain.RunStatusCancelled, Error: err.Error()})
		}

		if err := validate(request); err != nil {
			fail(err)
			return
		}
		if e.limits.MaxTurns <= 0 || e.limits.MaxToolCalls < 0 ||
			e.limits.MaxInputTokens < 0 || e.limits.MaxOutputTokens < 0 || e.limits.MaxCostMicros < 0 {
			fail(ErrInvalidRequest)
			return
		}
		if err := validateState(state, request); err != nil {
			fail(err)
			return
		}
		if err := runCtx.Err(); err != nil {
			terminateContext(err)
			return
		}

		if !resumed {
			if !plainEmit(domain.EventRunCreated, map[string]string{
				"release_id": request.Manifest.ReleaseID,
				"mode":       string(request.Mode),
			}) {
				return
			}
		}

		toolDefinitions := toModelToolDefinitions(e.tools.Definitions())
		recoveringTools := resumed && state.Phase == PhaseTools
		for {
			if state.Phase == PhaseTools {
				if !e.executePendingTools(runCtx, &state, recoveringTools, emit, terminateContext, fail, inconclusive) {
					return
				}
				recoveringTools = false
				state.Phase = PhaseModel
				state.PendingToolCalls = nil
				state.NextToolIndex = 0
				state.Turn++
				continue
			}
			if state.Turn > e.limits.MaxTurns {
				fail(ErrMaxTurns)
				return
			}
			if !emit(domain.EventModelStarted, map[string]any{
				"adapter": e.model.Name(),
				"turn":    state.Turn,
			}, &state) {
				return
			}

			result, err := e.runModelTurn(runCtx, request.RunID, state.Turn, state.Messages, toolDefinitions, plainEmit)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					terminateContext(err)
				} else {
					fail(err)
				}
				return
			}
			state.Usage.InputTokens += result.usage.InputTokens
			state.Usage.OutputTokens += result.usage.OutputTokens
			state.Usage.CostMicros += result.usage.CostMicros
			if result.usage != (model.Usage{}) {
				if !emit(domain.EventUsageRecorded, map[string]any{
					"turn":          state.Turn,
					"input_tokens":  result.usage.InputTokens,
					"output_tokens": result.usage.OutputTokens,
					"cost_micros":   result.usage.CostMicros,
					"total":         state.Usage,
				}, &state) {
					return
				}
			}
			if budgetErr := exceededBudget(e.limits, state.Usage); budgetErr != nil {
				inconclusive(budgetErr)
				return
			}

			if result.finalOutput != nil {
				plainEmit(domain.EventRunSucceeded, domain.FinalResult{Status: domain.RunStatusSucceeded, Output: *result.finalOutput})
				return
			}
			if len(result.toolCalls) == 0 {
				fail(ErrEmptyModelTurn)
				return
			}

			if state.ToolCallsUsed+len(result.toolCalls) > e.limits.MaxToolCalls {
				fail(ErrMaxToolCalls)
				return
			}
			state.ToolCallsUsed += len(result.toolCalls)
			state.Messages = append(state.Messages, model.Message{
				Role:      "assistant",
				Content:   result.assistantText,
				ToolCalls: result.toolCalls,
			})
			state.Phase = PhaseTools
			state.PendingToolCalls = cloneToolCalls(result.toolCalls)
			state.NextToolIndex = 0
		}
	}()

	return output
}

func (e *Engine) executePendingTools(
	ctx context.Context,
	state *ExecutionState,
	recovering bool,
	emit func(domain.EventType, any, *ExecutionState) bool,
	terminateContext func(error),
	fail func(error),
	inconclusive func(error),
) bool {
	for state.NextToolIndex < len(state.PendingToolCalls) {
		call := state.PendingToolCalls[state.NextToolIndex]
		if recovering {
			definition, exists := e.tools.Definition(call.Name)
			if !exists || !definition.ReadOnly {
				inconclusive(fmt.Errorf("%w: %s", ErrUnsafeToolRecovery, call.Name))
				return false
			}
		}
		if !emit(domain.EventToolRequested, map[string]any{
			"tool_call_id": call.ID,
			"name":         call.Name,
			"arguments":    json.RawMessage(call.Arguments),
		}, state) {
			return false
		}
		toolResult, err := e.tools.Execute(ctx, call.Name, call.Arguments)
		if err != nil {
			if ctx.Err() != nil {
				terminateContext(ctx.Err())
				return false
			}
			emit(domain.EventToolFailed, map[string]string{
				"tool_call_id": call.ID, "name": call.Name, "error": err.Error(),
			}, nil)
			fail(fmt.Errorf("execute tool %s: %w", call.Name, err))
			return false
		}
		state.Messages = append(state.Messages, model.Message{
			Role: "tool", Content: toolResult.Content, ToolCallID: call.ID, ToolName: call.Name,
		})
		state.NextToolIndex++
		if !emit(domain.EventToolCompleted, map[string]string{
			"tool_call_id": call.ID, "name": call.Name, "content": toolResult.Content,
		}, state) {
			return false
		}
	}
	return true
}

type turnResult struct {
	assistantText string
	toolCalls     []model.ToolCall
	finalOutput   *string
	usage         model.Usage
}

func (e *Engine) runModelTurn(
	ctx context.Context,
	runID string,
	turn int,
	messages []model.Message,
	tools []model.ToolDefinition,
	emit func(domain.EventType, any) bool,
) (turnResult, error) {
	modelEvents, modelErrors := e.model.Stream(ctx, model.Request{
		RunID:    runID,
		Turn:     turn,
		Messages: append([]model.Message(nil), messages...),
		Tools:    tools,
	})

	var result turnResult
	var text strings.Builder
	for modelEvents != nil || modelErrors != nil {
		select {
		case <-ctx.Done():
			return turnResult{}, ctx.Err()
		case event, ok := <-modelEvents:
			if !ok {
				modelEvents = nil
				continue
			}
			switch event.Type {
			case model.EventTextDelta:
				text.WriteString(event.Delta)
				if !emit(domain.EventModelDelta, map[string]any{"turn": turn, "delta": event.Delta}) {
					return turnResult{}, ctx.Err()
				}
			case model.EventToolCall:
				result.toolCalls = append(result.toolCalls, event.ToolCall)
			case model.EventUsage:
				result.usage.InputTokens += event.Usage.InputTokens
				result.usage.OutputTokens += event.Usage.OutputTokens
				result.usage.CostMicros += event.Usage.CostMicros
			case model.EventCompleted:
				output := event.Output
				result.finalOutput = &output
			}
		case err, ok := <-modelErrors:
			if !ok {
				modelErrors = nil
				continue
			}
			if err != nil {
				return turnResult{}, err
			}
		}
	}
	result.assistantText = text.String()
	return result, nil
}

func exceededBudget(limits Limits, usage model.Usage) error {
	if limits.MaxInputTokens > 0 && usage.InputTokens > limits.MaxInputTokens {
		return ErrInputTokens
	}
	if limits.MaxOutputTokens > 0 && usage.OutputTokens > limits.MaxOutputTokens {
		return ErrOutputTokens
	}
	if limits.MaxCostMicros > 0 && usage.CostMicros > limits.MaxCostMicros {
		return ErrCostBudget
	}
	return nil
}

func toModelToolDefinitions(definitions []tool.Definition) []model.ToolDefinition {
	result := make([]model.ToolDefinition, 0, len(definitions))
	for _, definition := range definitions {
		result = append(result, model.ToolDefinition{
			Name:        definition.Name,
			Description: definition.Description,
			InputSchema: append(json.RawMessage(nil), definition.InputSchema...),
		})
	}
	return result
}

func validate(request domain.RunRequest) error {
	if request.RunID == "" || request.TenantID == "" || request.IdempotencyKey == "" {
		return ErrInvalidRequest
	}
	if request.Manifest.ReleaseID == "" || request.Manifest.Checksum == "" {
		return ErrInvalidRequest
	}
	if request.Input == "" {
		return ErrInvalidRequest
	}
	return nil
}
