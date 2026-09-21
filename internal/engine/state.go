package engine

import (
	"encoding/json"
	"errors"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/model"
)

const StateVersion = 1

type Phase string

const (
	PhaseModel Phase = "MODEL"
	PhaseTools Phase = "TOOLS"
)

var ErrInvalidState = errors.New("invalid engine checkpoint state")

type ExecutionState struct {
	Version          int              `json:"version"`
	RunID            string           `json:"run_id"`
	ManifestChecksum string           `json:"manifest_checksum"`
	Phase            Phase            `json:"phase"`
	Turn             int              `json:"turn"`
	Messages         []model.Message  `json:"messages"`
	PendingToolCalls []model.ToolCall `json:"pending_tool_calls,omitempty"`
	NextToolIndex    int              `json:"next_tool_index"`
	ToolCallsUsed    int              `json:"tool_calls_used"`
	Usage            model.Usage      `json:"usage"`
}

type Output struct {
	Event      domain.RunEvent
	Checkpoint *ExecutionState
}

func InitialState(request domain.RunRequest) ExecutionState {
	return ExecutionState{
		Version:          StateVersion,
		RunID:            request.RunID,
		ManifestChecksum: request.Manifest.Checksum,
		Phase:            PhaseModel,
		Turn:             1,
		Messages:         []model.Message{{Role: "user", Content: request.Input}},
	}
}

func EncodeState(state ExecutionState) (json.RawMessage, error) {
	return json.Marshal(state)
}

func DecodeState(encoded json.RawMessage) (ExecutionState, error) {
	var state ExecutionState
	if err := json.Unmarshal(encoded, &state); err != nil {
		return ExecutionState{}, err
	}
	return state, nil
}

func validateState(state ExecutionState, request domain.RunRequest) error {
	if state.Version != StateVersion || state.RunID != request.RunID ||
		state.ManifestChecksum != request.Manifest.Checksum || state.Turn < 1 ||
		state.ToolCallsUsed < 0 || state.NextToolIndex < 0 ||
		state.NextToolIndex > len(state.PendingToolCalls) || len(state.Messages) == 0 {
		return ErrInvalidState
	}
	switch state.Phase {
	case PhaseModel:
		if len(state.PendingToolCalls) != 0 || state.NextToolIndex != 0 {
			return ErrInvalidState
		}
	case PhaseTools:
		if len(state.PendingToolCalls) == 0 {
			return ErrInvalidState
		}
	default:
		return ErrInvalidState
	}
	return nil
}

func cloneState(state ExecutionState) ExecutionState {
	state.Messages = make([]model.Message, len(state.Messages))
	for index, message := range state.Messages {
		state.Messages[index] = message
		state.Messages[index].ToolCalls = cloneToolCalls(message.ToolCalls)
	}
	state.PendingToolCalls = cloneToolCalls(state.PendingToolCalls)
	return state
}

func cloneToolCalls(calls []model.ToolCall) []model.ToolCall {
	result := make([]model.ToolCall, len(calls))
	for index, call := range calls {
		result[index] = call
		result[index].Arguments = append(json.RawMessage(nil), call.Arguments...)
	}
	return result
}
