package model

import (
	"context"
	"encoding/json"
)

type Capability string

const (
	CapabilityStreaming        Capability = "STREAMING"
	CapabilityToolCalling      Capability = "TOOL_CALLING"
	CapabilityStructuredOutput Capability = "STRUCTURED_OUTPUT"
)

type Message struct {
	Role       string
	Content    string
	ToolCallID string
	ToolName   string
	ToolCalls  []ToolCall
}

type ToolCall struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}

type ToolDefinition struct {
	Name        string
	Description string
	InputSchema json.RawMessage
}

type Request struct {
	RunID    string
	Turn     int
	Messages []Message
	Tools    []ToolDefinition
}

type Usage struct {
	InputTokens  int   `json:"input_tokens"`
	OutputTokens int   `json:"output_tokens"`
	CostMicros   int64 `json:"cost_micros"`
}

type EventType string

const (
	EventTextDelta EventType = "TEXT_DELTA"
	EventToolCall  EventType = "TOOL_CALL"
	EventUsage     EventType = "USAGE"
	EventCompleted EventType = "COMPLETED"
)

type Event struct {
	Type     EventType
	Delta    string
	Output   string
	ToolCall ToolCall
	Usage    Usage
}

type Adapter interface {
	Name() string
	Capabilities(context.Context) map[Capability]bool
	Stream(context.Context, Request) (<-chan Event, <-chan error)
}
