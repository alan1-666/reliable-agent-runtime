package main

import (
	"context"
	"encoding/json"
	"fmt"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/engine"
	"safemarket/agent-runtime/internal/model"
	"safemarket/agent-runtime/internal/model/fake"
	"safemarket/agent-runtime/internal/tool"
)

func main() {
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Function{
		Def: tool.Definition{
			Name:        "service_status",
			Description: "returns the current service status",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"}},"required":["service"]}`),
			ReadOnly:    true,
		},
		Run: func(_ context.Context, arguments json.RawMessage) (tool.Result, error) {
			return tool.Result{Content: `{"service":"order-service","status":"healthy"}`}, nil
		},
	}); err != nil {
		panic(err)
	}

	modelAdapter := fake.Scripted(
		[]model.Event{{
			Type: model.EventToolCall,
			ToolCall: model.ToolCall{
				ID:        "call-1",
				Name:      "service_status",
				Arguments: json.RawMessage(`{"service":"order-service"}`),
			},
		}},
		[]model.Event{
			{Type: model.EventTextDelta, Delta: "order-service is healthy"},
			{Type: model.EventUsage, Usage: model.Usage{InputTokens: 86, OutputTokens: 12, CostMicros: 4}},
			{Type: model.EventCompleted, Output: "order-service is healthy"},
		},
	)

	runtime := engine.New(modelAdapter, engine.WithTools(registry))
	events := runtime.Run(context.Background(), domain.RunRequest{
		RunID:          "demo-run",
		TenantID:       "demo-tenant",
		IdempotencyKey: "demo-request",
		Input:          "check order-service",
		Mode:           domain.ExecutionModeEval,
		Manifest: domain.ReleaseManifest{
			ReleaseID: "demo-release",
			Checksum:  "sha256:demo",
		},
	})

	for event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(encoded))
	}
}
