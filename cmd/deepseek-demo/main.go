package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/engine"
	"safemarket/agent-runtime/internal/model/openaicompat"
	"safemarket/agent-runtime/internal/tool"
)

func main() {
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "DEEPSEEK_API_KEY is required")
		os.Exit(2)
	}
	baseURL := envOrDefault("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
	modelName := envOrDefault("DEEPSEEK_MODEL", "deepseek-chat")

	extraBody := map[string]any{}
	if thinking := os.Getenv("DEEPSEEK_THINKING"); thinking != "" {
		extraBody["thinking"] = map[string]string{"type": thinking}
	}

	adapter, err := openaicompat.New(openaicompat.Config{
		BaseURL:   baseURL,
		APIKey:    apiKey,
		Model:     modelName,
		MaxTokens: 1024,
		ExtraBody: extraBody,
	})
	if err != nil {
		panic(err)
	}

	registry := tool.NewRegistry()
	if err := registry.Register(tool.Function{
		Def: tool.Definition{
			Name:        "service_status",
			Description: "Get the health status of an internal service.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"}},"required":["service"],"additionalProperties":false}`),
			ReadOnly:    true,
		},
		Run: func(_ context.Context, arguments json.RawMessage) (tool.Result, error) {
			return tool.Result{Content: `{"service":"order-service","status":"healthy","source":"demo"}`}, nil
		},
	}); err != nil {
		panic(err)
	}

	input := "Use service_status to check order-service, then answer with the observed status."
	if len(os.Args) > 1 {
		input = strings.Join(os.Args[1:], " ")
	}

	runtime := engine.New(adapter,
		engine.WithTools(registry),
		engine.WithLimits(engine.Limits{
			MaxTurns:        4,
			MaxToolCalls:    4,
			MaxInputTokens:  20_000,
			MaxOutputTokens: 4_000,
		}),
	)
	events := runtime.Run(context.Background(), domain.RunRequest{
		RunID:          fmt.Sprintf("deepseek-%d", time.Now().UnixNano()),
		TenantID:       "local-demo",
		IdempotencyKey: fmt.Sprintf("local-%d", time.Now().UnixNano()),
		Input:          input,
		Mode:           domain.ExecutionModeEval,
		Deadline:       time.Now().Add(90 * time.Second),
		Manifest: domain.ReleaseManifest{
			ReleaseID:           "local-deepseek-demo",
			Checksum:            "sha256:local-demo",
			ModelProfileVersion: modelName,
			ToolsetVersion:      "demo-v1",
		},
	})

	for event := range events {
		encoded, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			panic(marshalErr)
		}
		fmt.Println(string(encoded))
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
