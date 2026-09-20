package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/engine"
	"safemarket/agent-runtime/internal/model/openaicompat"
	"safemarket/agent-runtime/internal/store/postgres"
	"safemarket/agent-runtime/internal/tool"
	"safemarket/agent-runtime/internal/worker"
)

func main() {
	ctx := context.Background()
	databaseURL := envOrDefault("DATABASE_URL", "postgres://agent_runtime:agent_runtime_dev@127.0.0.1:54329/agent_runtime?sslmode=disable")
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	if apiKey == "" {
		fatal("DEEPSEEK_API_KEY is required")
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		fatal("connect postgres: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		fatal("ping postgres: %v", err)
	}

	adapter, err := openaicompat.New(openaicompat.Config{
		BaseURL:   envOrDefault("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
		APIKey:    apiKey,
		Model:     envOrDefault("DEEPSEEK_MODEL", "deepseek-chat"),
		MaxTokens: 1024,
	})
	if err != nil {
		fatal("create model adapter: %v", err)
	}
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Function{
		Def: tool.Definition{
			Name:        "service_status",
			Description: "Get the current health of an internal service.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"}},"required":["service"],"additionalProperties":false}`),
			ReadOnly:    true,
		},
		Run: func(_ context.Context, arguments json.RawMessage) (tool.Result, error) {
			return tool.Result{Content: `{"service":"order-service","status":"healthy","source":"persistent-demo"}`}, nil
		},
	}); err != nil {
		fatal("register tool: %v", err)
	}

	repository := postgres.New(pool)
	runtime := engine.New(adapter, engine.WithTools(registry), engine.WithLimits(engine.Limits{
		MaxTurns:        4,
		MaxToolCalls:    4,
		MaxInputTokens:  20_000,
		MaxOutputTokens: 4_000,
	}))
	workerConfig := worker.DefaultConfig("persistent-demo-worker")
	runWorker, err := worker.New(repository, runtime, workerConfig)
	if err != nil {
		fatal("create worker: %v", err)
	}

	now := time.Now().UTC()
	runID := fmt.Sprintf("demo-%d", now.UnixNano())
	request := domain.RunRequest{
		RunID:          runID,
		TenantID:       "local-demo",
		IdempotencyKey: runID,
		Input:          "Use service_status to inspect order-service and summarize only the observed result.",
		Mode:           domain.ExecutionModeEval,
		Deadline:       now.Add(90 * time.Second),
		Manifest: domain.ReleaseManifest{
			ReleaseID:           "persistent-demo-v1",
			Checksum:            "sha256:persistent-demo-v1",
			ModelProfileVersion: envOrDefault("DEEPSEEK_MODEL", "deepseek-chat"),
			ToolsetVersion:      "demo-v1",
		},
	}
	if _, created, err := repository.CreateRun(ctx, request, now); err != nil {
		fatal("create run: %v", err)
	} else if !created {
		fatal("run already exists: %s", runID)
	}
	if executed, err := runWorker.RunOnce(ctx); err != nil {
		fatal("execute run: %v", err)
	} else if !executed {
		fatal("worker found no queued run")
	}

	events, err := repository.ListEvents(ctx, runID, 0)
	if err != nil {
		fatal("list events: %v", err)
	}
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			fatal("encode event: %v", err)
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

func fatal(format string, arguments ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", arguments...)
	os.Exit(1)
}
