package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"safemarket/agent-runtime/internal/engine"
	"safemarket/agent-runtime/internal/model/openaicompat"
	"safemarket/agent-runtime/internal/store/postgres"
	"safemarket/agent-runtime/internal/tool"
	runtimeworker "safemarket/agent-runtime/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	if apiKey == "" {
		fatal("DEEPSEEK_API_KEY is required")
	}
	databaseURL := envOrDefault("DATABASE_URL", "postgres://agent_runtime:agent_runtime_dev@127.0.0.1:54329/agent_runtime?sslmode=disable")
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
		MaxTokens: 2048,
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
		Run: func(_ context.Context, _ json.RawMessage) (tool.Result, error) {
			return tool.Result{Content: `{"service":"order-service","status":"healthy","source":"runtime-worker"}`}, nil
		},
	}); err != nil {
		fatal("register tool: %v", err)
	}

	runtime := engine.New(adapter, engine.WithTools(registry), engine.WithLimits(engine.Limits{
		MaxTurns:        8,
		MaxToolCalls:    16,
		MaxInputTokens:  50_000,
		MaxOutputTokens: 8_000,
	}))
	workerID := envOrDefault("WORKER_ID", defaultWorkerID())
	config := runtimeworker.DefaultConfig(workerID)
	runWorker, err := runtimeworker.New(postgres.New(pool), runtime, config)
	if err != nil {
		fatal("create worker: %v", err)
	}
	fmt.Printf("agent runtime worker %s started\n", workerID)
	if err := runWorker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fatal("run worker: %v", err)
	}
}

func defaultWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "worker"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
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
