package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/store"
)

func TestRepositoryLifecycle(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	schema := fmt.Sprintf("runtime_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	}()

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	migrationPath := filepath.Join("..", "..", "..", "migrations", "001_runtime.up.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}

	repository := New(pool)
	now := time.Now().UTC().Truncate(time.Microsecond)
	request := domain.RunRequest{
		RunID:          "run-1",
		TenantID:       "tenant-1",
		IdempotencyKey: "request-1",
		Input:          "inspect incident",
		Mode:           domain.ExecutionModeEval,
		Manifest: domain.ReleaseManifest{
			ReleaseID: "release-1",
			Checksum:  "sha256:test",
		},
	}
	record, created, err := repository.CreateRun(ctx, request, now)
	if err != nil || !created || record.LastEventSeq != 1 {
		t.Fatalf("create: record=%+v created=%v err=%v", record, created, err)
	}
	duplicate, created, err := repository.CreateRun(ctx, request, now.Add(time.Second))
	if err != nil || created || duplicate.ID != record.ID {
		t.Fatalf("idempotent create: record=%+v created=%v err=%v", duplicate, created, err)
	}

	firstLease, err := repository.LeaseNext(ctx, "worker-a", now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	secondLease, err := repository.LeaseNext(ctx, "worker-b", now.Add(2*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if secondLease.FenceToken <= firstLease.FenceToken {
		t.Fatalf("fence did not advance: first=%+v second=%+v", firstLease, secondLease)
	}
	if _, err := repository.AppendEvent(
		ctx, request.RunID, "worker-a", firstLease.FenceToken,
		domain.EventModelStarted, nil, now.Add(3*time.Second),
	); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("old worker error = %v", err)
	}
	event, err := repository.AppendEvent(
		ctx, request.RunID, "worker-b", secondLease.FenceToken,
		domain.EventModelStarted, nil, now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := store.Checkpoint{
		RunID:         request.RunID,
		FenceToken:    secondLease.FenceToken,
		EventSequence: event.Sequence,
		State:         []byte(`{"turn":1}`),
		CreatedAt:     now.Add(4 * time.Second),
	}
	if err := repository.SaveCheckpoint(ctx, "worker-b", checkpoint); err != nil {
		t.Fatal(err)
	}
	result := domain.FinalResult{Status: domain.RunStatusSucceeded, Output: "done"}
	if _, err := repository.FinishRun(
		ctx, request.RunID, "worker-b", secondLease.FenceToken,
		result, domain.EventRunSucceeded, now.Add(5*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	final, err := repository.GetRun(ctx, request.RunID)
	if err != nil || final.Status != domain.RunStatusSucceeded || final.FinalResult == nil {
		t.Fatalf("final run = %+v err=%v", final, err)
	}
	events, err := repository.ListEvents(ctx, request.RunID, 0)
	if err != nil || len(events) != 3 {
		t.Fatalf("events = %+v err=%v", events, err)
	}
}
