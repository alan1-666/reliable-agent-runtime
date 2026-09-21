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

	"safemarket/agent-runtime/internal/outbox"
)

func TestLeaseExpiryFencesOldPublisher(t *testing.T) {
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
	schema := fmt.Sprintf("outbox_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`) }()

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
	for _, name := range []string{"001_runtime.up.sql", "002_outbox_leasing.up.sql"} {
		migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (
			aggregate_type, aggregate_id, event_type, payload,
			next_attempt_at, created_at
		) VALUES ('RUN', 'run-1', 'RUN_CREATED', '{}', $1, $1)`, now); err != nil {
		t.Fatal(err)
	}
	repository := New(pool)
	first, err := repository.LeaseBatch(ctx, "publisher-a", now, time.Second, 10)
	if err != nil || len(first) != 1 || first[0].LeaseToken != 1 || first[0].Attempts != 1 {
		t.Fatalf("first lease=%+v err=%v", first, err)
	}
	if _, err := repository.LeaseBatch(ctx, "publisher-b", now.Add(500*time.Millisecond), time.Second, 10); !errors.Is(err, outbox.ErrNoMessages) {
		t.Fatalf("concurrent lease error=%v", err)
	}
	second, err := repository.LeaseBatch(ctx, "publisher-b", now.Add(2*time.Second), time.Minute, 10)
	if err != nil || len(second) != 1 || second[0].LeaseToken != 2 || second[0].Attempts != 2 {
		t.Fatalf("second lease=%+v err=%v", second, err)
	}
	if err := repository.MarkPublished(ctx, first[0].ID, "publisher-a", first[0].LeaseToken, now.Add(2500*time.Millisecond)); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("old publisher error=%v", err)
	}
	if err := repository.MarkPublished(ctx, second[0].ID, "publisher-b", second[0].LeaseToken, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.LeaseBatch(ctx, "publisher-c", now.Add(4*time.Second), time.Minute, 10); !errors.Is(err, outbox.ErrNoMessages) {
		t.Fatalf("published message was leased again: %v", err)
	}
}
