package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

type fakeRepository struct {
	messages  []Message
	leaseErr  error
	published []int64
	retries   []retryRecord
}

type retryRecord struct {
	messageID   int64
	leaseToken  uint64
	nextAttempt time.Time
	error       string
}

func (repository *fakeRepository) LeaseBatch(context.Context, string, time.Time, time.Duration, int) ([]Message, error) {
	return append([]Message(nil), repository.messages...), repository.leaseErr
}

func (repository *fakeRepository) MarkPublished(_ context.Context, messageID int64, _ string, _ uint64, _ time.Time) error {
	repository.published = append(repository.published, messageID)
	return nil
}

func (repository *fakeRepository) MarkRetry(
	_ context.Context,
	messageID int64,
	_ string,
	leaseToken uint64,
	nextAttempt time.Time,
	errorMessage string,
) error {
	repository.retries = append(repository.retries, retryRecord{
		messageID: messageID, leaseToken: leaseToken, nextAttempt: nextAttempt, error: errorMessage,
	})
	return nil
}

type sinkFunc func(context.Context, Message) error

func (function sinkFunc) Publish(ctx context.Context, message Message) error {
	return function(ctx, message)
}

func TestRunOncePublishesWholeBatch(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	repository := &fakeRepository{messages: []Message{
		{ID: 1, LeaseToken: 10, Attempts: 1},
		{ID: 2, LeaseToken: 11, Attempts: 1},
	}}
	var published []int64
	publisher := mustPublisher(t, repository, sinkFunc(func(_ context.Context, message Message) error {
		published = append(published, message.ID)
		return nil
	}), now)

	count, err := publisher.RunOnce(context.Background())
	if err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if len(published) != 2 || len(repository.published) != 2 || len(repository.retries) != 0 {
		t.Fatalf("sink=%v marked=%v retries=%v", published, repository.published, repository.retries)
	}
}

func TestRunOnceReschedulesFailureWithCappedExponentialBackoff(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	repository := &fakeRepository{messages: []Message{{ID: 7, LeaseToken: 3, Attempts: 4}}}
	publisher := mustPublisher(t, repository, sinkFunc(func(context.Context, Message) error {
		return errors.New("broker unavailable")
	}), now)

	count, err := publisher.RunOnce(context.Background())
	if count != 1 || err == nil || len(repository.retries) != 1 {
		t.Fatalf("count=%d err=%v retries=%+v", count, err, repository.retries)
	}
	retry := repository.retries[0]
	if retry.messageID != 7 || retry.leaseToken != 3 || !retry.nextAttempt.Equal(now.Add(8*time.Second)) {
		t.Fatalf("unexpected retry: %+v", retry)
	}
}

func TestPublishTimeoutIsRescheduled(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	repository := &fakeRepository{messages: []Message{{ID: 1, LeaseToken: 1, Attempts: 1}}}
	publisher := mustPublisher(t, repository, sinkFunc(func(ctx context.Context, _ Message) error {
		<-ctx.Done()
		return ctx.Err()
	}), now)
	publisher.config.PublishTimeout = time.Millisecond

	if _, err := publisher.RunOnce(context.Background()); err == nil || len(repository.retries) != 1 {
		t.Fatalf("err=%v retries=%+v", err, repository.retries)
	}
}

func TestRunOnceTreatsEmptyQueueAsSuccess(t *testing.T) {
	repository := &fakeRepository{leaseErr: ErrNoMessages}
	publisher := mustPublisher(t, repository, sinkFunc(func(context.Context, Message) error {
		t.Fatal("sink should not be called")
		return nil
	}), time.Unix(100, 0).UTC())
	count, err := publisher.RunOnce(context.Background())
	if count != 0 || err != nil {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestRunStopsOnCancellation(t *testing.T) {
	repository := &fakeRepository{leaseErr: ErrNoMessages}
	publisher := mustPublisher(t, repository, sinkFunc(func(context.Context, Message) error { return nil }), time.Now())
	publisher.config.PollInterval = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	wait.Add(1)
	var runErr error
	go func() {
		defer wait.Done()
		runErr = publisher.Run(ctx)
	}()
	cancel()
	wait.Wait()
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("error=%v", runErr)
	}
}

func mustPublisher(t *testing.T, repository Repository, sink Sink, now time.Time) *Publisher {
	t.Helper()
	config := DefaultConfig("publisher-a")
	config.BaseBackoff = time.Second
	config.MaxBackoff = 8 * time.Second
	publisher, err := NewPublisher(repository, sink, config, WithClock(fixedClock{now: now}))
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}
