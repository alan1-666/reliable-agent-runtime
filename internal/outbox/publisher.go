package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type Config struct {
	PublisherID    string
	BatchSize      int
	LeaseTTL       time.Duration
	PollInterval   time.Duration
	PublishTimeout time.Duration
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
}

func DefaultConfig(publisherID string) Config {
	return Config{
		PublisherID:    publisherID,
		BatchSize:      100,
		LeaseTTL:       30 * time.Second,
		PollInterval:   500 * time.Millisecond,
		PublishTimeout: 10 * time.Second,
		BaseBackoff:    time.Second,
		MaxBackoff:     time.Minute,
	}
}

type Publisher struct {
	repository Repository
	sink       Sink
	clock      Clock
	config     Config
}

type Option func(*Publisher)

func WithClock(clock Clock) Option {
	return func(publisher *Publisher) { publisher.clock = clock }
}

func NewPublisher(repository Repository, sink Sink, config Config, options ...Option) (*Publisher, error) {
	publisher := &Publisher{
		repository: repository,
		sink:       sink,
		clock:      realClock{},
		config:     config,
	}
	for _, option := range options {
		option(publisher)
	}
	if repository == nil || sink == nil || publisher.clock == nil || config.PublisherID == "" ||
		config.BatchSize <= 0 || config.LeaseTTL <= 0 || config.PollInterval <= 0 ||
		config.PublishTimeout <= 0 || config.PublishTimeout >= config.LeaseTTL ||
		config.BaseBackoff <= 0 || config.MaxBackoff < config.BaseBackoff {
		return nil, ErrInvalidConfig
	}
	return publisher, nil
}

// RunOnce publishes one leased batch. Individual failures are rescheduled and
// returned as a joined error after the rest of the batch has been attempted.
func (p *Publisher) RunOnce(ctx context.Context) (int, error) {
	messages, err := p.repository.LeaseBatch(
		ctx,
		p.config.PublisherID,
		p.clock.Now(),
		p.config.LeaseTTL,
		p.config.BatchSize,
	)
	if errors.Is(err, ErrNoMessages) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var failures []error
	for _, message := range messages {
		publishCtx, cancel := context.WithTimeout(ctx, p.config.PublishTimeout)
		err := p.sink.Publish(publishCtx, message)
		cancel()
		if err != nil {
			nextAttempt := p.clock.Now().Add(p.backoff(message.Attempts))
			if retryErr := p.repository.MarkRetry(ctx, message.ID, p.config.PublisherID, message.LeaseToken, nextAttempt, err.Error()); retryErr != nil {
				failures = append(failures, fmt.Errorf("message %d publish failed: %v; reschedule failed: %w", message.ID, err, retryErr))
			} else {
				failures = append(failures, fmt.Errorf("message %d publish failed: %w", message.ID, err))
			}
			continue
		}
		if err := p.repository.MarkPublished(ctx, message.ID, p.config.PublisherID, message.LeaseToken, p.clock.Now()); err != nil {
			failures = append(failures, fmt.Errorf("message %d mark published: %w", message.ID, err))
		}
	}
	return len(messages), errors.Join(failures...)
}

func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()
	for {
		count, err := p.RunOnce(ctx)
		if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return err
		}
		if count > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Publisher) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := p.config.BaseBackoff
	for index := 1; index < attempt && delay < p.config.MaxBackoff; index++ {
		if delay > p.config.MaxBackoff/2 {
			return p.config.MaxBackoff
		}
		delay *= 2
	}
	if delay > p.config.MaxBackoff {
		return p.config.MaxBackoff
	}
	return delay
}
