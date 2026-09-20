package fake

import (
	"context"
	"errors"
	"sync"

	"safemarket/agent-runtime/internal/model"
)

type Adapter struct {
	Events []model.Event
	Turns  [][]model.Event
	Err    error
	Block  bool

	mu       sync.Mutex
	call     int
	requests []model.Request
}

func (a *Adapter) Name() string {
	return "fake"
}

func (a *Adapter) Capabilities(context.Context) map[model.Capability]bool {
	return map[model.Capability]bool{
		model.CapabilityStreaming:   true,
		model.CapabilityToolCalling: true,
	}
}

func (a *Adapter) Stream(ctx context.Context, request model.Request) (<-chan model.Event, <-chan error) {
	events := make(chan model.Event)
	errs := make(chan error, 1)

	a.mu.Lock()
	a.requests = append(a.requests, request)
	call := a.call
	a.call++
	turnEvents := a.Events
	if len(a.Turns) > 0 {
		if call < len(a.Turns) {
			turnEvents = a.Turns[call]
		} else {
			turnEvents = nil
		}
	}
	a.mu.Unlock()

	go func() {
		defer close(events)
		defer close(errs)

		if a.Block {
			<-ctx.Done()
			errs <- ctx.Err()
			return
		}

		for _, event := range turnEvents {
			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			case events <- event:
			}
		}

		if a.Err != nil {
			errs <- a.Err
		}
	}()

	return events, errs
}

func (a *Adapter) Requests() []model.Request {
	a.mu.Lock()
	defer a.mu.Unlock()
	requests := make([]model.Request, len(a.requests))
	copy(requests, a.requests)
	return requests
}

func Successful(output string) *Adapter {
	return &Adapter{Events: []model.Event{
		{Type: model.EventTextDelta, Delta: output},
		{Type: model.EventCompleted, Output: output},
	}}
}

func Scripted(turns ...[]model.Event) *Adapter {
	return &Adapter{Turns: turns}
}

func Failed(message string) *Adapter {
	return &Adapter{Err: errors.New(message)}
}
