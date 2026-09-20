package openaicompat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"safemarket/agent-runtime/internal/model"
)

func TestAdapterStreamsTextUsageAndRequestContract(t *testing.T) {
	var captured map[string]any
	client := clientWith(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("unexpected authorization header")
		}
		if err := json.NewDecoder(request.Body).Decode(&captured); err != nil {
			t.Fatal(err)
		}
		return streamResponse(strings.Join([]string{
			`data: {"choices":[{"delta":{"content":"hello"},"finish_reason":""}]}`,
			"",
			`data: {"choices":[{"delta":{"content":" world"},"finish_reason":"stop"}]}`,
			"",
			`data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":2}}`,
			"",
			`data: [DONE]`,
		}, "\n")), nil
	})

	adapter := mustAdapter(t, Config{
		BaseURL:                "https://example.test",
		APIKey:                 "secret",
		Model:                  "deepseek-test",
		InputMicrosPerMillion:  100_000,
		OutputMicrosPerMillion: 500_000,
		HTTPClient:             client,
	})
	events, err := collect(adapter.Stream(context.Background(), sampleRequest()))
	if err != nil {
		t.Fatal(err)
	}

	if got := eventTypes(events); fmt.Sprint(got) != fmt.Sprint([]model.EventType{
		model.EventTextDelta, model.EventTextDelta, model.EventCompleted, model.EventUsage,
	}) {
		t.Fatalf("unexpected event types: %v", got)
	}
	if events[2].Output != "hello world" {
		t.Fatalf("output = %q", events[2].Output)
	}
	if events[3].Usage.InputTokens != 10 || events[3].Usage.OutputTokens != 2 || events[3].Usage.CostMicros != 2 {
		t.Fatalf("unexpected usage: %+v", events[3].Usage)
	}
	if captured["model"] != "deepseek-test" || captured["stream"] != true || captured["tool_choice"] != "auto" {
		t.Fatalf("unexpected request: %+v", captured)
	}
	if len(captured["tools"].([]any)) != 1 {
		t.Fatalf("tools missing from request: %+v", captured)
	}
}

func TestAdapterAssemblesStreamingToolCall(t *testing.T) {
	client := clientWith(func(_ *http.Request) (*http.Response, error) {
		return streamResponse(strings.Join([]string{
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"service_status","arguments":"{\"service\":"}}]},"finish_reason":""}]}`,
			"",
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"order-service\"}"}}]},"finish_reason":"tool_calls"}]}`,
			"",
			`data: [DONE]`,
		}, "\n")), nil
	})

	adapter := mustAdapter(t, Config{
		BaseURL: "https://example.test", APIKey: "secret", Model: "deepseek-test", HTTPClient: client,
	})
	events, err := collect(adapter.Stream(context.Background(), sampleRequest()))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != model.EventToolCall {
		t.Fatalf("unexpected events: %+v", events)
	}
	call := events[0].ToolCall
	if call.ID != "call-1" || call.Name != "service_status" || string(call.Arguments) != `{"service":"order-service"}` {
		t.Fatalf("unexpected tool call: %+v", call)
	}
}

func TestAdapterReturnsTypedHTTPError(t *testing.T) {
	client := clientWith(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       io.NopCloser(strings.NewReader(`{"error":"rate limited"}`)),
			Header:     make(http.Header),
		}, nil
	})
	adapter := mustAdapter(t, Config{
		BaseURL: "https://example.test", APIKey: "secret", Model: "deepseek-test", HTTPClient: client,
	})

	_, err := collect(adapter.Stream(context.Background(), sampleRequest()))
	var httpErr HTTPError
	if !errors.As(err, &httpErr) || !httpErr.Retryable() || httpErr.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAdapterPropagatesCancellation(t *testing.T) {
	started := make(chan struct{})
	client := clientWith(func(request *http.Request) (*http.Response, error) {
		close(started)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})
	adapter := mustAdapter(t, Config{
		BaseURL: "https://example.test", APIKey: "secret", Model: "deepseek-test", HTTPClient: client,
	})
	ctx, cancel := context.WithCancel(context.Background())
	events, errs := adapter.Stream(ctx, sampleRequest())
	<-started
	cancel()
	for range events {
	}
	err := <-errs
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
}

func sampleRequest() model.Request {
	return model.Request{
		RunID: "run-1",
		Turn:  1,
		Messages: []model.Message{
			{Role: "user", Content: "check service"},
		},
		Tools: []model.ToolDefinition{{
			Name:        "service_status",
			Description: "returns service status",
			InputSchema: json.RawMessage(`{"type":"object"}`),
		}},
	}
}

func mustAdapter(t *testing.T, config Config) *Adapter {
	t.Helper()
	adapter, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func collect(events <-chan model.Event, errs <-chan error) ([]model.Event, error) {
	var collected []model.Event
	for events != nil || errs != nil {
		select {
		case event, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			collected = append(collected, event)
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil {
				return collected, err
			}
		}
	}
	return collected, nil
}

func eventTypes(events []model.Event) []model.EventType {
	result := make([]model.EventType, 0, len(events))
	for _, event := range events {
		result = append(result, event.Type)
	}
	return result
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	_, err := New(Config{BaseURL: "://", APIKey: "", Model: ""})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v, want invalid config", err)
	}
}

func TestHTTPClientTimeoutCanBeConfigured(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	adapter := mustAdapter(t, Config{BaseURL: "https://example.com", APIKey: "secret", Model: "model", HTTPClient: client})
	if adapter.client != client {
		t.Fatal("custom HTTP client was not retained")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func clientWith(roundTrip roundTripFunc) *http.Client {
	return &http.Client{Transport: roundTrip}
}

func streamResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
