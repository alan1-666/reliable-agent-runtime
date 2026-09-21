package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/store"
)

func TestCreateRunIsIdempotentAndRejectsChangedRequest(t *testing.T) {
	repository := store.NewMemoryRepository()
	handler := newTestHandler(t, repository)
	body := `{"input":"hello","mode":"EVAL","manifest":{"release_id":"release-1","checksum":"sha256:test","agent_version":"","prompt_version":"","skill_versions":null,"toolset_version":"","model_profile_version":"","policy_version":""}}`

	first := createRequest(handler, body, "key-1")
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d body=%s", first.Code, first.Body.String())
	}
	second := createRequest(handler, body, "key-1")
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d body=%s", second.Code, second.Body.String())
	}
	var firstRun, secondRun runResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstRun); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondRun); err != nil {
		t.Fatal(err)
	}
	if firstRun.ID != secondRun.ID || firstRun.Status != domain.RunStatusQueued {
		t.Fatalf("first=%+v second=%+v", firstRun, secondRun)
	}

	changed := strings.Replace(body, `"hello"`, `"different"`, 1)
	conflict := createRequest(handler, changed, "key-1")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body=%s", conflict.Code, conflict.Body.String())
	}
}

func TestGetRunDoesNotCrossTenantBoundary(t *testing.T) {
	repository := store.NewMemoryRepository()
	handler := newTestHandler(t, repository)
	createRequest(handler, validCreateBody(), "key-1")

	request := httptest.NewRequest(http.MethodGet, "/v1/runs/run-fixed", nil)
	request.Header.Set("X-Tenant-ID", "another-tenant")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
}

func TestSSEStartsAfterCursorAndStopsAtTerminal(t *testing.T) {
	repository := store.NewMemoryRepository()
	handler := newTestHandler(t, repository)
	now := time.Now().UTC()
	request := domain.RunRequest{
		RunID:          "run-1",
		TenantID:       "tenant-1",
		IdempotencyKey: "key-1",
		Input:          "hello",
		Mode:           domain.ExecutionModeEval,
		Manifest: domain.ReleaseManifest{
			ReleaseID: "release-1",
			Checksum:  "sha256:test",
		},
	}
	if _, _, err := repository.CreateRun(context.Background(), request, now); err != nil {
		t.Fatal(err)
	}
	lease, err := repository.LeaseNext(context.Background(), "worker-a", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.AppendEvent(
		context.Background(), request.RunID, "worker-a", lease.FenceToken,
		domain.EventModelStarted, json.RawMessage(`{"turn":1}`), now.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.FinishRun(
		context.Background(), request.RunID, "worker-a", lease.FenceToken,
		domain.FinalResult{Status: domain.RunStatusSucceeded, Output: "done"},
		domain.EventRunSucceeded, now.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}

	httpRequest := httptest.NewRequest(http.MethodGet, "/v1/runs/run-1/events?after_seq=1", nil)
	httpRequest.Header.Set("X-Tenant-ID", "tenant-1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httpRequest)

	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	body := response.Body.String()
	if strings.Contains(body, "RUN_CREATED") || !strings.Contains(body, "id: 2\n") || !strings.Contains(body, "id: 3\n") ||
		!strings.Contains(body, "event: MODEL_STARTED") || !strings.Contains(body, "event: RUN_SUCCEEDED") {
		t.Fatalf("unexpected SSE body:\n%s", body)
	}
}

func TestCreateRejectsUnknownJSONField(t *testing.T) {
	handler := newTestHandler(t, store.NewMemoryRepository())
	body := strings.TrimSuffix(validCreateBody(), "}") + `,"unexpected":true}`
	response := createRequest(handler, body, "key-1")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
}

func newTestHandler(t *testing.T, repository store.Repository) *Handler {
	t.Helper()
	config := DefaultConfig()
	config.PollInterval = time.Millisecond
	config.HeartbeatInterval = time.Second
	config.GenerateID = func() (string, error) { return "run-fixed", nil }
	handler, err := New(repository, config)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func createRequest(handler http.Handler, body, key string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/runs", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Tenant-ID", "tenant-1")
	request.Header.Set("Idempotency-Key", key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func validCreateBody() string {
	return `{"input":"hello","mode":"EVAL","manifest":{"release_id":"release-1","checksum":"sha256:test"}}`
}
