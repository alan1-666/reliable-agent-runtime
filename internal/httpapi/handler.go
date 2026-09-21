package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"safemarket/agent-runtime/internal/domain"
	"safemarket/agent-runtime/internal/store"
)

type IDGenerator func() (string, error)

type Config struct {
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	GenerateID        IDGenerator
}

func DefaultConfig() Config {
	return Config{
		PollInterval:      250 * time.Millisecond,
		HeartbeatInterval: 15 * time.Second,
		GenerateID:        randomID,
	}
}

type Handler struct {
	repository store.Repository
	config     Config
	router     *http.ServeMux
}

func New(repository store.Repository, config Config) (*Handler, error) {
	if repository == nil || config.PollInterval <= 0 || config.HeartbeatInterval <= 0 || config.GenerateID == nil {
		return nil, errors.New("invalid HTTP API configuration")
	}
	handler := &Handler{repository: repository, config: config, router: http.NewServeMux()}
	handler.router.HandleFunc("POST /v1/runs", handler.createRun)
	handler.router.HandleFunc("GET /v1/runs/{run_id}", handler.getRun)
	handler.router.HandleFunc("GET /v1/runs/{run_id}/events", handler.streamEvents)
	handler.router.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
	})
	return handler, nil
}

func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	h.router.ServeHTTP(response, request)
}

type createRunRequest struct {
	RunID    string                 `json:"run_id,omitempty"`
	Input    string                 `json:"input"`
	Mode     domain.ExecutionMode   `json:"mode"`
	Deadline *time.Time             `json:"deadline,omitempty"`
	Manifest domain.ReleaseManifest `json:"manifest"`
}

type runResponse struct {
	ID             string                 `json:"id"`
	TenantID       string                 `json:"tenant_id"`
	IdempotencyKey string                 `json:"idempotency_key"`
	Status         domain.RunStatus       `json:"status"`
	Mode           domain.ExecutionMode   `json:"mode"`
	Manifest       domain.ReleaseManifest `json:"manifest"`
	Deadline       *time.Time             `json:"deadline,omitempty"`
	Attempt        int                    `json:"attempt"`
	LastEventSeq   uint64                 `json:"last_event_seq"`
	FinalResult    *domain.FinalResult    `json:"final_result,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
}

func (h *Handler) createRun(response http.ResponseWriter, request *http.Request) {
	tenantID := strings.TrimSpace(request.Header.Get("X-Tenant-ID"))
	idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if tenantID == "" || idempotencyKey == "" {
		writeError(response, http.StatusBadRequest, "X-Tenant-ID and Idempotency-Key headers are required")
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 1<<20)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var body createRunRequest
	if err := decoder.Decode(&body); err != nil {
		writeError(response, http.StatusBadRequest, "invalid JSON request")
		return
	}
	if err := ensureJSONEnd(decoder); err != nil {
		writeError(response, http.StatusBadRequest, "request body must contain one JSON object")
		return
	}
	if err := validateCreate(body); err != nil {
		writeError(response, http.StatusBadRequest, err.Error())
		return
	}
	if body.RunID == "" {
		generated, err := h.config.GenerateID()
		if err != nil {
			writeError(response, http.StatusInternalServerError, "generate run id")
			return
		}
		body.RunID = generated
	}
	deadline := time.Time{}
	if body.Deadline != nil {
		deadline = body.Deadline.UTC()
	}
	record, created, err := h.repository.CreateRun(request.Context(), domain.RunRequest{
		RunID:          body.RunID,
		TenantID:       tenantID,
		IdempotencyKey: idempotencyKey,
		Input:          body.Input,
		Mode:           body.Mode,
		Deadline:       deadline,
		Manifest:       body.Manifest,
	}, time.Now().UTC())
	if errors.Is(err, store.ErrIdempotencyConflict) {
		writeError(response, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeError(response, http.StatusInternalServerError, "create run")
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(response, status, toRunResponse(record))
}

func (h *Handler) getRun(response http.ResponseWriter, request *http.Request) {
	tenantID, ok := requireTenant(response, request)
	if !ok {
		return
	}
	record, err := h.repository.GetRun(request.Context(), request.PathValue("run_id"))
	if errors.Is(err, store.ErrNotFound) || (err == nil && record.TenantID != tenantID) {
		writeError(response, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		writeError(response, http.StatusInternalServerError, "get run")
		return
	}
	writeJSON(response, http.StatusOK, toRunResponse(record))
}

func (h *Handler) streamEvents(response http.ResponseWriter, request *http.Request) {
	tenantID, ok := requireTenant(response, request)
	if !ok {
		return
	}
	flusher, ok := response.(http.Flusher)
	if !ok {
		writeError(response, http.StatusInternalServerError, "streaming is not supported")
		return
	}
	afterSequence, err := eventCursor(request)
	if err != nil {
		writeError(response, http.StatusBadRequest, err.Error())
		return
	}
	runID := request.PathValue("run_id")
	record, err := h.repository.GetRun(request.Context(), runID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && record.TenantID != tenantID) {
		writeError(response, http.StatusNotFound, "run not found")
		return
	} else if err != nil {
		writeError(response, http.StatusInternalServerError, "get run")
		return
	}

	response.Header().Set("Content-Type", "text/event-stream")
	response.Header().Set("Cache-Control", "no-cache")
	response.Header().Set("Connection", "keep-alive")
	response.WriteHeader(http.StatusOK)
	flusher.Flush()

	poll := time.NewTicker(h.config.PollInterval)
	heartbeat := time.NewTicker(h.config.HeartbeatInterval)
	defer poll.Stop()
	defer heartbeat.Stop()
	for {
		terminal, nextSequence, err := h.writeAvailableEvents(request.Context(), response, runID, afterSequence)
		if err != nil {
			return
		}
		if nextSequence > afterSequence {
			afterSequence = nextSequence
			flusher.Flush()
		}
		if terminal {
			return
		}
		select {
		case <-request.Context().Done():
			return
		case <-poll.C:
		case <-heartbeat.C:
			if _, err := fmt.Fprint(response, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (h *Handler) writeAvailableEvents(
	ctx context.Context,
	response http.ResponseWriter,
	runID string,
	afterSequence uint64,
) (bool, uint64, error) {
	events, err := h.repository.ListEvents(ctx, runID, afterSequence)
	if err != nil {
		return false, afterSequence, err
	}
	latest := afterSequence
	terminal := false
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return false, latest, err
		}
		if _, err := fmt.Fprintf(response, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Type, encoded); err != nil {
			return false, latest, err
		}
		latest = event.Sequence
		if _, ok := terminalStatus(event.Type); ok {
			terminal = true
		}
	}
	if len(events) == 0 {
		record, err := h.repository.GetRun(ctx, runID)
		if err != nil {
			return false, latest, err
		}
		terminal = store.IsTerminal(record.Status)
	}
	return terminal, latest, nil
}

func validateCreate(request createRunRequest) error {
	if strings.TrimSpace(request.Input) == "" {
		return errors.New("input is required")
	}
	switch request.Mode {
	case domain.ExecutionModeLive, domain.ExecutionModeShadow, domain.ExecutionModeEval, domain.ExecutionModeReplay:
	default:
		return errors.New("invalid execution mode")
	}
	if request.Manifest.ReleaseID == "" || request.Manifest.Checksum == "" {
		return errors.New("manifest release_id and checksum are required")
	}
	return nil
}

func eventCursor(request *http.Request) (uint64, error) {
	value := request.URL.Query().Get("after_seq")
	if value == "" {
		value = request.Header.Get("Last-Event-ID")
	}
	if value == "" {
		return 0, nil
	}
	cursor, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errors.New("after_seq and Last-Event-ID must be unsigned integers")
	}
	return cursor, nil
}

func terminalStatus(eventType domain.EventType) (domain.RunStatus, bool) {
	switch eventType {
	case domain.EventRunSucceeded:
		return domain.RunStatusSucceeded, true
	case domain.EventRunFailed:
		return domain.RunStatusFailed, true
	case domain.EventRunCancelled:
		return domain.RunStatusCancelled, true
	case domain.EventRunInconclusive:
		return domain.RunStatusInconclusive, true
	case domain.EventRunTimedOut:
		return domain.RunStatusTimedOut, true
	default:
		return "", false
	}
}

func toRunResponse(record store.RunRecord) runResponse {
	response := runResponse{
		ID:             record.ID,
		TenantID:       record.TenantID,
		IdempotencyKey: record.IdempotencyKey,
		Status:         record.Status,
		Mode:           record.Mode,
		Manifest:       record.Manifest,
		Attempt:        record.Attempt,
		LastEventSeq:   record.LastEventSeq,
		FinalResult:    record.FinalResult,
		CreatedAt:      record.CreatedAt,
		UpdatedAt:      record.UpdatedAt,
	}
	if !record.Deadline.IsZero() {
		deadline := record.Deadline
		response.Deadline = &deadline
	}
	return response
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("extra JSON value")
}

func requireTenant(response http.ResponseWriter, request *http.Request) (string, bool) {
	tenantID := strings.TrimSpace(request.Header.Get("X-Tenant-ID"))
	if tenantID == "" {
		writeError(response, http.StatusBadRequest, "X-Tenant-ID header is required")
		return "", false
	}
	return tenantID, true
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func writeError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": message})
}

func randomID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "run_" + hex.EncodeToString(value[:]), nil
}
