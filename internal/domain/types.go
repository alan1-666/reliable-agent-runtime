package domain

import (
	"encoding/json"
	"time"
)

type RunStatus string

const (
	RunStatusQueued          RunStatus = "QUEUED"
	RunStatusRunning         RunStatus = "RUNNING"
	RunStatusWaitingApproval RunStatus = "WAITING_APPROVAL"
	RunStatusRetryWait       RunStatus = "RETRY_WAIT"
	RunStatusSucceeded       RunStatus = "SUCCEEDED"
	RunStatusFailed          RunStatus = "FAILED"
	RunStatusCancelled       RunStatus = "CANCELLED"
	RunStatusInconclusive    RunStatus = "INCONCLUSIVE"
	RunStatusTimedOut        RunStatus = "TIMED_OUT"
)

type EventType string

const (
	EventRunCreated      EventType = "RUN_CREATED"
	EventModelStarted    EventType = "MODEL_STARTED"
	EventModelDelta      EventType = "MODEL_DELTA"
	EventUsageRecorded   EventType = "USAGE_RECORDED"
	EventToolRequested   EventType = "TOOL_REQUESTED"
	EventToolCompleted   EventType = "TOOL_COMPLETED"
	EventToolFailed      EventType = "TOOL_FAILED"
	EventRunSucceeded    EventType = "RUN_SUCCEEDED"
	EventRunFailed       EventType = "RUN_FAILED"
	EventRunCancelled    EventType = "RUN_CANCELLED"
	EventRunInconclusive EventType = "RUN_INCONCLUSIVE"
	EventRunTimedOut     EventType = "RUN_TIMED_OUT"
)

type ExecutionMode string

const (
	ExecutionModeLive   ExecutionMode = "LIVE"
	ExecutionModeShadow ExecutionMode = "SHADOW"
	ExecutionModeEval   ExecutionMode = "EVAL"
	ExecutionModeReplay ExecutionMode = "REPLAY"
)

type ReleaseManifest struct {
	ReleaseID           string   `json:"release_id"`
	Checksum            string   `json:"checksum"`
	AgentVersion        string   `json:"agent_version"`
	PromptVersion       string   `json:"prompt_version"`
	SkillVersions       []string `json:"skill_versions"`
	ToolsetVersion      string   `json:"toolset_version"`
	ModelProfileVersion string   `json:"model_profile_version"`
	PolicyVersion       string   `json:"policy_version"`
}

type RunRequest struct {
	RunID          string
	TenantID       string
	IdempotencyKey string
	Input          string
	Mode           ExecutionMode
	Deadline       time.Time
	Manifest       ReleaseManifest
}

type RunEvent struct {
	RunID      string          `json:"run_id"`
	Sequence   uint64          `json:"seq"`
	Type       EventType       `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

type FinalResult struct {
	Status RunStatus `json:"status"`
	Output string    `json:"output,omitempty"`
	Error  string    `json:"error,omitempty"`
}
