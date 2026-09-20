package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"safemarket/agent-runtime/internal/domain"
)

func RequestHash(request domain.RunRequest) (string, error) {
	encoded, err := json.Marshal(struct {
		TenantID       string
		IdempotencyKey string
		Input          string
		Mode           domain.ExecutionMode
		Deadline       string
		Manifest       domain.ReleaseManifest
	}{
		TenantID:       request.TenantID,
		IdempotencyKey: request.IdempotencyKey,
		Input:          request.Input,
		Mode:           request.Mode,
		Deadline:       request.Deadline.UTC().Format(timeFormat),
		Manifest:       request.Manifest,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

const timeFormat = "2006-01-02T15:04:05.999999999Z07:00"
