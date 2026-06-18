package api

import (
	"fmt"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

// Batch and per-channel content size limits enforced on ingest.
const (
	MaxBatchSize    = 1000
	MaxContentSMS   = 160
	MaxContentPush  = 256
	MaxContentEmail = 10000
)

// contentLimit returns the per-channel content length cap.
func contentLimit(it createItem) int {
	switch it.Channel {
	case models.ChannelSMS:
		return MaxContentSMS
	case models.ChannelPush:
		return MaxContentPush
	default:
		return MaxContentEmail
	}
}

// validateItem checks a create request post-render; empty priority is allowed (treated as normal downstream).
func validateItem(it createItem) error {
	if !it.Channel.Valid() {
		return fmt.Errorf("invalid channel: %q", it.Channel)
	}
	if it.Priority != "" && !it.Priority.Valid() {
		return fmt.Errorf("invalid priority: %q", it.Priority)
	}
	if it.Content == "" {
		return fmt.Errorf("content is required")
	}
	if limit := contentLimit(it); len(it.Content) > limit {
		return fmt.Errorf("content exceeds %d-byte limit for channel %q", limit, it.Channel)
	}
	return nil
}
