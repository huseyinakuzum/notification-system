package models

import "testing"

func TestNotificationCanTransition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		from Status
		to   Status
		want bool
	}{
		{"scheduled to queued", StatusScheduled, StatusQueued, true},
		{"scheduled to cancelled", StatusScheduled, StatusCancelled, true},
		{"queued to processing", StatusQueued, StatusProcessing, true},
		{"processing to sent", StatusProcessing, StatusSent, true},
		{"processing to failed", StatusProcessing, StatusFailed, true},
		{"processing to scheduled retry", StatusProcessing, StatusScheduled, true},
		{"scheduled to processing illegal", StatusScheduled, StatusProcessing, false},
		{"scheduled to sent illegal", StatusScheduled, StatusSent, false},
		{"queued to cancelled illegal", StatusQueued, StatusCancelled, false},
		{"queued to sent illegal", StatusQueued, StatusSent, false},
		{"processing to cancelled illegal", StatusProcessing, StatusCancelled, false},
		{"processing to queued illegal", StatusProcessing, StatusQueued, false},
		{"self processing illegal", StatusProcessing, StatusProcessing, false},
		{"self scheduled illegal", StatusScheduled, StatusScheduled, false},
		{"from terminal sent illegal", StatusSent, StatusScheduled, false},
		{"from terminal failed illegal", StatusFailed, StatusProcessing, false},
		{"from terminal cancelled illegal", StatusCancelled, StatusScheduled, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := NotificationCanTransition(tt.from, tt.to); got != tt.want {
				t.Errorf("NotificationCanTransition(%q, %q) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}
