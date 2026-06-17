package api

import (
	"strings"
	"testing"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

func TestValidateItem(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		item    createItem
		wantErr string
	}{
		{
			name: "valid sms",
			item: createItem{Channel: models.ChannelSMS, Content: "hello"},
		},
		{
			name: "valid email",
			item: createItem{Channel: models.ChannelEmail, Content: "hello", Priority: models.PriorityHigh},
		},
		{
			name: "valid push",
			item: createItem{Channel: models.ChannelPush, Content: "hello"},
		},
		{
			name:    "invalid channel",
			item:    createItem{Channel: "slack", Content: "hello"},
			wantErr: "channel",
		},
		{
			name:    "empty content",
			item:    createItem{Channel: models.ChannelSMS},
			wantErr: "content",
		},
		{
			name:    "content over sms limit",
			item:    createItem{Channel: models.ChannelSMS, Content: strings.Repeat("x", MaxContentSMS+1)},
			wantErr: "limit",
		},
		{
			name:    "invalid priority",
			item:    createItem{Channel: models.ChannelSMS, Content: "hello", Priority: "urgent"},
			wantErr: "priority",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateItem(tt.item)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateItem() unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateItem() error = nil, want error mentioning %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateItem() error = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}
