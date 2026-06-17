package models

import "testing"

func TestStatusValid(t *testing.T) {
	t.Parallel()
	valid := []Status{StatusScheduled, StatusQueued, StatusProcessing, StatusSent, StatusFailed, StatusCancelled}
	for _, s := range valid {
		if !s.Valid() {
			t.Errorf("Status(%q).Valid() = false, want true", s)
		}
	}
	invalid := []Status{"", "PENDING", "pending", "done", "unknown"}
	for _, s := range invalid {
		if s.Valid() {
			t.Errorf("Status(%q).Valid() = true, want false", s)
		}
	}
}

func TestChannelValid(t *testing.T) {
	t.Parallel()
	valid := []Channel{ChannelSMS, ChannelEmail, ChannelPush}
	for _, c := range valid {
		if !c.Valid() {
			t.Errorf("Channel(%q).Valid() = false, want true", c)
		}
	}
	invalid := []Channel{"", "SMS", "webhook", "slack"}
	for _, c := range invalid {
		if c.Valid() {
			t.Errorf("Channel(%q).Valid() = true, want false", c)
		}
	}
}

func TestPriorityValid(t *testing.T) {
	t.Parallel()
	valid := []Priority{PriorityHigh, PriorityNormal, PriorityLow}
	for _, p := range valid {
		if !p.Valid() {
			t.Errorf("Priority(%q).Valid() = false, want true", p)
		}
	}
	invalid := []Priority{"", "HIGH", "urgent", "medium"}
	for _, p := range invalid {
		if p.Valid() {
			t.Errorf("Priority(%q).Valid() = true, want false", p)
		}
	}
}
