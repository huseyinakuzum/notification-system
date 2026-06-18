// Package models defines the domain types for the notification system and the
// state-machine rules that govern legal status transitions.
package models

// Status is the lifecycle state of a notification.
type Status string

// Notification lifecycle states.
const (
	StatusScheduled  Status = "scheduled"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

func (s Status) Valid() bool {
	switch s {
	case StatusScheduled, StatusQueued, StatusProcessing, StatusSent, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// Channel is the delivery transport for a notification.
type Channel string

// Supported delivery channels.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	default:
		return false
	}
}

// Priority controls relative scheduling urgency of a notification.
type Priority string

// Supported priority levels.
const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	default:
		return false
	}
}
