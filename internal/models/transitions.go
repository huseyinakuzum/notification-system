package models

import "slices"

// notificationTransitions holds the legal status edges for notifications.
//
// scheduled  -> queued, cancelled
// queued     -> processing
// processing -> sent, failed, scheduled   (scheduled = retry re-release)
var notificationTransitions = map[Status][]Status{
	StatusScheduled:  {StatusQueued, StatusCancelled},
	StatusQueued:     {StatusProcessing},
	StatusProcessing: {StatusSent, StatusFailed, StatusScheduled},
}

// NotificationCanTransition reports whether a notifications row may move from
// one status to another. A transition to the same status is never legal.
func NotificationCanTransition(from, to Status) bool {
	return canTransition(notificationTransitions, from, to)
}

func canTransition(table map[Status][]Status, from, to Status) bool {
	if from == to {
		return false
	}
	return slices.Contains(table[from], to)
}
