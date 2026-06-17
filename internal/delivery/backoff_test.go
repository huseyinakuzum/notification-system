package delivery

import (
	"testing"
	"time"
)

func TestBackoffGrowsExponentially(t *testing.T) {
	base := time.Second
	max := time.Hour
	// jitter 0 → deterministic base*2^(attempt-1)
	cases := map[int]time.Duration{
		1: 1 * time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,
	}
	for attempt, want := range cases {
		got := backoff(attempt, base, max, 0, func() float64 { return 0.5 })
		if got != want {
			t.Errorf("attempt %d: got %v, want %v", attempt, got, want)
		}
	}
}

func TestBackoffCapsAtMax(t *testing.T) {
	got := backoff(20, time.Second, 5*time.Minute, 0, func() float64 { return 0.5 })
	if got != 5*time.Minute {
		t.Errorf("got %v, want cap 5m", got)
	}
}

func TestBackoffAppliesJitter(t *testing.T) {
	base := 10 * time.Second
	// jitter 0.2 → range [8s, 12s]. rnd=0 → -20%, rnd=1 → +20%.
	low := backoff(1, base, time.Hour, 0.2, func() float64 { return 0 })
	high := backoff(1, base, time.Hour, 0.2, func() float64 { return 1 })
	if low != 8*time.Second {
		t.Errorf("rnd=0: got %v, want 8s", low)
	}
	if high != 12*time.Second {
		t.Errorf("rnd=1: got %v, want 12s", high)
	}
}

func TestBackoffAttemptFloor(t *testing.T) {
	// attempt <= 0 treated as 1.
	got := backoff(0, time.Second, time.Hour, 0, func() float64 { return 0.5 })
	if got != time.Second {
		t.Errorf("got %v, want 1s", got)
	}
}
