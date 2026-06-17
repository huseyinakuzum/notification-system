package scheduler

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

type fakeStore struct {
	flips     atomic.Int64
	lastBatch atomic.Int64
}

func (f *fakeStore) FlipDue(_ context.Context, batch int) (int, error) {
	f.flips.Add(1)
	f.lastBatch.Store(int64(batch))
	return 1, nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func TestSchedulerRunsLoopAndStops(t *testing.T) {
	store := &fakeStore{}
	cfg := Config{
		PollInterval: 5 * time.Millisecond,
		BatchSize:    42,
	}
	s := New(store, cfg, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = s.Run(ctx)
		close(done)
	}()

	time.Sleep(60 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancel")
	}

	if store.flips.Load() == 0 {
		t.Error("poll loop never flipped due rows")
	}
	if got := store.lastBatch.Load(); got != 42 {
		t.Errorf("flip batch = %d, want 42", got)
	}
}

func TestSchedulerLastPollAdvances(t *testing.T) {
	store := &fakeStore{}
	s := New(store, Config{
		PollInterval: 5 * time.Millisecond,
		BatchSize:    1,
	}, discardLogger())

	if !s.LastPoll().IsZero() {
		t.Fatal("LastPoll should be zero before any tick")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	deadline := time.After(time.Second)
	for s.LastPoll().IsZero() {
		select {
		case <-deadline:
			t.Fatal("LastPoll never advanced")
		case <-time.After(2 * time.Millisecond):
		}
	}
}
