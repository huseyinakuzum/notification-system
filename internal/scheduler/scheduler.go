// Package scheduler runs the due-engine that flips scheduled notifications to
// queued once their gate time is reached.
package scheduler

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/huseyinakuzum/notification-system/internal/obs"
)

// Store is the unified due-engine: it flips scheduled rows whose gate has been
// reached to queued, covering both first-send and retry re-release.
type Store interface {
	FlipDue(ctx context.Context, batch int) (int, error)
}

// Config controls the poll cadence and per-poll flip batch size.
type Config struct {
	PollInterval time.Duration
	BatchSize    int
}

// Scheduler polls the store on a fixed interval, flipping due rows to queued.
type Scheduler struct {
	store    Store
	cfg      Config
	logger   *slog.Logger
	lastPoll atomic.Int64
}

// New builds a Scheduler backed by store with the given config and logger.
func New(store Store, cfg Config, logger *slog.Logger) *Scheduler {
	return &Scheduler{store: store, cfg: cfg, logger: logger}
}

// Run polls until ctx is cancelled, returning nil on graceful shutdown.
func (s *Scheduler) Run(ctx context.Context) error {
	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.poll(ctx)
		}
	}
}

// LastPoll reports the time of the most recent successful poll, or the zero
// time if no poll has succeeded yet.
func (s *Scheduler) LastPoll() time.Time {
	ns := s.lastPoll.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (s *Scheduler) poll(ctx context.Context) {
	n, err := s.store.FlipDue(ctx, s.cfg.BatchSize)
	if err != nil {
		obs.SchedulerPollErrors.Inc()
		s.logger.Error("scheduler poll", "error", err)
		return
	}
	s.lastPoll.Store(time.Now().UnixNano())
	if n > 0 {
		obs.SchedulerFlipped.Add(float64(n))
		s.logger.Info("scheduler queued", "count", n)
	}
}
