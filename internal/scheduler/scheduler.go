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

type Config struct {
	PollInterval time.Duration
	BatchSize    int
}

type Scheduler struct {
	store    Store
	cfg      Config
	logger   *slog.Logger
	lastPoll atomic.Int64
}

func New(store Store, cfg Config, logger *slog.Logger) *Scheduler {
	return &Scheduler{store: store, cfg: cfg, logger: logger}
}

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
