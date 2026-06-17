package delivery

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Reader is the subset of segmentio/kafka-go's Reader the worker needs: manual
// fetch + commit so offsets advance only after a message is handled.
type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// rescanStore backs DB-as-source-of-truth recovery, independent of Kafka.
type rescanStore interface {
	ListClaimable(ctx context.Context, limit int) ([]uuid.UUID, error)
	ReapStuck(ctx context.Context, timeout time.Duration) (int, error)
}

// Worker owns the three priority readers and drives delivery: a strict
// high→normal→low selector with anti-starvation, plus a periodic DB rescan that
// re-drives due retries and reaps crash-stranded rows. Kafka is a low-latency
// hint; the DB is the source of truth.
type Worker struct {
	readers  [3]Reader // indexed by lane (high, normal, low)
	dispatch *dispatcher
	store    rescanStore
	cfg      Config
	logger   logger
}

// logger is the minimal slog surface the worker uses (keeps the struct testable).
type logger interface {
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
	Info(msg string, args ...any)
}

// Store is the persistence surface the delivery worker needs: per-message claim
// and transitions plus DB-source-of-truth rescan/reap.
type Store interface {
	claimStore
	rescanStore
}

// Config tunes the whole delivery stage: selection, rate limiting, retry timing,
// and crash recovery.
type Config struct {
	AgingThreshold int
	RescanInterval time.Duration
	ReapTimeout    time.Duration
	ClaimBatch     int
	RatePerChannel int
	RateBurst      int
	BackoffBase    time.Duration
	BackoffMax     time.Duration
	BackoffJitter  float64
}

// NewWorker wires the delivery worker from its collaborators. readers must be
// ordered high, normal, low.
func NewWorker(readers [3]Reader, store Store, snd sender, dlq dlqProducer, cfg Config, log *slog.Logger) *Worker {
	d := &dispatcher{
		store:   store,
		sender:  snd,
		dlq:     dlq,
		limiter: newChannelLimiter(cfg.RatePerChannel, cfg.RateBurst),
		cfg: dispatcherConfig{
			BackoffBase:   cfg.BackoffBase,
			BackoffMax:    cfg.BackoffMax,
			BackoffJitter: cfg.BackoffJitter,
		},
		rnd:    rand.Float64,
		logger: log,
	}
	return &Worker{
		readers:  readers,
		dispatch: d,
		store:    store,
		cfg:      cfg,
		logger:   log,
	}
}

// Run blocks until ctx is cancelled, then drains its goroutines.
func (w *Worker) Run(ctx context.Context) error {
	holders := [3]chan kafka.Message{
		make(chan kafka.Message, 1),
		make(chan kafka.Message, 1),
		make(chan kafka.Message, 1),
	}

	var wg sync.WaitGroup
	for l := range 3 {
		wg.Go(func() { w.fetchLoop(ctx, lane(l), holders[l]) })
	}
	wg.Go(func() { w.rescanLoop(ctx) })

	w.selectLoop(ctx, holders)
	wg.Wait()
	return nil
}

func (w *Worker) selectLoop(ctx context.Context, holders [3]chan kafka.Message) {
	p := newPicker(w.cfg.AgingThreshold)
	for {
		if ctx.Err() != nil {
			return
		}
		avail := [3]bool{len(holders[0]) > 0, len(holders[1]) > 0, len(holders[2]) > 0}
		l, ok := p.pick(avail)
		if ok {
			w.process(ctx, l, <-holders[l])
			continue
		}
		// nothing buffered: block until any lane delivers or ctx ends.
		select {
		case <-ctx.Done():
			return
		case m := <-holders[laneHigh]:
			w.process(ctx, laneHigh, m)
		case m := <-holders[laneNormal]:
			w.process(ctx, laneNormal, m)
		case m := <-holders[laneLow]:
			w.process(ctx, laneLow, m)
		}
	}
}

func (w *Worker) fetchLoop(ctx context.Context, l lane, holder chan<- kafka.Message) {
	r := w.readers[l]
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("fetch message", "lane", int(l), "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case holder <- m:
		}
	}
}

func (w *Worker) process(ctx context.Context, l lane, m kafka.Message) {
	id, err := uuid.ParseBytes(m.Key)
	if err != nil {
		w.logger.Error("delivery message has invalid id key", "lane", int(l), "key", string(m.Key))
		w.commit(ctx, l, m) // undecodable: advance offset, the row (if any) is recovered by rescan
		return
	}
	if err := w.dispatch.handle(ctx, id); err != nil {
		if ctx.Err() != nil {
			return
		}
		// leave offset uncommitted: redelivery or DB rescan re-drives it.
		w.logger.Error("handle delivery", "id", id, "error", err)
		return
	}
	w.commit(ctx, l, m)
}

func (w *Worker) commit(ctx context.Context, l lane, m kafka.Message) {
	if err := w.readers[l].CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
		w.logger.Error("commit delivery offset", "lane", int(l), "error", err)
	}
}

func (w *Worker) rescanLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.RescanInterval)
	defer t.Stop()
	w.rescan(ctx) // startup recovery before the first tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.rescan(ctx)
		}
	}
}

func (w *Worker) rescan(ctx context.Context) {
	if n, err := w.store.ReapStuck(ctx, w.cfg.ReapTimeout); err != nil {
		w.logger.Error("reap stuck", "error", err)
	} else if n > 0 {
		w.logger.Info("reaped stuck deliveries", "count", n)
	}

	ids, err := w.store.ListClaimable(ctx, w.cfg.ClaimBatch)
	if err != nil {
		w.logger.Error("list claimable", "error", err)
		return
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		if err := w.dispatch.handle(ctx, id); err != nil && ctx.Err() == nil {
			w.logger.Error("rescan handle", "id", id, "error", err)
		}
	}
}
