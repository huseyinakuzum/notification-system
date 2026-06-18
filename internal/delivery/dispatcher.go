package delivery

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/huseyinakuzum/notification-system/internal/models"
	"github.com/huseyinakuzum/notification-system/internal/obs"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

var tracer = otel.Tracer("delivery")

type claimStore interface {
	Claim(ctx context.Context, id uuid.UUID) (models.Notification, error)
	MarkSent(ctx context.Context, id uuid.UUID, providerMessageID string) error
	MarkRetry(ctx context.Context, id uuid.UUID, nextAttemptAt time.Time, lastErr string) error
	MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) error
}

type sender interface {
	Send(ctx context.Context, n models.Notification) SendResult
}

type dlqProducer interface {
	Produce(ctx context.Context, n models.Notification, reason string) error
}

type dispatcherConfig struct {
	BackoffBase   time.Duration
	BackoffMax    time.Duration
	BackoffJitter float64
}

// dispatcher handles one notification end to end: claim, rate-limit, send, record outcome.
type dispatcher struct {
	store   claimStore
	sender  sender
	dlq     dlqProducer
	limiter *channelLimiter
	cfg     dispatcherConfig
	rnd     func() float64
	logger  *slog.Logger
}

// handle processes one notification id; a non-claimable row is a no-op.
func (d *dispatcher) handle(ctx context.Context, id uuid.UUID) error {
	ctx, span := tracer.Start(ctx, "delivery.handle",
		trace.WithAttributes(attribute.String("notification.id", id.String())))
	defer span.End()

	n, err := d.store.Claim(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.String("notification.channel", string(n.Channel)))

	if err := d.limiter.wait(ctx, n.Channel); err != nil {
		return err
	}

	start := time.Now()
	res := d.sender.Send(ctx, n)
	obs.DeliveryDuration.WithLabelValues(string(n.Channel)).Observe(time.Since(start).Seconds())

	prio := string(n.Priority)
	switch res.Outcome {
	case OutcomeSent:
		obs.DeliveryAttempts.WithLabelValues("sent", string(n.Channel), prio).Inc()
		return d.finish(ctx, d.store.MarkSent(ctx, n.ID, res.ProviderMessageID))
	case OutcomeFatal:
		obs.DeliveryAttempts.WithLabelValues("fatal", string(n.Channel), prio).Inc()
		return d.fail(ctx, n, res.Detail)
	default: // OutcomeRetry
		if n.Attempts >= n.MaxAttempts {
			obs.DeliveryAttempts.WithLabelValues("fatal", string(n.Channel), prio).Inc()
			return d.fail(ctx, n, res.Detail)
		}
		obs.DeliveryAttempts.WithLabelValues("retry", string(n.Channel), prio).Inc()
		next := time.Now().Add(backoff(n.Attempts, d.cfg.BackoffBase, d.cfg.BackoffMax, d.cfg.BackoffJitter, d.rnd))
		return d.finish(ctx, d.store.MarkRetry(ctx, n.ID, next, res.Detail))
	}
}

func (d *dispatcher) fail(ctx context.Context, n models.Notification, reason string) error {
	if err := d.finish(ctx, d.store.MarkFailed(ctx, n.ID, reason)); err != nil {
		return err
	}
	if err := d.dlq.Produce(ctx, n, reason); err != nil {
		d.logger.Error("dlq produce failed", "id", n.ID, "error", err)
		return nil
	}
	obs.DLQProduced.WithLabelValues(string(n.Channel)).Inc()
	return nil
}

// finish swallows ErrConflict (a reaper or duplicate raced us), propagates the rest.
func (d *dispatcher) finish(_ context.Context, err error) error {
	if errors.Is(err, repository.ErrConflict) {
		d.logger.Warn("status transition skipped (row no longer processing)")
		return nil
	}
	return err
}
