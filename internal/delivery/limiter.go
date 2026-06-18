package delivery

import (
	"context"
	"sync"

	"golang.org/x/time/rate"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

// channelLimiter rate-limits each channel independently so one channel can't drain another's budget.
type channelLimiter struct {
	perSecond rate.Limit
	burst     int

	mu       sync.Mutex
	limiters map[models.Channel]*rate.Limiter
}

func newChannelLimiter(perSecond, burst int) *channelLimiter {
	return &channelLimiter{
		perSecond: rate.Limit(perSecond),
		burst:     burst,
		limiters:  make(map[models.Channel]*rate.Limiter),
	}
}

// wait blocks until a token is available for ch or ctx is done.
func (c *channelLimiter) wait(ctx context.Context, ch models.Channel) error {
	return c.forChannel(ch).Wait(ctx)
}

func (c *channelLimiter) forChannel(ch models.Channel) *rate.Limiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.limiters[ch]
	if !ok {
		l = rate.NewLimiter(c.perSecond, c.burst)
		c.limiters[ch] = l
	}
	return l
}
