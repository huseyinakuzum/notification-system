package delivery

import (
	"context"
	"testing"
	"time"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

func TestChannelLimiterThrottles(t *testing.T) {
	// 50/s, burst 1 → ~20ms between grants after the first.
	lim := newChannelLimiter(50, 1)
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 3; i++ {
		if err := lim.wait(ctx, models.ChannelSMS); err != nil {
			t.Fatalf("wait %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Errorf("3 grants took %v, expected >=30ms throttle", elapsed)
	}
}

func TestChannelLimiterIsolatesChannels(t *testing.T) {
	// Each channel has its own bucket; first grant on each is immediate.
	lim := newChannelLimiter(1, 1) // 1/s: second grant on same channel would block ~1s
	ctx := context.Background()
	start := time.Now()
	if err := lim.wait(ctx, models.ChannelSMS); err != nil {
		t.Fatal(err)
	}
	if err := lim.wait(ctx, models.ChannelEmail); err != nil {
		t.Fatal(err)
	}
	if err := lim.wait(ctx, models.ChannelPush); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("independent channels blocked each other: %v", elapsed)
	}
}

func TestChannelLimiterRespectsContext(t *testing.T) {
	lim := newChannelLimiter(1, 1) // 1/s
	ctx, cancel := context.WithCancel(context.Background())
	if err := lim.wait(ctx, models.ChannelSMS); err != nil { // consume burst
		t.Fatal(err)
	}
	cancel() // next wait should fail fast, not block ~1s
	if err := lim.wait(ctx, models.ChannelSMS); err == nil {
		t.Error("expected error from cancelled context")
	}
}
