// Package delivery consumes the priority delivery topics and dispatches
// notifications to the external provider with rate limiting, retries, and a DLQ.
package delivery

import "time"

// backoff returns base*2^(attempt-1) capped at maxDelay, with ±jitter applied. rnd returns [0,1].
func backoff(attempt int, base, maxDelay time.Duration, jitter float64, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	d := base
	for i := 1; i < attempt; i++ {
		d <<= 1
		if d >= maxDelay {
			d = maxDelay
			break
		}
	}
	if d > maxDelay {
		d = maxDelay
	}

	if jitter > 0 {
		factor := 1 + jitter*(2*rnd()-1)
		d = time.Duration(float64(d) * factor)
	}
	return d
}
