package delivery

import "time"

// backoff computes the retry delay for a given attempt using exponential
// growth (base * 2^(attempt-1)) capped at max, then applies symmetric jitter.
// rnd must return a value in [0,1]; jitter 0.2 spreads the result by ±20%.
func backoff(attempt int, base, max time.Duration, jitter float64, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	d := base
	for i := 1; i < attempt; i++ {
		d <<= 1
		if d >= max {
			d = max
			break
		}
	}
	if d > max {
		d = max
	}

	if jitter > 0 {
		factor := 1 + jitter*(2*rnd()-1)
		d = time.Duration(float64(d) * factor)
	}
	return d
}
