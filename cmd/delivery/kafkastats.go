package main

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/huseyinakuzum/notification-system/internal/obs"
)

const kafkaStatsInterval = 5 * time.Second

// exportKafkaStats polls each reader's Stats() on a fixed interval and feeds the
// obs metrics. kafka-go reports Lag/Offset/Queue* as current values (Set) and
// Messages/Fetches/Errors/Rebalances/Timeouts as deltas since the last call (Add).
func exportKafkaStats(ctx context.Context, readers [3]*kafkago.Reader) {
	ticker := time.NewTicker(kafkaStatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i, r := range readers {
				topic := priorityTopics[i].topic
				s := r.Stats()

				obs.KafkaReaderLag.WithLabelValues(topic).Set(float64(s.Lag))
				obs.KafkaReaderOffset.WithLabelValues(topic).Set(float64(s.Offset))
				obs.KafkaReaderQueueLength.WithLabelValues(topic).Set(float64(s.QueueLength))
				obs.KafkaReaderQueueCapacity.WithLabelValues(topic).Set(float64(s.QueueCapacity))

				obs.KafkaReaderMessages.WithLabelValues(topic).Add(float64(s.Messages))
				obs.KafkaReaderFetches.WithLabelValues(topic).Add(float64(s.Fetches))
				obs.KafkaReaderErrors.WithLabelValues(topic).Add(float64(s.Errors))
				obs.KafkaReaderRebalances.WithLabelValues(topic).Add(float64(s.Rebalances))
				obs.KafkaReaderTimeouts.WithLabelValues(topic).Add(float64(s.Timeouts))
			}
		}
	}
}
