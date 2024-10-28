package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type ReplayWorker struct {
	*Worker
	startTime time.Time
	endTime   time.Time
}

func NewReplayWorker(w *Worker, startTime, endTime time.Time) *ReplayWorker {
	return &ReplayWorker{
		Worker:    w,
		startTime: startTime,
		endTime:   endTime,
	}
}

func (w *ReplayWorker) Start(ctx context.Context) error {
	stream := fmt.Sprintf("pgflo_%s_stream", w.group)
	subject := fmt.Sprintf("pgflo.%s", w.group)

	w.logger.Info().
		Str("stream", stream).
		Str("subject", subject).
		Str("group", w.group).
		Time("start_time", w.startTime).
		Time("end_time", w.endTime).
		Msg("Starting replay worker")

	js := w.natsClient.JetStream()

	// Create unique consumer name for replay
	// TODO - uniq?
	consumerName := fmt.Sprintf("pgflo_%s_replay_%d", w.group, time.Now().UnixNano())

	consumerConfig := &nats.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: subject,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverByStartTimePolicy,
		OptStartTime:  &w.startTime,
		ReplayPolicy:  nats.ReplayOriginalPolicy,
		MaxDeliver:    1, // Only deliver once
		AckWait:       30 * time.Second,
		MaxAckPending: w.batchSize,
	}

	_, err := js.AddConsumer(stream, consumerConfig)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to add replay consumer")
		return fmt.Errorf("failed to add replay consumer: %w", err)
	}

	// Cleanup consumer when done
	defer func() {
		if err := js.DeleteConsumer(stream, consumerName); err != nil {
			w.logger.Error().Err(err).Msg("Failed to delete replay consumer")
		}
	}()

	sub, err := js.PullSubscribe(subject, consumerName)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to subscribe to subject")
		return fmt.Errorf("failed to subscribe to subject: %w", err)
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.processReplayMessages(ctx, sub); err != nil && err != context.Canceled {
			w.logger.Error().Err(err).Msg("Error processing replay messages")
		}
	}()

	<-ctx.Done()
	w.logger.Info().Msg("Received shutdown signal. Initiating graceful shutdown...")

	w.wg.Wait()
	w.logger.Debug().Msg("All goroutines finished")

	return w.flushBuffer()
}

func (w *ReplayWorker) processReplayMessages(ctx context.Context, sub *nats.Subscription) error {
	flushTicker := time.NewTicker(w.flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info().Msg("Flushing remaining messages")
			return w.flushBuffer()
		case <-flushTicker.C:
			if err := w.flushBuffer(); err != nil {
				w.logger.Error().Err(err).Msg("Failed to flush buffer on interval")
			}
		default:
			msgs, err := sub.Fetch(w.batchSize, nats.MaxWait(500*time.Millisecond))
			if err != nil {
				if err == nats.ErrTimeout {
					continue
				}
				w.logger.Error().Err(err).Msg("Error fetching messages")
				continue
			}

			for _, msg := range msgs {
				metadata, err := msg.Metadata()
				if err != nil {
					w.logger.Error().Err(err).Msg("Failed to get message metadata")
					continue
				}

				// Check if message is within time range
				// TODO - alternative?
				if metadata.Timestamp.After(w.endTime) {
					w.logger.Info().Msg("Reached end time, finishing replay")
					return w.flushBuffer()
				}

				if err := w.processMessage(msg); err != nil {
					w.logger.Error().Err(err).Msg("Failed to process message")
				}
				if err := msg.Ack(); err != nil {
					w.logger.Error().Err(err).Msg("Failed to acknowledge message")
				}
			}

			if len(w.buffer) >= w.batchSize {
				if err := w.flushBuffer(); err != nil {
					w.logger.Error().Err(err).Msg("Failed to flush buffer")
				}
			}
		}
	}
}
