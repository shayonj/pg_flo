package worker

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/shayonj/pg_flo/pkg/pgflonats"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/sinks"
	"github.com/shayonj/pg_flo/pkg/utils"
)

// Worker represents a worker that processes messages from NATS
type Worker struct {
	natsClient *pgflonats.NATSClient
	ruleEngine *rules.RuleEngine
	sink       sinks.Sink
	group      string
	logger     zerolog.Logger
	batchSize  int
	maxRetries int
}

// NewWorker creates and returns a new Worker instance
func NewWorker(natsClient *pgflonats.NATSClient, ruleEngine *rules.RuleEngine, sink sinks.Sink, group string) *Worker {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	return &Worker{
		natsClient: natsClient,
		ruleEngine: ruleEngine,
		sink:       sink,
		group:      group,
		logger:     logger,
		batchSize:  100,
		maxRetries: 3,
	}
}

// Start begins the worker's message processing loop
func (w *Worker) Start(ctx context.Context) error {
	stream := fmt.Sprintf("pgflo_%s_stream", w.group)
	subject := fmt.Sprintf("pgflo.%s", w.group)

	w.logger.Info().
		Str("stream", stream).
		Str("subject", subject).
		Str("group", w.group).
		Msg("Starting worker")

	js := w.natsClient.JetStream()

	cons, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
	})
	if err != nil {
		return fmt.Errorf("failed to create ordered consumer: %w", err)
	}

	return w.processMessages(ctx, cons)
}

// processMessages continuously processes messages from the NATS consumer
func (w *Worker) processMessages(ctx context.Context, cons jetstream.Consumer) error {
	iter, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("failed to get message iterator: %w", err)
	}
	defer iter.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := iter.Next()
			if err != nil {
				w.logger.Error().Err(err).Msg("Failed to get next message")
				continue
			}

			if err := w.processMessage(msg); err != nil {
				w.logger.Error().Err(err).Msg("Failed to process message")
				// Note: OrderedConsumer doesn't support Nak()
			}
			// Note: OrderedConsumer doesn't require explicit Ack()
		}
	}
}

// processMessage handles a single message, applying rules and writing to the sink
func (w *Worker) processMessage(msg jetstream.Msg) error {
	var cdcMessage utils.CDCMessage
	err := cdcMessage.UnmarshalBinary(msg.Data())
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to unmarshal message")
		return err
	}

	if w.ruleEngine != nil {
		processedMessage, err := w.ruleEngine.ApplyRules(&cdcMessage)
		if err != nil {
			w.logger.Error().Err(err).Msg("Failed to apply rules")
			return err
		}
		if processedMessage == nil {
			w.logger.Debug().Msg("Message filtered out by rules")
			return nil
		}
		cdcMessage = *processedMessage
	}

	err = w.sink.WriteBatch([]*utils.CDCMessage{&cdcMessage})
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to write to sink")
		return err
	}

	return nil
}
