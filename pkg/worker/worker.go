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

// Worker represents a worker that processes messages from NATS.
type Worker struct {
	natsClient *pgflonats.NATSClient
	ruleEngine *rules.RuleEngine
	sink       sinks.Sink
	group      string
	logger     zerolog.Logger
	batchSize  int
	maxRetries int
}

// NewWorker creates and returns a new Worker instance with the provided NATS client, rule engine, sink, and group.
func NewWorker(natsClient *pgflonats.NATSClient, ruleEngine *rules.RuleEngine, sink sinks.Sink, group string) *Worker {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Str("component", "worker").Logger()

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

// Start begins the worker's message processing loop, setting up the NATS consumer and processing messages.
func (w *Worker) Start(ctx context.Context) error {
	stream := fmt.Sprintf("pgflo_%s_stream", w.group)
	subject := fmt.Sprintf("pgflo.%s", w.group)

	w.logger.Info().
		Str("stream", stream).
		Str("subject", subject).
		Str("group", w.group).
		Msg("Starting worker")

	state, err := w.natsClient.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get state: %w", err)
	}

	js := w.natsClient.JetStream()
	streamInfo, err := js.Stream(ctx, stream)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to get stream info")
		return fmt.Errorf("failed to get stream info: %w", err)
	}

	info, err := streamInfo.Info(ctx)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to get stream info details")
		return fmt.Errorf("failed to get stream info details: %w", err)
	}

	w.logger.Info().
		Uint64("messages", info.State.Msgs).
		Uint64("first_seq", info.State.FirstSeq).
		Uint64("last_seq", info.State.LastSeq).
		Msg("Stream info")

	startSeq := state.LastProcessedSeq + 1
	if startSeq < info.State.FirstSeq {
		w.logger.Warn().
			Uint64("start_seq", startSeq).
			Uint64("stream_first_seq", info.State.FirstSeq).
			Msg("Start sequence is before the first available message, adjusting to stream's first sequence")
		startSeq = info.State.FirstSeq
	}

	w.logger.Info().Uint64("start_seq", startSeq).Msg("Starting consumer from sequence")

	consumerConfig := jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
		DeliverPolicy:  jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:    startSeq,
	}

	cons, err := js.OrderedConsumer(ctx, stream, consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create ordered consumer: %w", err)
	}

	return w.processMessages(ctx, cons)
}

// processMessages continuously processes messages from the NATS consumer.
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

// processMessage handles a single message, applying rules, writing to the sink, and updating the last processed sequence.
func (w *Worker) processMessage(msg jetstream.Msg) error {
	metadata, err := msg.Metadata()
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to get message metadata")
		return err
	}

	w.logger.Debug().
		Uint64("stream_seq", metadata.Sequence.Stream).
		Uint64("consumer_seq", metadata.Sequence.Consumer).
		Msg("Processing message")

	var cdcMessage utils.CDCMessage
	err = cdcMessage.UnmarshalBinary(msg.Data())
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

	state, err := w.natsClient.GetState(context.Background())
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to get current state")
		return err
	}

	if metadata.Sequence.Stream > state.LastProcessedSeq {
		state.LastProcessedSeq = metadata.Sequence.Stream
		if err := w.natsClient.SaveState(context.Background(), state); err != nil {
			w.logger.Error().Err(err).Msg("Failed to save state")
		} else {
			w.logger.Debug().Uint64("last_processed_seq", state.LastProcessedSeq).Msg("Updated last processed sequence")
		}
	}

	return nil
}
