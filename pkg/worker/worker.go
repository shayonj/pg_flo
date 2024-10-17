package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/shayonj/pg_flo/pkg/pgflonats"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/sinks"
	"github.com/shayonj/pg_flo/pkg/utils"
)

// Worker represents a worker that processes messages from NATS.
type Worker struct {
	natsClient     *pgflonats.NATSClient
	ruleEngine     *rules.RuleEngine
	sink           sinks.Sink
	group          string
	logger         zerolog.Logger
	batchSize      int
	buffer         []*utils.CDCMessage
	lastSavedState uint64
	flushInterval  time.Duration
	shutdownCh     chan struct{}
	wg             sync.WaitGroup
}

// NewWorker creates and returns a new Worker instance with the provided NATS client, rule engine, sink, and group.
func NewWorker(natsClient *pgflonats.NATSClient, ruleEngine *rules.RuleEngine, sink sinks.Sink, group string) *Worker {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Str("component", "worker").Logger()

	return &Worker{
		natsClient:     natsClient,
		ruleEngine:     ruleEngine,
		sink:           sink,
		group:          group,
		logger:         logger,
		batchSize:      1000,
		buffer:         make([]*utils.CDCMessage, 0, 1000),
		lastSavedState: 0,
		flushInterval:  1 * time.Second,
		shutdownCh:     make(chan struct{}),
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

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.processMessages(ctx, cons); err != nil && err != context.Canceled {
			w.logger.Error().Err(err).Msg("Error processing messages")
		}
	}()

	<-ctx.Done()
	w.logger.Info().Msg("Received shutdown signal. Initiating graceful shutdown...")

	w.wg.Wait()
	w.logger.Debug().Msg("All goroutines finished")

	return w.flushBuffer()
}

// processMessages continuously processes messages from the NATS consumer.
func (w *Worker) processMessages(ctx context.Context, cons jetstream.Consumer) error {
	iter, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("failed to get message iterator: %w", err)
	}

	flushTicker := time.NewTicker(w.flushInterval)
	defer flushTicker.Stop()

	msgCh := make(chan jetstream.Msg)
	errCh := make(chan error)

	go func() {
		defer close(msgCh)
		for {
			msg, err := iter.Next()
			if err != nil {
				if err == jetstream.ErrMsgIteratorClosed {
					return
				}
				errCh <- err
				return
			}
			select {
			case msgCh <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info().Msg("Flushing remaining messages")
			return w.flushBuffer()
		case <-flushTicker.C:
			if err := w.flushBuffer(); err != nil {
				w.logger.Error().Err(err).Msg("Failed to flush buffer on interval")
			}
		case err := <-errCh:
			w.logger.Error().Err(err).Msg("Error receiving message")
			return err
		case msg, ok := <-msgCh:
			if !ok {
				w.logger.Info().Msg("Message channel closed, flushing buffer")
				return w.flushBuffer()
			}
			if err := w.processMessage(msg); err != nil {
				w.logger.Error().Err(err).Msg("Failed to process message")
			}
			if len(w.buffer) >= w.batchSize {
				if err := w.flushBuffer(); err != nil {
					w.logger.Error().Err(err).Msg("Failed to flush buffer")
				}
			}
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

	w.buffer = append(w.buffer, &cdcMessage)
	w.lastSavedState = metadata.Sequence.Stream

	return nil
}

// flushBuffer writes the buffered messages to the sink and updates the last processed sequence.
func (w *Worker) flushBuffer() error {
	if len(w.buffer) == 0 {
		return nil
	}

	w.logger.Debug().Int("messages", len(w.buffer)).Msg("Flushing buffer")

	err := w.sink.WriteBatch(w.buffer)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to write batch to sink")
		return err
	}

	state, err := w.natsClient.GetState(context.Background())
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to get current state")
		return err
	}

	state.LastProcessedSeq = w.lastSavedState
	if err := w.natsClient.SaveState(context.Background(), state); err != nil {
		w.logger.Error().Err(err).Msg("Failed to save state")
		return err
	}

	w.buffer = w.buffer[:0]
	return nil
}
