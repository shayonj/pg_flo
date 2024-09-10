package worker

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/shayonj/pg_flo/pkg/pgflonats"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/sinks"
	"github.com/shayonj/pg_flo/pkg/utils"
)

type Worker struct {
	natsClient *pgflonats.NATSClient
	ruleEngine *rules.RuleEngine
	sink       sinks.Sink
	group      string
	logger     zerolog.Logger
}

func NewWorker(natsClient *pgflonats.NATSClient, ruleEngine *rules.RuleEngine, sink sinks.Sink, group string) *Worker {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
	return &Worker{
		natsClient: natsClient,
		ruleEngine: ruleEngine,
		sink:       sink,
		group:      group,
		logger:     logger,
	}
}

func (w *Worker) Start(ctx context.Context) error {
	stream := fmt.Sprintf("pgflo_%s_stream", w.group)
	consumer := fmt.Sprintf("pgflo_%s_consumer", w.group)

	w.logger.Info().Str("stream", stream).Str("consumer", consumer).Msg("Starting worker")

	_, err := w.natsClient.GetStreamInfo()
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to get stream info")
		return fmt.Errorf("failed to get stream info: %w", err)
	}

	_, err = w.natsClient.CreateConsumer(consumer)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to create consumer")
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	sub, err := w.natsClient.Subscribe(stream, consumer, w.processMessage)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to subscribe")
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer sub.Unsubscribe()

	w.logger.Info().Msg("Worker started successfully")

	<-ctx.Done()
	w.logger.Info().Msg("Worker stopping")
	return nil
}

func (w *Worker) processMessage(msg *nats.Msg) {
	var cdcMessage utils.CDCMessage
	err := cdcMessage.UnmarshalBinary(msg.Data)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to unmarshal message")
		return
	}

	w.logger.Debug().
		Str("type", cdcMessage.Type).
		Str("schema", cdcMessage.Schema).
		Str("table", cdcMessage.Table).
		Str("lsn", cdcMessage.LSN.String()).
		Msg("Processing message")

	if w.ruleEngine != nil {
		processedMessage, err := w.ruleEngine.ApplyRules(&cdcMessage)
		if err != nil {
			w.logger.Error().Err(err).Msg("Failed to apply rules")
			return
		}
		if processedMessage == nil {
			w.logger.Debug().Msg("Message filtered out by rules")
			return
		}
		cdcMessage = *processedMessage
	}

	err = w.sink.WriteBatch([]*utils.CDCMessage{&cdcMessage})
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to write to sink")
		return
	}

	w.logger.Debug().
		Str("type", cdcMessage.Type).
		Str("schema", cdcMessage.Schema).
		Str("table", cdcMessage.Table).
		Str("lsn", cdcMessage.LSN.String()).
		Msg("Message processed and written to sink")

	msg.Ack()
}
