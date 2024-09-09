package worker

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
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
}

func NewWorker(natsClient *pgflonats.NATSClient, ruleEngine *rules.RuleEngine, sink sinks.Sink, group string) *Worker {
	return &Worker{
		natsClient: natsClient,
		ruleEngine: ruleEngine,
		sink:       sink,
		group:      group,
	}
}

func (w *Worker) Start(ctx context.Context) error {
	stream := fmt.Sprintf("pgflo_%s_stream", w.group)
	consumer := fmt.Sprintf("pgflo_%s_consumer", w.group)

	_, err := w.natsClient.GetStreamInfo()
	if err != nil {
		return fmt.Errorf("failed to get stream info: %w", err)
	}

	_, err = w.natsClient.CreateConsumer(consumer)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	sub, err := w.natsClient.Subscribe(stream, consumer, w.processMessage)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer sub.Unsubscribe()

	<-ctx.Done()
	return nil
}

func (w *Worker) processMessage(msg *nats.Msg) {
	var cdcMessage utils.CDCMessage
	err := cdcMessage.UnmarshalBinary(msg.Data)
	if err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal message")
		return
	}

	if w.ruleEngine != nil {
		processedMessage, err := w.ruleEngine.ApplyRules(&cdcMessage)
		if err != nil {
			log.Error().Err(err).Msg("Failed to apply rules")
			return
		}
		if processedMessage == nil {
			// Message filtered out by rules
			return
		}
		cdcMessage = *processedMessage
	}

	err = w.sink.WriteBatch([]*utils.CDCMessage{&cdcMessage})
	if err != nil {
		log.Error().Err(err).Msg("Failed to write to sink")
		return
	}

	err = w.sink.SetLastLSN(cdcMessage.LSN)
	if err != nil {
		log.Error().Err(err).Msg("Failed to set last LSN")
	}

	msg.Ack()
}
