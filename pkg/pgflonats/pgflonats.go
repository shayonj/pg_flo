package pgflonats

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/nats-io/nats.go"
)

const (
	defaultNATSURL = "nats://localhost:4222"
	envNATSURL     = "PG_FLO_NATS_URL"
)

type NATSClient struct {
	conn        *nats.Conn
	js          nats.JetStreamContext
	stream      string
	stateBucket string
}

// NewNATSClient creates a new NATS client with the specified configuration
func NewNATSClient(url, stream, group string) (*NATSClient, error) {
	if url == "" {
		url = os.Getenv(envNATSURL)
		if url == "" {
			url = defaultNATSURL
		}
	}

	if stream == "" {
		stream = fmt.Sprintf("pgflo_%s_stream", group)
	}

	nc, err := nats.Connect(url,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			fmt.Printf("Disconnected due to: %s, will attempt reconnects\n", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			fmt.Printf("Reconnected [%s]\n", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			fmt.Printf("Exiting: %v\n", nc.LastError())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	stateBucket := fmt.Sprintf("pg_flo_state_%s", group)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     stateBucket,
		Subjects: []string{stateBucket},
		Storage:  nats.FileStorage,
		Replicas: 1, // TODO - Adjust based on NATS JetStream setup
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create state stream: %w", err)
	}

	return &NATSClient{
		conn:        nc,
		js:          js,
		stream:      stream,
		stateBucket: stateBucket,
	}, nil
}

// PublishMessage publishes a message to the specified NATS subject
func (nc *NATSClient) PublishMessage(subject string, data []byte) error {
	_, err := nc.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

// Close closes the NATS connection
func (nc *NATSClient) Close() error {
	nc.conn.Close()
	return nil
}

// GetStreamInfo retrieves information about the NATS stream
func (nc *NATSClient) GetStreamInfo() (*nats.StreamInfo, error) {
	return nc.js.StreamInfo(nc.stream)
}

// PurgeStream purges all messages from the NATS stream
func (nc *NATSClient) PurgeStream() error {
	return nc.js.PurgeStream(nc.stream)
}

// DeleteStream deletes the NATS stream
func (nc *NATSClient) DeleteStream() error {
	return nc.js.DeleteStream(nc.stream)
}

// SaveState saves the current replication state to NATS
func (nc *NATSClient) SaveState(lsn pglogrepl.LSN) error {
	state := struct {
		LSN pglogrepl.LSN `json:"lsn"`
	}{LSN: lsn}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}

	_, err = nc.js.Publish(nc.stateBucket, data)
	if err != nil {
		return fmt.Errorf("failed to publish state: %v", err)
	}

	return nil
}

// GetLastState retrieves the last saved replication state from NATS
func (nc *NATSClient) GetLastState() (pglogrepl.LSN, error) {
	info, err := nc.js.StreamInfo(nc.stateBucket)
	if err != nil {
		if err == nats.ErrStreamNotFound {
			return 0, nil // No stream yet, start from the beginning
		}
		return 0, fmt.Errorf("failed to get stream info: %v", err)
	}

	if info.State.LastSeq == 0 {
		return 0, nil
	}

	msg, err := nc.js.GetMsg(nc.stateBucket, info.State.LastSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to get last state: %v", err)
	}

	var state struct {
		LSN pglogrepl.LSN `json:"lsn"`
	}
	if err := json.Unmarshal(msg.Data, &state); err != nil {
		return 0, fmt.Errorf("failed to unmarshal state: %v", err)
	}

	return state.LSN, nil
}
