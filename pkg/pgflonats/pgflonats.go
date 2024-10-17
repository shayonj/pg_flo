package pgflonats

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultNATSURL = "nats://localhost:4222"
	envNATSURL     = "PG_FLO_NATS_URL"
)

// NATSClient represents a client for interacting with NATS
type NATSClient struct {
	conn        *nats.Conn
	js          jetstream.JetStream
	stream      string
	stateBucket string
}

// State represents the current state of the replication process
type State struct {
	LSN              pglogrepl.LSN `json:"lsn"`
	LastProcessedSeq uint64        `json:"last_processed_seq"`
}

// NewNATSClient creates a new NATS client with the specified configuration, setting up the connection, main stream, and state bucket.
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
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
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

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Create the main stream
	streamConfig := jetstream.StreamConfig{
		Name:      stream,
		Subjects:  []string{fmt.Sprintf("pgflo.%s", group)},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour,
	}
	_, err = js.CreateStream(context.Background(), streamConfig)
	if err != nil && err != jetstream.ErrStreamNameAlreadyInUse {
		return nil, fmt.Errorf("failed to create main stream: %w", err)
	}

	// Create the state bucket
	stateBucket := fmt.Sprintf("pg_flo_state_%s", group)
	_, err = js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket: stateBucket,
	})
	if err != nil && err != jetstream.ErrBucketExists {
		return nil, fmt.Errorf("failed to create state bucket: %w", err)
	}

	return &NATSClient{
		conn:        nc,
		js:          js,
		stream:      stream,
		stateBucket: stateBucket,
	}, nil
}

// PublishMessage publishes a message to the specified NATS subject.
func (nc *NATSClient) PublishMessage(ctx context.Context, subject string, data []byte) error {
	_, err := nc.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

// Close closes the NATS connection.
func (nc *NATSClient) Close() error {
	nc.conn.Close()
	return nil
}

// GetStreamInfo retrieves information about the NATS stream.
func (nc *NATSClient) GetStreamInfo(ctx context.Context) (*jetstream.StreamInfo, error) {
	stream, err := nc.js.Stream(ctx, nc.stream)
	if err != nil {
		return nil, err
	}
	return stream.Info(ctx)
}

// PurgeStream purges all messages from the NATS stream.
func (nc *NATSClient) PurgeStream(ctx context.Context) error {
	stream, err := nc.js.Stream(ctx, nc.stream)
	if err != nil {
		return err
	}
	return stream.Purge(ctx)
}

// DeleteStream deletes the NATS stream.
func (nc *NATSClient) DeleteStream(ctx context.Context) error {
	return nc.js.DeleteStream(ctx, nc.stream)
}

// SaveState saves the current replication state to NATS.
func (nc *NATSClient) SaveState(ctx context.Context, state State) error {
	kv, err := nc.js.KeyValue(ctx, nc.stateBucket)
	if err != nil {
		return fmt.Errorf("failed to get KV bucket: %v", err)
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}

	_, err = kv.Put(ctx, "state", data)
	if err != nil {
		return fmt.Errorf("failed to save state: %v", err)
	}

	return nil
}

// GetState retrieves the last saved state from NATS, initializing a new state if none is found.
func (nc *NATSClient) GetState(ctx context.Context) (State, error) {
	kv, err := nc.js.KeyValue(ctx, nc.stateBucket)
	if err != nil {
		return State{}, fmt.Errorf("failed to get KV bucket: %v", err)
	}

	entry, err := kv.Get(ctx, "state")
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			initialState := State{LastProcessedSeq: 0}
			if err := nc.SaveState(ctx, initialState); err != nil {
				return State{}, fmt.Errorf("failed to save initial state: %v", err)
			}
			return initialState, nil
		}
		return State{}, fmt.Errorf("failed to get state: %v", err)
	}

	var state State
	if err := json.Unmarshal(entry.Value(), &state); err != nil {
		return State{}, fmt.Errorf("failed to unmarshal state: %v", err)
	}

	return state, nil
}

// JetStream returns the JetStream context.
func (nc *NATSClient) JetStream() jetstream.JetStream {
	return nc.js
}
