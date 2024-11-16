package replicator

import (
	"context"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/nats-io/nats.go"
	"github.com/pgflo/pg_flo/pkg/pgflonats"
)

type Replicator interface {
	StartReplication() error
}

type ReplicationConnection interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
	CreateReplicationSlot(ctx context.Context, slotName string) (pglogrepl.CreateReplicationSlotResult, error)
	StartReplication(ctx context.Context, slotName string, startLSN pglogrepl.LSN, options pglogrepl.StartReplicationOptions) error
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	SendStandbyStatusUpdate(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error
}

type StandardConnection interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Acquire(ctx context.Context) (PgxPoolConn, error)
}

type PgxPoolConn interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Release()
}

type NATSClient interface {
	PublishMessage(subject string, data []byte) error
	Close() error
	SaveState(state pgflonats.State) error
	GetState() (pgflonats.State, error)
	JetStream() nats.JetStreamContext
}
