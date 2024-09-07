package replicator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"errors"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"
	"github.com/shayonj/pg_flo/pkg/utils"
)

// GeneratePublicationName generates a deterministic publication name based on the group name
func GeneratePublicationName(group string) string {
	group = strings.ReplaceAll(group, "-", "_")
	return fmt.Sprintf("%s_publication", group)
}

// BaseReplicator provides core functionality for PostgreSQL logical replication
type BaseReplicator struct {
	Config          Config
	ReplicationConn ReplicationConnection
	StandardConn    StandardConnection
	Relations       map[uint32]*pglogrepl.RelationMessage
	Logger          zerolog.Logger
	TableDetails    map[string][]string
	LastLSN         pglogrepl.LSN
	NATSClient      NATSClient
}

// NewBaseReplicator creates a new BaseReplicator instance
func NewBaseReplicator(config Config, replicationConn ReplicationConnection, standardConn StandardConnection, natsClient NATSClient) *BaseReplicator {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	br := &BaseReplicator{
		Config:          config,
		ReplicationConn: replicationConn,
		StandardConn:    standardConn,
		Relations:       make(map[uint32]*pglogrepl.RelationMessage),
		Logger:          logger,
		TableDetails:    make(map[string][]string),
		NATSClient:      natsClient,
	}

	// Initialize the OID map with custom types from the database
	if err := InitializeOIDMap(context.Background(), standardConn); err != nil {
		br.Logger.Error().Err(err).Msg("Failed to initialize OID map")
	}

	if err := br.InitializePrimaryKeyInfo(); err != nil {
		br.Logger.Error().Err(err).Msg("Failed to initialize primary key info")
	}

	return br
}

// CreatePublication creates a new publication if it doesn't exist
func (r *BaseReplicator) CreatePublication() error {
	publicationName := GeneratePublicationName(r.Config.Group)
	exists, err := r.checkPublicationExists(publicationName)
	if err != nil {
		return fmt.Errorf("failed to check if publication exists: %w", err)
	}

	if exists {
		r.Logger.Info().Str("publication", publicationName).Msg("Publication already exists")
		return nil
	}

	query := r.buildCreatePublicationQuery()
	_, err = r.StandardConn.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	r.Logger.Info().Str("publication", publicationName).Msg("Publication created successfully")
	return nil
}

// buildCreatePublicationQuery constructs the SQL query for creating a publication
func (r *BaseReplicator) buildCreatePublicationQuery() string {
	publicationName := GeneratePublicationName(r.Config.Group)
	if len(r.Config.Tables) == 0 {
		return fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", publicationName)
	}
	return fmt.Sprintf("CREATE PUBLICATION \"%s\" FOR TABLE %s", publicationName, pgx.Identifier(r.Config.Tables).Sanitize())
}

// checkPublicationExists checks if a publication with the given name exists
func (r *BaseReplicator) checkPublicationExists(publicationName string) (bool, error) {
	var exists bool
	err := r.StandardConn.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", publicationName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking publication: %w", err)
	}
	return exists, nil
}

// StartReplicationFromLSN initiates the replication process from a given LSN
func (r *BaseReplicator) StartReplicationFromLSN(ctx context.Context, startLSN pglogrepl.LSN) error {

	publicationName := GeneratePublicationName(r.Config.Group)
	err := r.ReplicationConn.StartReplication(ctx, publicationName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", publicationName),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	r.Logger.Info().Str("startLSN", startLSN.String()).Msg("Replication started successfully")

	errChan := make(chan error, 1)
	go func() {
		errChan <- r.StreamChanges(ctx)
	}()

	select {
	case <-ctx.Done():
		return r.gracefulShutdown()
	case err := <-errChan:
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	}
}

// StreamChanges continuously processes replication messages
func (r *BaseReplicator) StreamChanges(ctx context.Context) error {
	lastStatusUpdate := time.Now()
	standbyMessageTimeout := time.Second * 10

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := r.ProcessNextMessage(ctx, &lastStatusUpdate, standbyMessageTimeout); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				return err
			}
		}
	}
}

// ProcessNextMessage handles the next replication message
func (r *BaseReplicator) ProcessNextMessage(ctx context.Context, lastStatusUpdate *time.Time, standbyMessageTimeout time.Duration) error {
	msg, err := r.ReplicationConn.ReceiveMessage(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("context error while receiving message: %w", ctx.Err())
		}
		return fmt.Errorf("failed to receive message: %w", err)
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		if err := r.handleCopyData(ctx, msg, lastStatusUpdate); err != nil {
			return err
		}
	default:
		r.Logger.Warn().Type("message", msg).Msg("Received unexpected message")
	}

	if time.Since(*lastStatusUpdate) >= standbyMessageTimeout {
		if err := r.SendStandbyStatusUpdate(ctx); err != nil {
			return fmt.Errorf("failed to send standby status update: %w", err)
		}
		*lastStatusUpdate = time.Now()
	}

	return nil
}

// handleCopyData processes CopyData messages
func (r *BaseReplicator) handleCopyData(ctx context.Context, msg *pgproto3.CopyData, lastStatusUpdate *time.Time) error {
	switch msg.Data[0] {
	case pglogrepl.XLogDataByteID:
		return r.handleXLogData(ctx, msg.Data[1:], lastStatusUpdate)
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return r.handlePrimaryKeepaliveMessage(ctx, msg.Data[1:], lastStatusUpdate)
	default:
		r.Logger.Warn().Uint8("messageType", msg.Data[0]).Msg("Received unexpected CopyData message type")
	}
	return nil
}

// handleXLogData processes XLogData messages
func (r *BaseReplicator) handleXLogData(ctx context.Context, data []byte, lastStatusUpdate *time.Time) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse XLogData: %w", err)
	}

	if err := r.processWALData(ctx, xld.WALData); err != nil {
		return fmt.Errorf("failed to process WAL data: %w", err)
	}

	*lastStatusUpdate = time.Now()
	return nil
}

// handlePrimaryKeepaliveMessage processes PrimaryKeepaliveMessage messages
func (r *BaseReplicator) handlePrimaryKeepaliveMessage(ctx context.Context, data []byte, lastStatusUpdate *time.Time) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("failed to parse primary keepalive message: %w", err)
	}
	if pkm.ReplyRequested {
		if err := r.SendStandbyStatusUpdate(ctx); err != nil {
			return fmt.Errorf("failed to send standby status update: %w", err)
		}
		*lastStatusUpdate = time.Now()
	}
	return nil
}

// processWALData handles different types of WAL messages
func (r *BaseReplicator) processWALData(ctx context.Context, walData []byte) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("failed to parse WAL data: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		r.handleRelationMessage(msg)
	case *pglogrepl.BeginMessage:
		return r.HandleBeginMessage(ctx, msg)
	case *pglogrepl.InsertMessage:
		return r.HandleInsertMessage(ctx, msg)
	case *pglogrepl.UpdateMessage:
		return r.HandleUpdateMessage(ctx, msg)
	case *pglogrepl.DeleteMessage:
		return r.HandleDeleteMessage(ctx, msg)
	case *pglogrepl.CommitMessage:
		return r.HandleCommitMessage(ctx, msg)
	default:
		r.Logger.Warn().Type("message", msg).Msg("Received unexpected logical replication message")
	}

	return nil
}

// handleRelationMessage handles RelationMessage messages
func (r *BaseReplicator) handleRelationMessage(msg *pglogrepl.RelationMessage) {
	r.Relations[msg.RelationID] = msg
	r.Logger.Info().Str("table", msg.RelationName).Uint32("id", msg.RelationID).Msg("Relation message received")
}

// HandleBeginMessage handles BeginMessage messages
func (r *BaseReplicator) HandleBeginMessage(ctx context.Context, msg *pglogrepl.BeginMessage) error {
	return nil
}

// HandleInsertMessage handles InsertMessage messages
func (r *BaseReplicator) HandleInsertMessage(ctx context.Context, msg *pglogrepl.InsertMessage) error {
	relation, ok := r.Relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:     "INSERT",
		Schema:   relation.Namespace,
		Table:    relation.RelationName,
		Columns:  relation.Columns,
		NewTuple: msg.Tuple,
	}

	r.AddPrimaryKeyInfo(&cdcMessage, relation.RelationName)
	return r.PublishToNATS(ctx, cdcMessage)
}

// HandleUpdateMessage handles UpdateMessage messages
func (r *BaseReplicator) HandleUpdateMessage(ctx context.Context, msg *pglogrepl.UpdateMessage) error {
	relation, ok := r.Relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:     "UPDATE",
		Schema:   relation.Namespace,
		Table:    relation.RelationName,
		Columns:  relation.Columns,
		NewTuple: msg.NewTuple,
		OldTuple: msg.OldTuple,
	}

	r.AddPrimaryKeyInfo(&cdcMessage, relation.RelationName)
	return r.PublishToNATS(ctx, cdcMessage)
}

// HandleDeleteMessage handles DeleteMessage messages
func (r *BaseReplicator) HandleDeleteMessage(ctx context.Context, msg *pglogrepl.DeleteMessage) error {
	relation, ok := r.Relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:     "DELETE",
		Schema:   relation.Namespace,
		Table:    relation.RelationName,
		Columns:  relation.Columns,
		OldTuple: msg.OldTuple,
	}

	r.AddPrimaryKeyInfo(&cdcMessage, relation.RelationName)
	return r.PublishToNATS(ctx, cdcMessage)
}

// HandleCommitMessage processes a commit message and publishes it to NATS
func (r *BaseReplicator) HandleCommitMessage(ctx context.Context, msg *pglogrepl.CommitMessage) error {
	r.LastLSN = msg.CommitLSN

	if err := r.SaveState(msg.CommitLSN); err != nil {
		r.Logger.Error().Err(err).Msg("Failed to save replication state")
		return err
	}

	return nil
}

// PublishToNATS publishes a message to NATS
func (r *BaseReplicator) PublishToNATS(ctx context.Context, data utils.CDCMessage) error {
	binaryData, err := data.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	subject := fmt.Sprintf("pgflo.%s", r.Config.Group)
	return r.NATSClient.PublishMessage(subject, binaryData)
}

// AddPrimaryKeyInfo adds primary key information to the CDCMessage
func (r *BaseReplicator) AddPrimaryKeyInfo(message *utils.CDCMessage, table string) {
	if pkColumns, ok := r.TableDetails[table]; ok && len(pkColumns) > 0 {
		message.PrimaryKeyColumn = pkColumns[0]
	}
}

// SendStandbyStatusUpdate sends a status update to the primary server
func (r *BaseReplicator) SendStandbyStatusUpdate(ctx context.Context) error {
	err := r.ReplicationConn.SendStandbyStatusUpdate(ctx, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: r.LastLSN + 1,
		WALFlushPosition: r.LastLSN + 1,
		WALApplyPosition: r.LastLSN + 1,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	})
	if err != nil {
		return fmt.Errorf("failed to send standby status update: %w", err)
	}

	r.Logger.Debug().Str("lsn", r.LastLSN.String()).Msg("Sent standby status update")
	return nil
}

// SendFinalStandbyStatusUpdate sends a final status update before shutting down
func (r *BaseReplicator) SendFinalStandbyStatusUpdate() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.SendStandbyStatusUpdate(ctx); err != nil {
		return fmt.Errorf("failed to send standby status update: %w", err)
	}

	r.Logger.Info().Msg("Sent final standby status update")
	return nil
}

// CreateReplicationSlot ensures that a replication slot exists, creating one if necessary
func (r *BaseReplicator) CreateReplicationSlot(ctx context.Context) error {
	publicationName := GeneratePublicationName(r.Config.Group)
	exists, err := r.CheckReplicationSlotExists(publicationName)
	if err != nil {
		return fmt.Errorf("failed to check replication slot: %w", err)
	}

	if !exists {
		r.Logger.Info().Str("slot", publicationName).Msg("Creating replication slot")
		result, err := r.ReplicationConn.CreateReplicationSlot(ctx, publicationName)
		if err != nil {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
		r.Logger.Info().
			Str("slot", publicationName).
			Str("consistentPoint", result.ConsistentPoint).
			Str("snapshotName", result.SnapshotName).
			Msg("Replication slot created successfully")
	} else {
		r.Logger.Info().Str("slot", publicationName).Msg("Replication slot already exists")
	}

	return nil
}

// CheckReplicationSlotExists checks if a slot with the given name already exists
func (r *BaseReplicator) CheckReplicationSlotExists(slotName string) (bool, error) {
	var exists bool
	err := r.StandardConn.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking replication slot: %w", err)
	}
	return exists, nil
}

// gracefulShutdown performs a graceful shutdown of the replicator
func (r *BaseReplicator) gracefulShutdown() error {
	r.Logger.Info().Msg("Initiating graceful shutdown")

	if err := r.SendFinalStandbyStatusUpdate(); err != nil {
		r.Logger.Error().Err(err).Msg("Failed to send final standby status update")
	}

	if err := r.closeConnections(); err != nil {
		r.Logger.Error().Err(err).Msg("Failed to close connections")
	}

	r.Logger.Info().Msg("Graceful shutdown completed")
	return nil
}

// closeConnections closes all open database connections
func (r *BaseReplicator) closeConnections() error {
	if err := r.ReplicationConn.Close(context.Background()); err != nil {
		return fmt.Errorf("failed to close replication connection: %w", err)
	}
	if err := r.StandardConn.Close(context.Background()); err != nil {
		return fmt.Errorf("failed to close standard connection: %w", err)
	}
	return nil
}

// InitializePrimaryKeyInfo initializes primary key information for all tables
func (r *BaseReplicator) InitializePrimaryKeyInfo() error {
	for _, table := range r.Config.Tables {
		column, err := r.getPrimaryKeyColumn(r.Config.Schema, table)
		if err != nil {
			return err
		}
		r.TableDetails[table] = []string{column}
	}
	return nil
}

// getPrimaryKeyColumn retrieves the primary key column for a given table
func (r *BaseReplicator) getPrimaryKeyColumn(schema, table string) (string, error) {
	query := `
		SELECT pg_attribute.attname
		FROM pg_index, pg_class, pg_attribute, pg_namespace
		WHERE
			pg_class.oid = $1::regclass AND
			indrelid = pg_class.oid AND
			nspname = $2 AND
			pg_class.relnamespace = pg_namespace.oid AND
			pg_attribute.attrelid = pg_class.oid AND
			pg_attribute.attnum = any(pg_index.indkey) AND
			indisprimary
		LIMIT 1
	`
	var column string
	err := r.StandardConn.QueryRow(context.Background(), query, fmt.Sprintf("%s.%s", schema, table), schema).Scan(&column)
	if err != nil {
		return "", fmt.Errorf("failed to query primary key column: %v", err)
	}
	return column, nil
}

// SaveState saves the current replication state
func (r *BaseReplicator) SaveState(lsn pglogrepl.LSN) error {
	return r.NATSClient.SaveState(lsn)
}

// GetLastState retrieves the last saved replication state
func (r *BaseReplicator) GetLastState() (pglogrepl.LSN, error) {
	return r.NATSClient.GetLastState()
}
