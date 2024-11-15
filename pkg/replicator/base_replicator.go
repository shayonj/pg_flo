package replicator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"errors"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shayonj/pg_flo/pkg/utils"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05.000"})
	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000Z07:00"
}

// GeneratePublicationName generates a deterministic publication name based on the group name
func GeneratePublicationName(group string) string {
	group = strings.ReplaceAll(group, "-", "_")
	return fmt.Sprintf("pg_flo_%s_publication", group)
}

// BaseReplicator provides core functionality for PostgreSQL logical replication
type BaseReplicator struct {
	Config               Config
	ReplicationConn      ReplicationConnection
	StandardConn         StandardConnection
	Relations            map[uint32]*pglogrepl.RelationMessage
	Logger               zerolog.Logger
	TableDetails         map[string][]string
	LastLSN              pglogrepl.LSN
	NATSClient           NATSClient
	TableReplicationKeys map[string]utils.ReplicationKey
}

// NewBaseReplicator creates a new BaseReplicator instance
func NewBaseReplicator(config Config, replicationConn ReplicationConnection, standardConn StandardConnection, natsClient NATSClient) *BaseReplicator {
	if config.Schema == "" {
		config.Schema = "public"
	}

	logger := log.With().Str("component", "replicator").Logger()

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

	publicationName := GeneratePublicationName(config.Group)
	br.Logger.Info().
		Str("host", config.Host).
		Int("port", int(config.Port)).
		Str("database", config.Database).
		Str("user", config.User).
		Str("group", config.Group).
		Str("schema", config.Schema).
		Strs("tables", config.Tables).
		Str("publication", publicationName).
		Msg("Starting PostgreSQL logical replication stream")

	return br
}

// buildCreatePublicationQuery constructs the SQL query for creating a publication
func (r *BaseReplicator) buildCreatePublicationQuery() (string, error) {
	publicationName := GeneratePublicationName(r.Config.Group)

	tables, err := r.GetConfiguredTables(context.Background())
	if err != nil {
		return "", fmt.Errorf("failed to get configured tables: %w", err)
	}

	sanitizedTables := make([]string, len(tables))
	for i, table := range tables {
		parts := strings.Split(table, ".")
		sanitizedTables[i] = pgx.Identifier{parts[0], parts[1]}.Sanitize()
	}

	return fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		pgx.Identifier{publicationName}.Sanitize(),
		strings.Join(sanitizedTables, ", ")), nil
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

	query, err := r.buildCreatePublicationQuery()
	if err != nil {
		return fmt.Errorf("failed to build publication query: %w", err)
	}

	_, err = r.StandardConn.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	r.Logger.Info().Str("publication", publicationName).Msg("Publication created successfully")
	return nil
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
func (r *BaseReplicator) StartReplicationFromLSN(ctx context.Context, startLSN pglogrepl.LSN, stopChan <-chan struct{}) error {
	publicationName := GeneratePublicationName(r.Config.Group)
	r.Logger.Info().Str("startLSN", startLSN.String()).Str("publication", publicationName).Msg("Starting replication")

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

	return r.StreamChanges(ctx, stopChan)
}

// StreamChanges continuously processes replication messages
func (r *BaseReplicator) StreamChanges(ctx context.Context, stopChan <-chan struct{}) error {
	lastStatusUpdate := time.Now()
	standbyMessageTimeout := time.Second * 10

	for {
		select {
		case <-ctx.Done():
			r.Logger.Info().Msg("Stopping StreamChanges")
			return nil
		case <-stopChan:
			r.Logger.Info().Msg("Stop signal received, exiting StreamChanges")
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
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			r.Logger.Debug().Msg("Context canceled or deadline exceeded, stopping message processing")
			return nil
		}
		r.Logger.Error().Err(err).Msg("Error processing next message")
		return err
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		if err := r.handleCopyData(ctx, msg, lastStatusUpdate); err != nil {
			return err
		}
	case *pgproto3.CopyDone:
		r.Logger.Info().Msg("Received CopyDone message")
	case *pgproto3.ReadyForQuery:
		r.Logger.Info().Msg("Received ReadyForQuery message")
	case *pgproto3.ErrorResponse:
		r.Logger.Error().
			Str("severity", msg.Severity).
			Str("code", msg.Code).
			Str("message", msg.Message).
			Any("msg", msg).
			Msg("Received ErrorResponse")
	default:
		r.Logger.Warn().
			Str("type", fmt.Sprintf("%T", msg)).
			Msg("Received unexpected message type")
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
		return r.handleXLogData(msg.Data[1:], lastStatusUpdate)
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return r.handlePrimaryKeepaliveMessage(ctx, msg.Data[1:], lastStatusUpdate)
	default:
		r.Logger.Warn().Uint8("messageType", msg.Data[0]).Msg("Received unexpected CopyData message type")
	}
	return nil
}

// handleXLogData processes XLogData messages
func (r *BaseReplicator) handleXLogData(data []byte, lastStatusUpdate *time.Time) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse XLogData: %w", err)
	}

	if err := r.processWALData(xld.WALData, xld.WALStart); err != nil {
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
func (r *BaseReplicator) processWALData(walData []byte, lsn pglogrepl.LSN) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("failed to parse WAL data: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		r.handleRelationMessage(msg)
	case *pglogrepl.BeginMessage:
		return r.HandleBeginMessage(msg)
	case *pglogrepl.InsertMessage:
		return r.HandleInsertMessage(msg, lsn)
	case *pglogrepl.UpdateMessage:
		return r.HandleUpdateMessage(msg, lsn)
	case *pglogrepl.DeleteMessage:
		return r.HandleDeleteMessage(msg, lsn)
	case *pglogrepl.CommitMessage:
		return r.HandleCommitMessage(msg)
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
func (r *BaseReplicator) HandleBeginMessage(_ *pglogrepl.BeginMessage) error {
	return nil
}

// HandleInsertMessage handles InsertMessage messages
func (r *BaseReplicator) HandleInsertMessage(msg *pglogrepl.InsertMessage, lsn pglogrepl.LSN) error {
	relation, ok := r.Relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:      utils.OperationInsert,
		Schema:    relation.Namespace,
		Table:     relation.RelationName,
		Columns:   relation.Columns,
		EmittedAt: time.Now(),
		NewTuple:  msg.Tuple,
		LSN:       lsn.String(),
	}

	r.AddPrimaryKeyInfo(&cdcMessage, relation.RelationName)
	return r.PublishToNATS(cdcMessage)
}

// HandleUpdateMessage handles UpdateMessage messages
func (r *BaseReplicator) HandleUpdateMessage(msg *pglogrepl.UpdateMessage, lsn pglogrepl.LSN) error {
	relation, ok := r.Relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:           utils.OperationUpdate,
		Schema:         relation.Namespace,
		Table:          relation.RelationName,
		Columns:        relation.Columns,
		NewTuple:       msg.NewTuple,
		OldTuple:       msg.OldTuple,
		LSN:            lsn.String(),
		EmittedAt:      time.Now(),
		ToastedColumns: make(map[string]bool),
	}

	// Track toasted columns
	for i, col := range relation.Columns {
		if msg.NewTuple != nil {
			newVal := msg.NewTuple.Columns[i]
			cdcMessage.ToastedColumns[col.Name] = newVal.DataType == 'u'
		}
	}

	// Add replication key information
	r.AddPrimaryKeyInfo(&cdcMessage, relation.RelationName)

	// Ensure we have valid column types
	for i, col := range relation.Columns {
		if msg.NewTuple != nil && msg.NewTuple.Columns[i].Data != nil {
			// Ensure proper type information is preserved
			col.DataType = uint32(msg.NewTuple.Columns[i].DataType)
		}
	}

	return r.PublishToNATS(cdcMessage)
}

// HandleDeleteMessage handles DeleteMessage messages
func (r *BaseReplicator) HandleDeleteMessage(msg *pglogrepl.DeleteMessage, lsn pglogrepl.LSN) error {
	relation, ok := r.Relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:      utils.OperationDelete,
		Schema:    relation.Namespace,
		Table:     relation.RelationName,
		Columns:   relation.Columns,
		OldTuple:  msg.OldTuple,
		EmittedAt: time.Now(),
		LSN:       lsn.String(),
	}

	r.AddPrimaryKeyInfo(&cdcMessage, relation.RelationName)
	return r.PublishToNATS(cdcMessage)
}

// HandleCommitMessage processes a commit message and publishes it to NATS
func (r *BaseReplicator) HandleCommitMessage(msg *pglogrepl.CommitMessage) error {
	r.LastLSN = msg.CommitLSN

	if err := r.SaveState(msg.CommitLSN); err != nil {
		r.Logger.Error().Err(err).Msg("Failed to save replication state")
		return err
	}

	return nil
}

// PublishToNATS publishes a message to NATS
func (r *BaseReplicator) PublishToNATS(data utils.CDCMessage) error {
	binaryData, err := data.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	subject := fmt.Sprintf("pgflo.%s", r.Config.Group)
	err = r.NATSClient.PublishMessage(subject, binaryData)
	if err != nil {
		r.Logger.Error().
			Err(err).
			Str("subject", subject).
			Str("group", r.Config.Group).
			Msg("Failed to publish message to NATS")
		return err
	}
	return nil
}

// AddPrimaryKeyInfo adds replication key information to the CDCMessage
func (r *BaseReplicator) AddPrimaryKeyInfo(message *utils.CDCMessage, table string) {
	if key, ok := r.TableReplicationKeys[table]; ok {
		message.ReplicationKey = key
	} else {
		r.Logger.Error().
			Str("table", table).
			Msg("No replication key information found for table. This should not happen as validation is done during initialization")
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

// GracefulShutdown performs a graceful shutdown of the replicator
func (r *BaseReplicator) GracefulShutdown(ctx context.Context) error {
	r.Logger.Info().Msg("Initiating graceful shutdown")

	if err := r.SendStandbyStatusUpdate(ctx); err != nil {
		r.Logger.Warn().Err(err).Msg("Failed to send final standby status update")
	}

	if err := r.SaveState(r.LastLSN); err != nil {
		r.Logger.Warn().Err(err).Msg("Failed to save final state")
	}

	if err := r.closeConnections(ctx); err != nil {
		r.Logger.Warn().Err(err).Msg("Failed to close connections")
	}

	r.Logger.Info().Msg("Base replicator shutdown completed")
	return nil
}

// closeConnections closes all open database connections
func (r *BaseReplicator) closeConnections(ctx context.Context) error {
	r.Logger.Info().Msg("Closing database connections")

	if err := r.ReplicationConn.Close(ctx); err != nil {
		return fmt.Errorf("failed to close replication connection: %w", err)
	}
	if err := r.StandardConn.Close(ctx); err != nil {
		return fmt.Errorf("failed to close standard connection: %w", err)
	}
	return nil
}

// InitializePrimaryKeyInfo initializes primary key information for all tables
func (r *BaseReplicator) InitializePrimaryKeyInfo() error {
	query := `
		WITH table_info AS (
			SELECT
				t.tablename,
				c.relreplident,
				(
					SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum))
					FROM pg_index i
					JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
					WHERE i.indrelid = c.oid AND i.indisprimary
				) as pk_columns,
				(
					SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum))
					FROM pg_index i
					JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
					WHERE i.indrelid = c.oid AND i.indisunique AND NOT i.indisprimary
					LIMIT 1
				) as unique_columns
			FROM pg_tables t
			JOIN pg_class c ON t.tablename = c.relname
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE t.schemaname = $1
		)
		SELECT
			tablename,
			relreplident::text,
			COALESCE(pk_columns, ARRAY[]::text[]) as pk_columns,
			COALESCE(unique_columns, ARRAY[]::text[]) as unique_columns
		FROM table_info;
	`

	rows, err := r.StandardConn.Query(context.Background(), query, r.Config.Schema)
	if err != nil {
		return fmt.Errorf("failed to query replication key info: %v", err)
	}
	defer rows.Close()

	r.TableReplicationKeys = make(map[string]utils.ReplicationKey)

	for rows.Next() {
		var (
			tableName       string
			replicaIdentity string
			pkColumns       []string
			uniqueColumns   []string
		)

		if err := rows.Scan(&tableName, &replicaIdentity, &pkColumns, &uniqueColumns); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		key := utils.ReplicationKey{}

		switch {
		case len(pkColumns) > 0:
			key = utils.ReplicationKey{
				Type:    utils.ReplicationKeyPK,
				Columns: pkColumns,
			}
		case len(uniqueColumns) > 0:
			key = utils.ReplicationKey{
				Type:    utils.ReplicationKeyUnique,
				Columns: uniqueColumns,
			}
		case replicaIdentity == "f":
			key = utils.ReplicationKey{
				Type:    utils.ReplicationKeyFull,
				Columns: nil,
			}
		}

		if err := r.validateTableReplicationKey(tableName, key); err != nil {
			r.Logger.Warn().
				Str("table", tableName).
				Str("replica_identity", replicaIdentity).
				Str("key_type", string(key.Type)).
				Strs("columns", key.Columns).
				Err(err).
				Msg("Invalid replication key configuration")
			continue
		}

		r.TableReplicationKeys[tableName] = key

		r.Logger.Debug().
			Str("table", tableName).
			Str("key_type", string(key.Type)).
			Strs("columns", key.Columns).
			Str("replica_identity", replicaIdentity).
			Msg("Initialized replication key configuration")
	}

	return rows.Err()
}

// SaveState saves the current replication state
func (r *BaseReplicator) SaveState(lsn pglogrepl.LSN) error {
	state, err := r.NATSClient.GetState()
	if err != nil {
		return fmt.Errorf("failed to get current state: %w", err)
	}
	state.LSN = lsn
	return r.NATSClient.SaveState(state)
}

// GetLastState retrieves the last saved replication state
func (r *BaseReplicator) GetLastState() (pglogrepl.LSN, error) {
	state, err := r.NATSClient.GetState()
	if err != nil {
		return 0, fmt.Errorf("failed to get state: %w", err)
	}
	return state.LSN, nil
}

// CheckReplicationSlotStatus checks the status of the replication slot
func (r *BaseReplicator) CheckReplicationSlotStatus(ctx context.Context) error {
	publicationName := GeneratePublicationName(r.Config.Group)
	var restartLSN string
	err := r.StandardConn.QueryRow(ctx,
		"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = $1",
		publicationName).Scan(&restartLSN)
	if err != nil {
		return fmt.Errorf("failed to query replication slot status: %w", err)
	}
	r.Logger.Info().Str("slotName", publicationName).Str("restartLSN", restartLSN).Msg("Replication slot status")
	return nil
}

// GetConfiguredTables returns all tables based on configuration
// If no specific tables are configured, returns all tables from the configured schema
func (r *BaseReplicator) GetConfiguredTables(ctx context.Context) ([]string, error) {
	if len(r.Config.Tables) > 0 {
		fullyQualifiedTables := make([]string, len(r.Config.Tables))
		for i, table := range r.Config.Tables {
			fullyQualifiedTables[i] = fmt.Sprintf("%s.%s", r.Config.Schema, table)
		}
		return fullyQualifiedTables, nil
	}

	rows, err := r.StandardConn.Query(ctx, `
		SELECT schemaname || '.' || tablename
		FROM pg_tables
		WHERE schemaname = $1
		AND schemaname NOT IN ('pg_catalog', 'information_schema', 'internal_pg_flo')
	`, r.Config.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

func (r *BaseReplicator) validateTableReplicationKey(tableName string, key utils.ReplicationKey) error {
	if !key.IsValid() {
		return fmt.Errorf(
			"table %q requires one of the following:\n"+
				"\t1. A PRIMARY KEY constraint\n"+
				"\t2. A UNIQUE constraint\n"+
				"\t3. REPLICA IDENTITY FULL (ALTER TABLE %s REPLICA IDENTITY FULL)",
			tableName, tableName)
	}
	return nil
}
