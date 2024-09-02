package replicator

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"errors"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/sinks"
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
	Sink            sinks.Sink
	Relations       map[uint32]*pglogrepl.RelationMessage
	Logger          zerolog.Logger
	TableDetails    map[string][]string
	Buffer          *Buffer
	LastLSN         pglogrepl.LSN
	RuleEngine      *rules.RuleEngine
}

// NewBaseReplicator creates a new BaseReplicator instance
func NewBaseReplicator(config Config, sink sinks.Sink, replicationConn ReplicationConnection, StandardConn StandardConnection, ruleEngine *rules.RuleEngine) *BaseReplicator {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	br := &BaseReplicator{
		Config:          config,
		ReplicationConn: replicationConn,
		StandardConn:    StandardConn,
		Sink:            sink,
		Relations:       make(map[uint32]*pglogrepl.RelationMessage),
		Logger:          logger,
		TableDetails:    make(map[string][]string),
		Buffer:          NewBuffer(1000, 5*time.Second), // 1000 rows or 5 seconds
		RuleEngine:      ruleEngine,
	}

	// Initialize the OID map with custom types from the database
	if err := InitializeOIDMap(context.Background(), StandardConn); err != nil {
		br.Logger.Error().Err(err).Msg("Failed to initialize OID map")
	}

	if err := br.InitializePrimaryKeyInfo(); err != nil {
		br.Logger.Error().Err(err).Msg("Failed to initialize primary key info")
	}

	br.StartPeriodicFlush()

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

	if err := r.processWALData(xld.WALData); err != nil {
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
func (r *BaseReplicator) processWALData(walData []byte) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("failed to parse WAL data: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		r.handleRelationMessage(msg)
	case *pglogrepl.BeginMessage:
		r.HandleBeginMessage(msg)
		return nil
	case *pglogrepl.InsertMessage:
		return r.HandleInsertMessage(msg)
	case *pglogrepl.UpdateMessage:
		return r.HandleUpdateMessage(msg)
	case *pglogrepl.DeleteMessage:
		return r.HandleDeleteMessage(msg)
	case *pglogrepl.CommitMessage:
		r.HandleCommitMessage(msg)
		return nil
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
func (r *BaseReplicator) HandleBeginMessage(msg *pglogrepl.BeginMessage) {
	// If this we needed for sinks, we can write to buffer
	// beginJSON, err := MarshalJSON(map[string]interface{}{
	// 	"type":             "BEGIN",
	// 	"final_lsn":        msg.FinalLSN,
	// 	"commit_timestamp": msg.CommitTime,
	// })

	r.Logger.Debug().Str("final_lsn", msg.FinalLSN.String()).Msg("Received BEGIN")
}

// HandleInsertMessage handles InsertMessage messages
func (r *BaseReplicator) HandleInsertMessage(msg *pglogrepl.InsertMessage) error {
	relation, ok := r.Relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	newRow := r.ConvertTupleDataToMap(relation, msg.Tuple)

	// Apply rules
	if r.RuleEngine != nil {
		var err error
		newRow, err = r.RuleEngine.ApplyRules(relation.RelationName, newRow, rules.OperationInsert)
		if err != nil {
			r.Logger.Error().Err(err).Msg("Failed to apply rules")
			return fmt.Errorf("failed to apply rules: %w", err)
		}
		if newRow == nil {
			// Row filtered out by rules
			return nil
		}
	}

	insertData := map[string]interface{}{
		"type":    "INSERT",
		"schema":  relation.Namespace,
		"table":   relation.RelationName,
		"new_row": newRow,
	}
	r.addPrimaryKeyInfo(insertData, relation.RelationName)
	if err := r.bufferWrite(insertData); err != nil {
		return fmt.Errorf("failed to buffer write: %w", err)
	}
	return nil
}

// HandleUpdateMessage handles UpdateMessage messages
func (r *BaseReplicator) HandleUpdateMessage(msg *pglogrepl.UpdateMessage) error {
	relation, ok := r.Relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}
	var oldRow, newRow = map[string]utils.CDCValue{}, map[string]utils.CDCValue{}

	if msg.OldTuple != nil {
		oldRow = r.ConvertTupleDataToMap(relation, msg.OldTuple)
	}

	if msg.NewTuple != nil {
		newRow = r.ConvertTupleDataToMap(relation, msg.NewTuple)
	}

	// Apply rules only for new row
	if r.RuleEngine != nil {
		var err error
		newRow, err = r.RuleEngine.ApplyRules(relation.RelationName, newRow, rules.OperationUpdate)
		if err != nil {
			r.Logger.Error().Err(err).Msg("Failed to apply rules")
			return fmt.Errorf("failed to apply rules: %w", err)
		}
		if newRow == nil {
			// Row filtered out by rules
			return nil
		}
	}

	updateData := map[string]interface{}{
		"type":    "UPDATE",
		"schema":  relation.Namespace,
		"table":   relation.RelationName,
		"old_row": oldRow,
		"new_row": newRow,
	}
	r.addPrimaryKeyInfo(updateData, relation.RelationName)
	if err := r.bufferWrite(updateData); err != nil {
		return fmt.Errorf("failed to buffer write: %w", err)
	}
	return nil
}

// HandleDeleteMessage handles DeleteMessage messages
func (r *BaseReplicator) HandleDeleteMessage(msg *pglogrepl.DeleteMessage) error {
	relation, ok := r.Relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	oldRow := r.ConvertTupleDataToMap(relation, msg.OldTuple)

	// Apply rules
	if r.RuleEngine != nil {
		var err error
		oldRow, err = r.RuleEngine.ApplyRules(relation.RelationName, oldRow, rules.OperationDelete)
		if err != nil {
			r.Logger.Error().Err(err).Msg("Failed to apply rules")
			return fmt.Errorf("failed to apply rules: %w", err)
		}
		if oldRow == nil {
			// Row filtered out by rules
			return nil
		}
	}

	deleteData := map[string]interface{}{
		"type":    "DELETE",
		"schema":  relation.Namespace,
		"table":   relation.RelationName,
		"old_row": oldRow,
	}
	r.addPrimaryKeyInfo(deleteData, relation.RelationName)
	if err := r.bufferWrite(deleteData); err != nil {
		return fmt.Errorf("failed to buffer write: %w", err)
	}
	return nil
}

// HandleCommitMessage processes a commit message and writes it to the buffer
func (r *BaseReplicator) HandleCommitMessage(msg *pglogrepl.CommitMessage) {
	// If this we needed for sinks, we can write to buffer
	// commitJSON, err := MarshalJSON(map[string]interface{}{
	// 	"type":             "COMMIT",
	// 	"lsn":              msg.CommitLSN,
	// 	"end_lsn":          msg.TransactionEndLSN,
	// 	"commit_timestamp": msg.CommitTime,
	// })

	r.Logger.Debug().Str("lsn", msg.CommitLSN.String()).Str("transaction_end_lsn", msg.TransactionEndLSN.String()).Msg("Received COMMIT")
	r.LastLSN = msg.CommitLSN
}

// bufferWrite adds data to the buffer and flushes if necessary
func (r *BaseReplicator) bufferWrite(data interface{}) error {
	shouldFlush := r.Buffer.Add(data)
	if shouldFlush {
		return r.FlushBuffer()
	}
	return nil
}

// FlushBuffer writes the buffered data to the sink
func (r *BaseReplicator) FlushBuffer() error {
	data := r.Buffer.Flush()
	if len(data) == 0 {
		return nil
	}

	if err := r.Sink.WriteBatch(data); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	if err := r.Sink.SetLastLSN(r.LastLSN); err != nil {
		return fmt.Errorf("failed to update last LSN: %w", err)
	}

	return nil
}

// ConvertTupleDataToMap converts tuple data to a map
func (r *BaseReplicator) ConvertTupleDataToMap(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]utils.CDCValue {
	result := make(map[string]utils.CDCValue)
	for i, col := range tuple.Columns {
		columnName := relation.Columns[i].Name
		dataType := relation.Columns[i].DataType

		if col.Data == nil {
			result[columnName] = utils.CDCValue{Type: dataType, Value: nil}
			continue
		}

		value := r.convertColumnValue(dataType, col.Data)
		result[columnName] = utils.CDCValue{Type: dataType, Value: value}
	}
	return result
}

// convertColumnValue converts the column value based on its data type
func (r *BaseReplicator) convertColumnValue(dataType uint32, data []byte) interface{} {
	switch dataType {
	case uint32(pgtype.Int2OID), uint32(pgtype.Int4OID), uint32(pgtype.Int8OID):
		val, _ := strconv.ParseInt(string(data), 10, 64)
		return val
	case uint32(pgtype.Float4OID), uint32(pgtype.Float8OID), uint32(pgtype.NumericOID):
		return string(data)
	case uint32(pgtype.BoolOID):
		return string(data)
	case uint32(pgtype.ByteaOID):
		return base64.StdEncoding.EncodeToString(data)
	case uint32(pgtype.TimestampOID), uint32(pgtype.TimestamptzOID):
		t, err := utils.ParseTimestamp(string(data))
		if err != nil {
			r.Logger.Warn().Err(err).Msgf("Failed to parse timestamp: %s", string(data))
			return string(data)
		}
		return t.Format(time.RFC3339Nano)
	default:
		return string(data)
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

// sendFinalStandbyStatusUpdate sends a final status update before shutting down
func (r *BaseReplicator) sendFinalStandbyStatusUpdate() error {
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
	exists, err := r.checkReplicationSlotExists(publicationName)
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

// checkReplicationSlotExists checks if a slot with the given name already exists
func (r *BaseReplicator) checkReplicationSlotExists(slotName string) (bool, error) {
	var exists bool
	err := r.StandardConn.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking replication slot: %w", err)
	}
	return exists, nil
}

func (r *BaseReplicator) gracefulShutdown() error {
	r.Logger.Info().Msg("Initiating graceful shutdown")

	if err := r.FlushBuffer(); err != nil {
		r.Logger.Error().Err(err).Msg("Failed to flush buffer during shutdown")
	}

	if err := r.sendFinalStandbyStatusUpdate(); err != nil {
		r.Logger.Error().Err(err).Msg("Failed to send final standby status update")
	}

	if err := r.closeConnections(); err != nil {
		r.Logger.Error().Err(err).Msg("Failed to close connections")
	}

	r.Logger.Info().Msg("Graceful shutdown completed")
	return nil
}

// closeConnections closes all open database connections.
func (r *BaseReplicator) closeConnections() error {
	if err := r.ReplicationConn.Close(context.Background()); err != nil {
		return fmt.Errorf("failed to close replication connection: %w", err)
	}
	if err := r.StandardConn.Close(context.Background()); err != nil {
		return fmt.Errorf("failed to close standard connection: %w", err)
	}
	return nil
}

// StartPeriodicFlush ensures any non flushed buffer is flushed every flushTimeout.
func (r *BaseReplicator) StartPeriodicFlush() {
	ticker := time.NewTicker(r.Buffer.flushTimeout)
	go func() {
		for range ticker.C {
			if r.Buffer.shouldFlush() {
				if err := r.FlushBuffer(); err != nil {
					r.Logger.Err(err)
				}
			}
		}
	}()
}

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

func (r *BaseReplicator) addPrimaryKeyInfo(event map[string]interface{}, table string) {
	if pkColumns, ok := r.TableDetails[table]; ok && len(pkColumns) > 0 {
		event["primary_key_column"] = pkColumns[0]
	}
}
