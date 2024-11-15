package sinks

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shayonj/pg_flo/pkg/utils"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05.000",
	})
}

// PostgresSink represents a sink for PostgreSQL database
type PostgresSink struct {
	conn                    *pgx.Conn
	disableForeignKeyChecks bool
	connConfig              *pgx.ConnConfig
	retryConfig             utils.RetryConfig
}

// NewPostgresSink creates a new PostgresSink instance
func NewPostgresSink(targetHost string, targetPort int, targetDBName, targetUser, targetPassword string, syncSchema bool, sourceHost string, sourcePort int, sourceDBName, sourceUser, sourcePassword string, disableForeignKeyChecks bool) (*PostgresSink, error) {
	connConfig, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s", targetHost, targetPort, targetDBName, targetUser, targetPassword))
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection config: %v", err)
	}

	sink := &PostgresSink{
		connConfig:              connConfig,
		disableForeignKeyChecks: disableForeignKeyChecks,
		retryConfig: utils.RetryConfig{
			MaxAttempts: 5,
			InitialWait: 1 * time.Second,
			MaxWait:     30 * time.Second,
		},
	}

	if err := sink.connect(context.Background()); err != nil {
		return nil, err
	}

	if syncSchema {
		if err := sink.syncSchema(sourceHost, sourcePort, sourceDBName, sourceUser, sourcePassword); err != nil {
			return nil, err
		}
	}

	return sink, nil
}

// New method to handle connection
func (s *PostgresSink) connect(ctx context.Context) error {
	var connMutex sync.Mutex

	return utils.WithRetry(ctx, s.retryConfig, func() error {
		conn, err := pgx.ConnectConfig(ctx, s.connConfig)
		if err != nil {
			log.Error().Err(err).Msg("Failed to connect to database, will retry")
			return err
		}
		connMutex.Lock()
		s.conn = conn
		connMutex.Unlock()
		return nil
	})
}

// syncSchema synchronizes the schema between source and target databases
func (s *PostgresSink) syncSchema(sourceHost string, sourcePort int, sourceDBName, sourceUser, sourcePassword string) error {
	dumpCmd := exec.Command("pg_dump", "--schema-only")
	dumpCmd.Env = append(os.Environ(),
		fmt.Sprintf("PGHOST=%s", sourceHost),
		fmt.Sprintf("PGPORT=%d", sourcePort),
		fmt.Sprintf("PGDATABASE=%s", sourceDBName),
		fmt.Sprintf("PGUSER=%s", sourceUser),
		fmt.Sprintf("PGPASSWORD=%s", sourcePassword),
	)
	schemaDump, err := dumpCmd.Output()
	if err != nil {
		log.Error().Err(err).Msg("Failed to dump schema from source database")
		return fmt.Errorf("failed to dump schema from source database: %v", err)
	}

	applyCmd := exec.Command("psql")
	applyCmd.Env = append(os.Environ(),
		fmt.Sprintf("PGHOST=%s", s.conn.Config().Host),
		fmt.Sprintf("PGPORT=%d", s.conn.Config().Port),
		fmt.Sprintf("PGDATABASE=%s", s.conn.Config().Database),
		fmt.Sprintf("PGUSER=%s", s.conn.Config().User),
		fmt.Sprintf("PGPASSWORD=%s", s.conn.Config().Password),
	)
	applyCmd.Stdin = strings.NewReader(string(schemaDump))

	output, err := applyCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to apply schema to target database: %v\nOutput: %s", err, string(output))
	}

	return nil
}

// handleInsert processes an insert operation
func (s *PostgresSink) handleInsert(tx pgx.Tx, message *utils.CDCMessage) error {
	columns := make([]string, 0, len(message.Columns))
	placeholders := make([]string, 0, len(message.Columns))
	values := make([]interface{}, 0, len(message.Columns))
	paramCount := 1

	for _, col := range message.Columns {
		if val, ok := message.NewValues[col.Name]; ok {
			columns = append(columns, col.Name)
			placeholders = append(placeholders, fmt.Sprintf("$%d", paramCount))
			values = append(values, val.Get())
			paramCount++
		}
	}

	if len(columns) == 0 {
		return fmt.Errorf("no columns to insert")
	}

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		message.Schema,
		message.Table,
		strings.Join(columns, ","),
		strings.Join(placeholders, ","),
	)

	if message.PrimaryKeyColumn != "" {
		query += fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", message.PrimaryKeyColumn)
	}

	_, err := tx.Exec(context.Background(), query, values...)
	return err
}

// handleUpdate processes an update operation
func (s *PostgresSink) handleUpdate(tx pgx.Tx, message *utils.CDCMessage) error {
	if len(message.NewValues) == 0 {
		return fmt.Errorf("no values to update")
	}

	setColumns := make([]string, 0, len(message.NewValues))
	values := make([]interface{}, 0, len(message.NewValues))
	paramCount := 1

	for colName, val := range message.NewValues {
		if message.GetColumnIndex(colName) == -1 {
			return fmt.Errorf("column %s not found in message columns", colName)
		}
		if !val.IsNull() {
			setColumns = append(setColumns, fmt.Sprintf("%s = $%d", colName, paramCount))
			values = append(values, val.Get())
			paramCount++
		}
	}

	whereClause, whereValues := s.buildWhereClause(message, paramCount)
	values = append(values, whereValues...)

	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		message.Schema,
		message.Table,
		strings.Join(setColumns, ", "),
		whereClause,
	)

	_, err := tx.Exec(context.Background(), query, values...)
	return err
}

// buildWhereClause builds the WHERE clause for the update operation
func (s *PostgresSink) buildWhereClause(message *utils.CDCMessage, startParam int) (string, []interface{}) {
	var conditions []string
	var values []interface{}
	paramCount := startParam

	if message.PrimaryKeyColumn != "" {
		if message.GetColumnIndex(message.PrimaryKeyColumn) == -1 {
			return "false", nil // Invalid primary key column
		}
		if oldVal, ok := message.OldValues[message.PrimaryKeyColumn]; ok {
			conditions = append(conditions, fmt.Sprintf("%s = $%d", message.PrimaryKeyColumn, paramCount))
			values = append(values, oldVal.Get())
			paramCount++
		}
	} else {
		for colName, val := range message.OldValues {
			if message.GetColumnIndex(colName) == -1 {
				continue // Skip invalid columns
			}
			if !val.IsNull() {
				conditions = append(conditions, fmt.Sprintf("%s = $%d", colName, paramCount))
				values = append(values, val.Get())
				paramCount++
			}
		}
	}

	if len(conditions) == 0 {
		return "true", nil
	}

	return strings.Join(conditions, " AND "), values
}

// handleDelete processes a delete operation
func (s *PostgresSink) handleDelete(tx pgx.Tx, message *utils.CDCMessage) error {
	if message.PrimaryKeyColumn == "" {
		return fmt.Errorf("primary key column not specified in the message")
	}

	pkValue, err := message.GetDecodedColumnValue(message.PrimaryKeyColumn)
	if err != nil {
		return fmt.Errorf("failed to get primary key value: %v", err)
	}

	colIndex := message.GetColumnIndex(message.PrimaryKeyColumn)
	if colIndex == -1 {
		return fmt.Errorf("primary key column %s not found in columns", message.PrimaryKeyColumn)
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s = $1",
		message.Schema, message.Table, message.PrimaryKeyColumn)

	_, err = tx.Exec(context.Background(), query, pkValue)
	return err
}

// handleDDL processes a DDL operation
func (s *PostgresSink) handleDDL(tx pgx.Tx, message *utils.CDCMessage) (pgx.Tx, error) {
	if s.conn == nil || s.conn.IsClosed() {
		if err := s.connect(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to reconnect to database: %v", err)
		}
		newTx, err := s.conn.Begin(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to begin new transaction: %v", err)
		}
		tx = newTx
	}

	ddlCommand, err := message.GetDecodedColumnValue("ddl_command")
	if err != nil {
		return tx, fmt.Errorf("failed to get DDL command: %v", err)
	}

	ddlString, ok := ddlCommand.(string)
	if !ok {
		return tx, fmt.Errorf("DDL command is not a string, got %T", ddlCommand)
	}

	log.Debug().Msgf("Executing DDL: %s", ddlString)

	if strings.Contains(strings.ToUpper(ddlString), "CONCURRENTLY") {
		if err := tx.Commit(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to commit transaction before concurrent DDL: %v", err)
		}

		if _, err := s.conn.Exec(context.Background(), ddlString); err != nil {
			if strings.Contains(err.Error(), "does not exist") {
				log.Warn().Msgf("Ignoring DDL for non-existent object: %s", ddlString)
				return s.conn.Begin(context.Background())
			}
			return nil, fmt.Errorf("failed to execute concurrent DDL: %v", err)
		}

		return s.conn.Begin(context.Background())
	}

	_, err = tx.Exec(context.Background(), ddlString)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			log.Warn().Msgf("Ignoring DDL for non-existent object: %s", ddlString)
			return tx, nil
		}
		return tx, fmt.Errorf("failed to execute DDL: %v", err)
	}

	return tx, nil
}

// disableForeignKeys disables foreign key checks
func (s *PostgresSink) disableForeignKeys(ctx context.Context) error {
	_, err := s.conn.Exec(ctx, "SET session_replication_role = 'replica';")
	return err
}

// enableForeignKeys enables foreign key checks
func (s *PostgresSink) enableForeignKeys(ctx context.Context) error {
	_, err := s.conn.Exec(ctx, "SET session_replication_role = 'origin';")
	return err
}

// WriteBatch writes a batch of CDC messages to the target database
func (s *PostgresSink) WriteBatch(messages []*utils.CDCMessage) error {
	ctx := context.Background()

	if s.conn == nil || s.conn.IsClosed() {
		if err := s.connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to database: %v", err)
		}
	}

	return s.writeBatchInternal(ctx, messages)
}

// New helper method to handle batch writing
func (s *PostgresSink) writeBatchInternal(ctx context.Context, messages []*utils.CDCMessage) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	defer func() {
		if tx != nil {
			if err := tx.Rollback(ctx); err != nil {
				log.Error().Err(err).Msg("failed to rollback transaction")
			}
		}
	}()

	if s.disableForeignKeyChecks {
		if err := s.disableForeignKeys(ctx); err != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				log.Error().Err(rollbackErr).Msg("failed to rollback transaction")
			}
			return fmt.Errorf("failed to disable foreign key checks: %v", err)
		}
		defer func() {
			if err := s.enableForeignKeys(ctx); err != nil {
				log.Error().Err(err).Msg("failed to re-enable foreign key checks")
			}
		}()
	}

	for _, message := range messages {
		primaryKeyColumn := message.MappedPrimaryKeyColumn
		if primaryKeyColumn != "" {
			message.PrimaryKeyColumn = message.MappedPrimaryKeyColumn
		}

		var operationErr error
		err := utils.WithRetry(ctx, s.retryConfig, func() error {
			if s.conn == nil || s.conn.IsClosed() {
				if err := s.connect(ctx); err != nil {
					return fmt.Errorf("failed to reconnect to database: %v", err)
				}
				// Start a new transaction if needed
				if tx == nil {
					newTx, err := s.conn.Begin(ctx)
					if err != nil {
						return fmt.Errorf("failed to begin new transaction: %v", err)
					}
					tx = newTx
				}
			}

			switch message.Type {
			case utils.OperationInsert:
				operationErr = s.handleInsert(tx, message)
			case utils.OperationUpdate:
				operationErr = s.handleUpdate(tx, message)
			case utils.OperationDelete:
				operationErr = s.handleDelete(tx, message)
			case utils.OperationDDL:
				var newTx pgx.Tx
				newTx, operationErr = s.handleDDL(tx, message)
				tx = newTx
			default:
				operationErr = fmt.Errorf("unknown event type: %s", message.Type)
			}

			if operationErr != nil && isConnectionError(operationErr) {
				return operationErr // Retry on connection errors
			}
			return nil
		})

		if err != nil || operationErr != nil {
			if tx != nil {
				if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
					log.Error().Err(rollbackErr).Msg("failed to rollback transaction")
				}
			}
			tx = nil
			return fmt.Errorf("failed to handle %s: %v-%v", message.Type, err, operationErr)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	tx = nil // Prevent deferred rollback
	return nil
}

// New helper function to determine if an error is a connection issue
func isConnectionError(err error) bool {
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	if err == pgx.ErrTxClosed {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// Check for specific PostgreSQL error codes related to connection issues
		switch pgErr.Code {
		case "08006", // connection_failure
			"08003", // connection_does_not_exist
			"57P01", // admin_shutdown
			"57P02", // crash_shutdown
			"57P03": // cannot_connect_now
			return true
		}
	}
	return false
}

// Close closes the database connection
func (s *PostgresSink) Close() error {
	return s.conn.Close(context.Background())
}
