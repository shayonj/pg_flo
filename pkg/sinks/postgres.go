package sinks

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"github.com/shayonj/pg_flo/pkg/utils"
)

// PostgresSink represents a sink for PostgreSQL database
type PostgresSink struct {
	conn                    *pgx.Conn
	disableForeignKeyChecks bool
}

// NewPostgresSink creates a new PostgresSink instance
func NewPostgresSink(targetHost string, targetPort int, targetDBName, targetUser, targetPassword string, syncSchema bool, sourceHost string, sourcePort int, sourceDBName, sourceUser, sourcePassword string, disableForeignKeyChecks bool) (*PostgresSink, error) {
	connConfig, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s", targetHost, targetPort, targetDBName, targetUser, targetPassword))
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection config: %v", err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to target database: %v", err)
	}

	sink := &PostgresSink{
		conn:                    conn,
		disableForeignKeyChecks: disableForeignKeyChecks,
	}

	if syncSchema {
		if err := sink.syncSchema(sourceHost, sourcePort, sourceDBName, sourceUser, sourcePassword); err != nil {
			return nil, err
		}
	}

	return sink, nil
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

	for i, col := range message.Columns {
		columns = append(columns, col.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		value, err := message.GetDecodedColumnValue(col.Name)
		if err != nil {
			return fmt.Errorf("failed to get column value: %v", err)
		}
		values = append(values, value)
	}

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		message.Schema, message.Table, strings.Join(columns, ","), strings.Join(placeholders, ","))

	_, err := tx.Exec(context.Background(), query, values...)
	return err
}

// handleUpdate processes an update operation
func (s *PostgresSink) handleUpdate(tx pgx.Tx, message *utils.CDCMessage) error {
	setClauses := make([]string, 0, len(message.Columns))
	values := make([]interface{}, 0, len(message.Columns))
	whereConditions := make([]string, 0)

	for i, col := range message.Columns {
		value, err := message.GetColumnValue(col.Name)
		if err != nil {
			return fmt.Errorf("failed to get column value: %v", err)
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col.Name, i+1))
		values = append(values, value)

		if col.Name == message.PrimaryKeyColumn {
			whereConditions = append(whereConditions, fmt.Sprintf("%s = $%d", col.Name, i+1))
		}
	}

	if len(whereConditions) == 0 {
		return fmt.Errorf("primary key column not found in the message")
	}

	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		message.Schema, message.Table, strings.Join(setClauses, ", "), strings.Join(whereConditions, " AND "))

	_, err := tx.Exec(context.Background(), query, values...)
	return err
}

// handleDelete processes a delete operation
func (s *PostgresSink) handleDelete(tx pgx.Tx, message *utils.CDCMessage) error {
	if message.PrimaryKeyColumn == "" {
		return fmt.Errorf("primary key column not specified in the message")
	}

	pkValue, err := message.GetColumnValue(message.PrimaryKeyColumn)
	if err != nil {
		return fmt.Errorf("failed to get primary key value: %v", err)
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s = $1",
		message.Schema, message.Table, message.PrimaryKeyColumn)

	_, err = tx.Exec(context.Background(), query, pkValue)
	return err
}

// handleDDL processes a DDL operation
func (s *PostgresSink) handleDDL(tx pgx.Tx, message *utils.CDCMessage) error {
	ddlCommand, err := message.GetColumnValue("command")
	if err != nil {
		return fmt.Errorf("failed to get DDL command: %v", err)
	}

	ddlString, ok := ddlCommand.(string)
	if !ok {
		return fmt.Errorf("DDL command is not a string")
	}

	_, err = tx.Exec(context.Background(), ddlString)
	return err
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
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			log.Error().Err(err).Msg("failed to rollback transaction")
		}
	}()

	if s.disableForeignKeyChecks {
		if err := s.disableForeignKeys(ctx); err != nil {
			return fmt.Errorf("failed to disable foreign key checks: %v", err)
		}
		defer func() {
			if err := s.enableForeignKeys(ctx); err != nil {
				log.Error().Err(err).Msg("failed to re-enable foreign key checks")
			}
		}()
	}

	for _, message := range messages {
		var err error
		switch message.Type {
		case "INSERT":
			err = s.handleInsert(tx, message)
		case "UPDATE":
			err = s.handleUpdate(tx, message)
		case "DELETE":
			err = s.handleDelete(tx, message)
		case "DDL":
			err = s.handleDDL(tx, message)
		default:
			return fmt.Errorf("unknown event type: %s", message.Type)
		}

		if err != nil {
			return fmt.Errorf("failed to handle %s: %v", message.Type, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// Close closes the database connection
func (s *PostgresSink) Close() error {
	return s.conn.Close(context.Background())
}
