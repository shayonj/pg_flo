package sinks

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"github.com/shayonj/pg_flo/pkg/utils"
)

// PostgresSink represents a sink for PostgreSQL database
type PostgresSink struct {
	conn        *pgx.Conn
	lastLSN     pglogrepl.LSN
	statusTable string
}

// NewPostgresSink creates a new PostgresSink instance
func NewPostgresSink(targetHost string, targetPort int, targetDBName, targetUser, targetPassword string, syncSchema bool, sourceHost string, sourcePort int, sourceDBName, sourceUser, sourcePassword string) (*PostgresSink, error) {
	connConfig, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s", targetHost, targetPort, targetDBName, targetUser, targetPassword))
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection config: %v", err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to target database: %v", err)
	}

	sink := &PostgresSink{
		conn:        conn,
		statusTable: "internal_pg_flo.lsn_status",
	}

	if syncSchema {
		if err := sink.syncSchema(sourceHost, sourcePort, sourceDBName, sourceUser, sourcePassword); err != nil {
			return nil, err
		}
	}

	if err := sink.setupStatusTable(); err != nil {
		return nil, err
	}

	if err := sink.loadStatus(); err != nil {
		log.Warn().Err(err).Msg("Failed to load status, starting from scratch")
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

// setupStatusTable creates the status table if it doesn't exist
func (s *PostgresSink) setupStatusTable() error {
	_, err := s.conn.Exec(context.Background(), `
		CREATE SCHEMA IF NOT EXISTS internal_pg_flo;
		CREATE TABLE IF NOT EXISTS internal_pg_flo.lsn_status (
			id SERIAL PRIMARY KEY,
			last_lsn TEXT NOT NULL
		);
	`)
	return err
}

// loadStatus loads the last known LSN from the status table
func (s *PostgresSink) loadStatus() error {
	var lastLSN string
	err := s.conn.QueryRow(context.Background(), "SELECT last_lsn FROM internal_pg_flo.lsn_status ORDER BY id DESC LIMIT 1").Scan(&lastLSN)
	if err != nil {
		return fmt.Errorf("failed to load last LSN: %v", err)
	}

	lsn, err := pglogrepl.ParseLSN(lastLSN)
	if err != nil {
		return fmt.Errorf("failed to parse last LSN: %v", err)
	}

	s.lastLSN = lsn
	return nil
}

// saveStatus saves the current LSN to the status table
func (s *PostgresSink) saveStatus() error {
	_, err := s.conn.Exec(context.Background(), "INSERT INTO internal_pg_flo.lsn_status (last_lsn) VALUES ($1)", s.lastLSN.String())
	return err
}

// handleInsert processes an insert operation
func (s *PostgresSink) handleInsert(tx pgx.Tx, message *utils.CDCMessage) error {
	columns := make([]string, 0, len(message.Columns))
	placeholders := make([]string, 0, len(message.Columns))
	values := make([]interface{}, 0, len(message.Columns))

	for i, col := range message.Columns {
		columns = append(columns, col.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		value, err := message.GetColumnValue(col.Name)
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

// WriteBatch writes a batch of CDC messages to the target database
func (s *PostgresSink) WriteBatch(data []interface{}) error {
	tx, err := s.conn.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(context.Background())

	for _, item := range data {
		message, ok := item.(*utils.CDCMessage)
		if !ok {
			return fmt.Errorf("failed to cast item to *utils.CDCMessage")
		}

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

	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// GetLastLSN returns the last processed LSN
func (s *PostgresSink) GetLastLSN() (pglogrepl.LSN, error) {
	return s.lastLSN, nil
}

// SetLastLSN sets the last processed LSN and saves it to the status table
func (s *PostgresSink) SetLastLSN(lsn pglogrepl.LSN) error {
	s.lastLSN = lsn
	return s.saveStatus()
}

// Close closes the database connection
func (s *PostgresSink) Close() error {
	return s.conn.Close(context.Background())
}
