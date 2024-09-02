package sinks

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"
	"github.com/shayonj/pg_flo/pkg/utils"
)

type PostgresSink struct {
	conn        *pgx.Conn
	lastLSN     pglogrepl.LSN
	statusTable string
}

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

func (s *PostgresSink) saveStatus() error {
	_, err := s.conn.Exec(context.Background(), "INSERT INTO internal_pg_flo.lsn_status (last_lsn) VALUES ($1)", s.lastLSN.String())
	return err
}

func (s *PostgresSink) handleInsert(tx pgx.Tx, event map[string]interface{}) error {
	schema := event["schema"].(string)
	table := event["table"].(string)
	newRow := make(map[string]utils.CDCValue)
	if err := mapstructure.Decode(event["new_row"], &newRow); err != nil {
		return fmt.Errorf("failed to decode new_row: %v", err)
	}

	columns := make([]string, 0, len(newRow))
	placeholders := make([]string, 0, len(newRow))
	values := make([]interface{}, 0, len(newRow))

	i := 1
	for col, val := range newRow {
		columns = append(columns, col)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		values = append(values, val.Value)
		i++
	}

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		schema, table, strings.Join(columns, ","), strings.Join(placeholders, ","))

	_, err := tx.Exec(context.Background(), query, values...)
	return err
}

func (s *PostgresSink) handleUpdate(tx pgx.Tx, event map[string]interface{}) error {
	schema := event["schema"].(string)
	table := event["table"].(string)
	newRow := make(map[string]utils.CDCValue)
	if err := mapstructure.Decode(event["new_row"], &newRow); err != nil {
		return fmt.Errorf("failed to decode new_row: %v", err)
	}

	columns := make([]string, 0, len(newRow))
	placeholders := make([]string, 0, len(newRow))
	updateClauses := make([]string, 0, len(newRow))
	values := make([]interface{}, 0, len(newRow))

	i := 1
	for col, val := range newRow {
		columns = append(columns, col)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		values = append(values, val.Value)
		i++
	}

	pkColumn, ok := event["primary_key_column"].(string)
	if !ok || pkColumn == "" {
		return fmt.Errorf("no primary key column found in event")
	}

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		schema, table, strings.Join(columns, ","), strings.Join(placeholders, ","), pkColumn, strings.Join(updateClauses, ","))

	_, err := tx.Exec(context.Background(), query, values...)
	return err
}

func (s *PostgresSink) handleDelete(tx pgx.Tx, event map[string]interface{}) error {
	schema := event["schema"].(string)
	table := event["table"].(string)

	pkColumn, ok := event["primary_key_column"].(string)
	if !ok || pkColumn == "" {
		return fmt.Errorf("no primary key column found in event")
	}

	oldRow, ok := event["old_row"].(map[string]utils.CDCValue)
	if !ok {
		return fmt.Errorf("no old_row found in event")
	}

	val, ok := oldRow[pkColumn]
	if !ok {
		return fmt.Errorf("primary key column %s not found in old_row", pkColumn)
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s = $1",
		schema, table, pkColumn)

	_, err := tx.Exec(context.Background(), query, val.Value)
	return err
}

func (s *PostgresSink) handleDDL(tx pgx.Tx, event map[string]interface{}) error {
	ddlCommand := event["command"].(string)
	_, err := tx.Exec(context.Background(), ddlCommand)
	return err
}

func (s *PostgresSink) WriteBatch(data []interface{}) error {
	tx, err := s.conn.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	for _, item := range data {
		event, ok := item.(map[string]interface{})
		if !ok {
			err := tx.Rollback(context.Background())
			if err != nil {
				log.Error().Err(err).Msg("failed to rollback transaction")
			}
			log.Error().Msg("failed to cast event to map[string]interface{}")
			return nil
		}

		eventType := event["type"].(string)
		switch eventType {
		case "INSERT":
			if err := s.handleInsert(tx, event); err != nil {
				err := tx.Rollback(context.Background())
				if err != nil {
					log.Error().Err(err).Msg("failed to rollback transaction")
				}
				log.Error().Msg("failed to handle insert")
				return nil
			}
		case "UPDATE":
			if err := s.handleUpdate(tx, event); err != nil {
				err := tx.Rollback(context.Background())
				if err != nil {
					log.Error().Err(err).Msg("failed to rollback transaction")
				}
				log.Error().Err(err).Msgf("failed to handle update: %v", event)
				return nil
			}
		case "DELETE":
			if err := s.handleDelete(tx, event); err != nil {
				err := tx.Rollback(context.Background())
				if err != nil {
					log.Error().Err(err).Msg("failed to rollback transaction")
				}
				log.Error().Err(err).Msgf("failed to handle delete: %v", event)
				return nil
			}
		case "DDL":
			if err := s.handleDDL(tx, event); err != nil {
				err := tx.Rollback(context.Background())
				if err != nil {
					log.Error().Err(err).Msg("failed to rollback transaction")
				}
				log.Error().Err(err).Msg("failed to handle DDL")
				return nil
			}
		default:
			err := tx.Rollback(context.Background())
			if err != nil {
				log.Error().Err(err).Msg("failed to rollback transaction")
			}
			log.Error().Msgf("unknown event type: %s", eventType)
			return nil
		}
	}

	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

func (s *PostgresSink) GetLastLSN() (pglogrepl.LSN, error) {
	return s.lastLSN, nil
}

func (s *PostgresSink) SetLastLSN(lsn pglogrepl.LSN) error {
	s.lastLSN = lsn
	return s.saveStatus()
}

func (s *PostgresSink) Close() error {
	return s.conn.Close(context.Background())
}
