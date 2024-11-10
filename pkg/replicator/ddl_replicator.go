package replicator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/shayonj/pg_flo/pkg/utils"
)

type DDLReplicator struct {
	DDLConn  StandardConnection
	BaseRepl *BaseReplicator
	Config   Config
}

// NewDDLReplicator creates a new DDLReplicator instance
func NewDDLReplicator(config Config, BaseRepl *BaseReplicator, ddlConn StandardConnection) (*DDLReplicator, error) {
	return &DDLReplicator{
		Config:   config,
		BaseRepl: BaseRepl,
		DDLConn:  ddlConn,
	}, nil
}

// SetupDDLTracking sets up the necessary schema, table, and triggers for DDL tracking
func (d *DDLReplicator) SetupDDLTracking(ctx context.Context) error {
	_, err := d.DDLConn.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS internal_pg_flo;

		CREATE TABLE IF NOT EXISTS internal_pg_flo.ddl_log (
			id SERIAL PRIMARY KEY,
			event_type TEXT NOT NULL,
			object_type TEXT,
			object_identity TEXT,
			table_name TEXT,
			ddl_command TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE OR REPLACE FUNCTION internal_pg_flo.ddl_trigger() RETURNS event_trigger AS $$
		DECLARE
			obj record;
			ddl_command text;
			table_name text;
		BEGIN
			SELECT current_query() INTO ddl_command;

			IF TG_EVENT = 'ddl_command_end' THEN
				FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
				LOOP
					-- Extract table name if object type is table or index
					IF obj.object_type IN ('table', 'table column') THEN
						SELECT nspname || '.' || relname
						INTO table_name
						FROM pg_class c
						JOIN pg_namespace n ON c.relnamespace = n.oid
						WHERE c.oid = obj.objid;
					ELSIF obj.object_type = 'index' THEN
						SELECT nspname || '.' || t.relname
						INTO table_name
						FROM pg_index i
						JOIN pg_class t ON t.oid = i.indrelid
						JOIN pg_namespace n ON t.relnamespace = n.oid
						WHERE i.indexrelid = obj.objid;
					ELSE
						table_name := NULL;
					END IF;

					INSERT INTO internal_pg_flo.ddl_log (event_type, object_type, object_identity, table_name, ddl_command)
					VALUES (TG_EVENT, obj.object_type, obj.object_identity, table_name, ddl_command);
				END LOOP;

			ELSIF TG_EVENT = 'sql_drop' THEN
				FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
				LOOP
					-- Attempt to extract table name if the object still exists
					BEGIN
						IF obj.object_type IN ('table', 'table column') THEN
							SELECT nspname || '.' || relname
							INTO table_name
							FROM pg_class c
							JOIN pg_namespace n ON c.relnamespace = n.oid
							WHERE c.oid = obj.objid;
					ELSIF obj.object_type = 'index' THEN
						SELECT nspname || '.' || t.relname
						INTO table_name
						FROM pg_index i
						JOIN pg_class t ON t.oid = i.indrelid
						JOIN pg_namespace n ON t.relnamespace = n.oid
						WHERE i.indexrelid = obj.objid;
					ELSE
						table_name := NULL;
					END IF;
				END;

				INSERT INTO internal_pg_flo.ddl_log (event_type, object_type, object_identity, table_name, ddl_command)
				VALUES (TG_EVENT, obj.object_type, obj.object_identity, table_name, ddl_command);
			END LOOP;

			ELSIF TG_EVENT = 'table_rewrite' THEN
				FOR obj IN SELECT * FROM pg_event_trigger_table_rewrite_oid()
				LOOP
					SELECT nspname || '.' || relname
					INTO table_name
					FROM pg_class c
					JOIN pg_namespace n ON c.relnamespace = n.oid
					WHERE c.oid = obj.oid;

					INSERT INTO internal_pg_flo.ddl_log (event_type, object_type, object_identity, table_name, ddl_command)
					VALUES (TG_EVENT, 'table', table_name, table_name, ddl_command);
				END LOOP;

			END IF;
		END;
	$$ LANGUAGE plpgsql;


		DROP EVENT TRIGGER IF EXISTS pg_flo_ddl_trigger;
		CREATE EVENT TRIGGER pg_flo_ddl_trigger ON ddl_command_end
		EXECUTE FUNCTION internal_pg_flo.ddl_trigger();

		DROP EVENT TRIGGER IF EXISTS pg_flo_drop_trigger;
		CREATE EVENT TRIGGER pg_flo_drop_trigger ON sql_drop
		EXECUTE FUNCTION internal_pg_flo.ddl_trigger();

		DROP EVENT TRIGGER IF EXISTS pg_flo_table_rewrite_trigger;
		CREATE EVENT TRIGGER pg_flo_table_rewrite_trigger ON table_rewrite
		EXECUTE FUNCTION internal_pg_flo.ddl_trigger();
	`)
	if err != nil {
		d.BaseRepl.Logger.Error().Err(err).Msg("Failed to setup DDL tracking")
		return err
	}
	return nil
}

// StartDDLReplication starts the DDL replication process
func (d *DDLReplicator) StartDDLReplication(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.BaseRepl.Logger.Info().Msg("Finishing processing of DDL events... (this can take a while)")

			// Create a new context without cancellation to process remaining events
			// Alternatively, when a shutdown is received we can trigger a ROLLBACK too ?
			shutdownCtx := context.Background()

			for {
				hasEvents, err := d.HasPendingDDLEvents(shutdownCtx)
				if err != nil {
					d.BaseRepl.Logger.Error().Err(err).Msg("Failed to check for pending DDL events during shutdown")
					break
				}
				if !hasEvents {
					break
				}
				if err := d.ProcessDDLEvents(shutdownCtx); err != nil {
					d.BaseRepl.Logger.Error().Err(err).Msg("Failed to process DDL events during shutdown")
				}

				time.Sleep(100 * time.Millisecond)
			}
			return
		case <-ticker.C:
			if err := d.ProcessDDLEvents(ctx); err != nil {
				d.BaseRepl.Logger.Error().Err(err).Msg("Failed to process DDL events")
			}
		}
	}
}

// ProcessDDLEvents processes DDL events from the log table
func (d *DDLReplicator) ProcessDDLEvents(ctx context.Context) error {
	rows, err := d.DDLConn.Query(ctx, `
		SELECT id, event_type, object_type, object_identity, table_name, ddl_command, created_at
		FROM internal_pg_flo.ddl_log
		ORDER BY created_at ASC
	`)
	if err != nil {
		d.BaseRepl.Logger.Error().Err(err).Msg("Failed to query DDL log")
		return nil
	}
	defer rows.Close()

	var processedIDs []int
	seenCommands := make(map[string]bool)

	for rows.Next() {
		var id int
		var eventType, objectType, objectIdentity, ddlCommand string
		var tableName sql.NullString
		var createdAt time.Time
		if err := rows.Scan(&id, &eventType, &objectType, &objectIdentity, &tableName, &ddlCommand, &createdAt); err != nil {
			d.BaseRepl.Logger.Error().Err(err).Msg("Failed to scan DDL log row")
			return nil
		}

		if d.shouldSkipDDLEvent(ddlCommand) {
			processedIDs = append(processedIDs, id)
			continue
		}

		if seenCommands[ddlCommand] {
			processedIDs = append(processedIDs, id)
			continue
		}
		seenCommands[ddlCommand] = true

		var schema, table string
		if tableName.Valid {
			schema, table = splitSchemaAndTable(tableName.String)
		} else {
			schema, table = "public", ""
		}

		cdcMessage := utils.CDCMessage{
			Type:      "DDL",
			Schema:    schema,
			Table:     table,
			EmittedAt: time.Now(),
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "event_type", DataType: pgtype.TextOID},
				{Name: "object_type", DataType: pgtype.TextOID},
				{Name: "object_identity", DataType: pgtype.TextOID},
				{Name: "ddl_command", DataType: pgtype.TextOID},
				{Name: "created_at", DataType: pgtype.TimestamptzOID},
			},
			NewTuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte(eventType)},
					{Data: []byte(objectType)},
					{Data: []byte(objectIdentity)},
					{Data: []byte(ddlCommand)},
					{Data: []byte(createdAt.Format(time.RFC3339))},
				},
			},
		}

		if err := d.BaseRepl.PublishToNATS(cdcMessage); err != nil {
			d.BaseRepl.Logger.Error().Err(err).Msg("Error during publishing DDL event to NATS")
			return nil
		}

		processedIDs = append(processedIDs, id)
	}

	if err := rows.Err(); err != nil {
		d.BaseRepl.Logger.Error().Err(err).Msg("Error during DDL log iteration")
		return nil
	}

	if len(processedIDs) > 0 {
		_, err = d.DDLConn.Exec(ctx, "DELETE FROM internal_pg_flo.ddl_log WHERE id = ANY($1)", processedIDs)
		if err != nil {
			d.BaseRepl.Logger.Error().Err(err).Msg("Failed to clear processed DDL events")
			return nil
		}
	}

	return nil
}

// splitSchemaAndTable splits a full table name into schema and table parts
func splitSchemaAndTable(fullName string) (string, string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", fullName
}

// Close closes the DDL connection
func (d *DDLReplicator) Close(ctx context.Context) error {
	if d.DDLConn != nil {
		return d.DDLConn.Close(ctx)
	}
	return nil
}

// Shutdown performs a graceful shutdown of the DDL replicator
func (d *DDLReplicator) Shutdown(ctx context.Context) error {
	d.BaseRepl.Logger.Info().Msg("Shutting down DDL replicator")
	if ctx.Err() != nil {
		ctx = context.Background()
	}
	if err := d.ProcessDDLEvents(ctx); err != nil {
		d.BaseRepl.Logger.Error().Err(err).Msg("Failed to process final DDL events")
		return err
	}
	return d.Close(ctx)
}

// HasPendingDDLEvents checks if there are pending DDL events in the log
func (d *DDLReplicator) HasPendingDDLEvents(ctx context.Context) (bool, error) {
	var count int
	err := d.DDLConn.QueryRow(ctx, `
		SELECT COUNT(*) FROM internal_pg_flo.ddl_log
	`).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// shouldSkipDDLEvent determines if a DDL event should be skipped from processing
func (d *DDLReplicator) shouldSkipDDLEvent(ddlCommand string) bool {
	// Skip pg_flo internal operations
	if strings.Contains(ddlCommand, "internal_pg_flo.") {
		return true
	}

	publicationName := GeneratePublicationName(d.Config.Group)
	if strings.Contains(ddlCommand, fmt.Sprintf("CREATE PUBLICATION %q", publicationName)) ||
		strings.Contains(ddlCommand, fmt.Sprintf("DROP PUBLICATION %q", publicationName)) ||
		strings.Contains(ddlCommand, "CREATE PUBLICATION pg_flo_") ||
		strings.Contains(ddlCommand, "DROP PUBLICATION pg_flo_") {
		return true
	}

	return false
}
