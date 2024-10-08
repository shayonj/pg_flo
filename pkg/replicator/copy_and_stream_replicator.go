package replicator

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/utils"
)

func (r *CopyAndStreamReplicator) NewBaseReplicator() *BaseReplicator {
	return &r.BaseReplicator
}

// CopyAndStreamReplicator implements a replication strategy that first copies existing data
// and then streams changes.
type CopyAndStreamReplicator struct {
	BaseReplicator
	MaxCopyWorkersPerTable int
	DDLReplicator          DDLReplicator
}

// CreatePublication creates a new publication for the specified tables.
func (r *CopyAndStreamReplicator) CreatePublication() error {
	return r.BaseReplicator.CreatePublication()
}

// StartReplication begins the replication process.
func (r *CopyAndStreamReplicator) StartReplication() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go r.handleShutdownSignal(sigChan, cancel)

	if err := r.BaseReplicator.CreateReplicationSlot(ctx); err != nil {
		return fmt.Errorf("failed to create replication slot: %v", err)
	}

	var ddlCancel context.CancelFunc
	if r.Config.TrackDDL {
		if err := r.DDLReplicator.SetupDDLTracking(ctx); err != nil {
			return fmt.Errorf("failed to set up DDL tracking: %v", err)
		}
		var ddlCtx context.Context
		ddlCtx, ddlCancel = context.WithCancel(ctx)
		go r.DDLReplicator.StartDDLReplication(ddlCtx)
	}

	defer func() {
		if r.Config.TrackDDL {
			ddlCancel()
			if err := r.DDLReplicator.Shutdown(context.Background()); err != nil {
				r.Logger.Error().Err(err).Msg("Failed to shutdown DDL replicator")
			}
		}
	}()

	startLSN, err := r.getStartLSN()
	if err != nil {
		return err
	}

	r.Logger.Info().Str("startLSN", startLSN.String()).Msg("Starting replication from LSN")
	return r.BaseReplicator.StartReplicationFromLSN(ctx, startLSN)
}

// handleShutdownSignal waits for a shutdown signal and cancels the context.
func (r *CopyAndStreamReplicator) handleShutdownSignal(sigChan <-chan os.Signal, cancel context.CancelFunc) {
	sig := <-sigChan
	r.Logger.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
	cancel()
}

// getStartLSN determines the starting LSN for replication.
func (r *CopyAndStreamReplicator) getStartLSN() (pglogrepl.LSN, error) {
	r.Logger.Info().Msg("No valid LSN found in sink, starting initial copy")
	if copyErr := r.ParallelCopy(context.Background()); copyErr != nil {
		return 0, fmt.Errorf("failed to perform parallel copy: %v", copyErr)
	}
	return r.BaseReplicator.LastLSN, nil
}

// ParallelCopy performs a parallel copy of all specified tables.
func (r *CopyAndStreamReplicator) ParallelCopy(ctx context.Context) error {

	for _, table := range r.Config.Tables {
		if err := r.analyzeTable(ctx, table); err != nil {
			return fmt.Errorf("failed to analyze table %s: %v", table, err)
		}
	}

	tx, err := r.startSnapshotTransaction(ctx)
	if err != nil {
		return err
	}

	snapshotID, startLSN, err := r.getSnapshotInfo(tx)
	if err != nil {
		return err
	}

	r.Logger.Info().Str("snapshotID", snapshotID).Str("startLSN", startLSN.String()).Msg("Starting parallel copy")

	r.BaseReplicator.LastLSN = startLSN

	if err := r.CopyTables(ctx, r.Config.Tables, snapshotID); err != nil {
		return err
	}

	return tx.Commit(context.Background())
}

// analyzeTable runs ANALYZE on the specified table.
func (r *CopyAndStreamReplicator) analyzeTable(ctx context.Context, tableName string) error {
	r.Logger.Info().Str("table", tableName).Msg("Running ANALYZE on table")
	_, err := r.BaseReplicator.StandardConn.Exec(ctx, fmt.Sprintf("ANALYZE %s", pgx.Identifier{tableName}.Sanitize()))
	return err
}

// startSnapshotTransaction starts a new transaction with serializable isolation level.
func (r *CopyAndStreamReplicator) startSnapshotTransaction(ctx context.Context) (pgx.Tx, error) {
	return r.BaseReplicator.StandardConn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadOnly,
	})
}

// getSnapshotInfo retrieves the snapshot ID and current WAL LSN.
func (r *CopyAndStreamReplicator) getSnapshotInfo(tx pgx.Tx) (string, pglogrepl.LSN, error) {
	var snapshotID string
	var startLSN pglogrepl.LSN
	err := tx.QueryRow(context.Background(), `
		SELECT pg_export_snapshot(), pg_current_wal_lsn()::text::pg_lsn
	`).Scan(&snapshotID, &startLSN)
	if err != nil {
		return "", 0, fmt.Errorf("failed to export snapshot and get LSN: %v", err)
	}
	return snapshotID, startLSN, nil
}

// CopyTables copies all specified tables in parallel.
func (r *CopyAndStreamReplicator) CopyTables(ctx context.Context, tables []string, snapshotID string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(tables))

	for _, table := range tables {
		wg.Add(1)
		go func(tableName string) {
			defer wg.Done()
			if err := r.CopyTable(ctx, tableName, snapshotID); err != nil {
				errChan <- fmt.Errorf("failed to copy table %s: %v", tableName, err)
			}
		}(table)
	}

	wg.Wait()
	close(errChan)

	return r.collectErrors(errChan)
}

// CopyTable copies a single table using multiple workers.
func (r *CopyAndStreamReplicator) CopyTable(ctx context.Context, tableName, snapshotID string) error {
	r.Logger.Info().Str("tableName", tableName).Msg("Copying table")

	relPages, err := r.getRelPages(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get relPages for table %s: %v", tableName, err)
	}

	r.Logger.Info().Str("table", tableName).Uint32("relPages", relPages).Msg("Retrieved relPages for table")

	ranges := r.generateRanges(relPages)
	rangesChan := make(chan [2]uint32, len(ranges))
	for _, rng := range ranges {
		rangesChan <- rng
	}
	close(rangesChan)

	var wg sync.WaitGroup
	errChan := make(chan error, r.MaxCopyWorkersPerTable)

	for i := 0; i < r.MaxCopyWorkersPerTable; i++ {
		wg.Add(1)
		go r.CopyTableWorker(ctx, &wg, errChan, rangesChan, tableName, snapshotID, i)
	}

	wg.Wait()
	close(errChan)

	return r.collectErrors(errChan)
}

// getRelPages retrieves the number of pages for a given table.
func (r *CopyAndStreamReplicator) getRelPages(ctx context.Context, tableName string) (uint32, error) {
	var relPages uint32
	err := r.BaseReplicator.StandardConn.QueryRow(ctx, `
		SELECT relpages
		FROM pg_class
		WHERE relname = $1
	`, tableName).Scan(&relPages)
	return relPages, err
}

// generateRanges creates a set of page ranges for copying.
func (r *CopyAndStreamReplicator) generateRanges(relPages uint32) [][2]uint32 {
	var ranges [][2]uint32
	batchSize := uint32(100)
	for start := uint32(0); start < relPages; start += batchSize {
		end := start + batchSize
		if end >= relPages {
			end = ^uint32(0) // Use max uint32 value for the last range
		}
		ranges = append(ranges, [2]uint32{start, end})
	}
	return ranges
}

// CopyTableWorker is a worker function that copies ranges of pages from a table.
func (r *CopyAndStreamReplicator) CopyTableWorker(ctx context.Context, wg *sync.WaitGroup, errChan chan<- error, rangesChan <-chan [2]uint32, tableName, snapshotID string, workerID int) {
	defer wg.Done()

	for rng := range rangesChan {
		startPage, endPage := rng[0], rng[1]

		rowsCopied, err := r.CopyTableRange(ctx, tableName, startPage, endPage, snapshotID, workerID)
		if err != nil {
			if err == context.Canceled {
				r.Logger.Info().Msg("Copy operation canceled")
				return
			}
			r.Logger.Err(err).Msg("Failed to copy table range")
			errChan <- fmt.Errorf("failed to copy table range: %v", err)
			return
		}

		r.Logger.Info().Str("table", tableName).Uint32("startPage", startPage).Uint32("endPage", endPage).Int64("rowsCopied", rowsCopied).Int("workerID", workerID).Msg("Copied table range")
	}
}

// CopyTableRange copies a range of pages from a table.
func (r *CopyAndStreamReplicator) CopyTableRange(ctx context.Context, tableName string, startPage, endPage uint32, snapshotID string, workerID int) (int64, error) {
	conn, err := r.BaseReplicator.StandardConn.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %v", err)
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to start transaction: %v", err)
	}
	defer func() {
		if err := tx.Commit(ctx); err != nil {
			r.Logger.Error().Err(err).Msg("Failed to commit transaction")
		}
	}()

	if setSnapshotErr := r.setTransactionSnapshot(tx, snapshotID); setSnapshotErr != nil {
		return 0, setSnapshotErr
	}

	schema, err := r.getSchemaName(tx, tableName)
	if err != nil {
		return 0, err
	}

	query := r.buildCopyQuery(tableName, startPage, endPage)
	return r.executeCopyQuery(ctx, tx, query, schema, tableName, startPage, endPage, workerID)
}

// setTransactionSnapshot sets the transaction snapshot.
func (r *CopyAndStreamReplicator) setTransactionSnapshot(tx pgx.Tx, snapshotID string) error {
	_, err := tx.Exec(context.Background(), fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID))
	if err != nil {
		return fmt.Errorf("failed to set transaction snapshot: %v", err)
	}
	return nil
}

// getSchemaName retrieves the schema name for a given table.
func (r *CopyAndStreamReplicator) getSchemaName(tx pgx.Tx, tableName string) (string, error) {
	var schema string
	err := tx.QueryRow(context.Background(), "SELECT schemaname FROM pg_tables WHERE tablename = $1", tableName).Scan(&schema)
	if err != nil {
		return "", fmt.Errorf("failed to get schema name: %v", err)
	}
	return schema, nil
}

// buildCopyQuery constructs the SQL query for copying a range of pages from a table.
func (r *CopyAndStreamReplicator) buildCopyQuery(tableName string, startPage, endPage uint32) string {
	query := fmt.Sprintf(`
			SELECT *
			FROM %s
			WHERE ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid`,
		pgx.Identifier{tableName}.Sanitize(), startPage, endPage)
	return query
}

// executeCopyQuery executes the copy query and writes the results to the sink.
func (r *CopyAndStreamReplicator) executeCopyQuery(ctx context.Context, tx pgx.Tx, query, schema, tableName string, startPage, endPage uint32, workerID int) (int64, error) {
	r.Logger.Debug().Str("copyQuery", query).Int("workerID", workerID).Msg("Executing initial copy query")

	rows, err := tx.Query(context.Background(), query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute initial copy query: %v", err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columnNames[i] = fd.Name
	}

	var copyCount int64
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, fmt.Errorf("error reading row: %v", err)
		}

		newRow := make(map[string]utils.CDCValue)
		for i, name := range columnNames {
			newRow[name] = utils.CDCValue{
				Type:  fieldDescriptions[i].DataTypeOID,
				Value: values[i],
			}
		}

		// Apply rules if the rule engine is available
		if r.BaseReplicator.RuleEngine != nil {
			newRow, err = r.BaseReplicator.RuleEngine.ApplyRules(tableName, newRow, rules.OperationInsert)
			if err != nil {
				return 0, fmt.Errorf("failed to apply rules: %v", err)
			}
			if newRow == nil {
				// Row filtered out by rules
				continue
			}
		}

		insertEvent := map[string]interface{}{
			"type":    "INSERT",
			"schema":  schema,
			"table":   tableName,
			"new_row": newRow,
		}
		r.BaseReplicator.addPrimaryKeyInfo(insertEvent, tableName)
		if err := r.bufferWrite(insertEvent); err != nil {
			return 0, fmt.Errorf("failed to write insert event to Buffer: %v", err)
		}

		copyCount++

		select {
		case <-ctx.Done():
			return copyCount, ctx.Err()
		default:
		}
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error during row iteration: %v", err)
	}

	// Flush the buffer after processing all rows for this range
	if err := r.FlushBuffer(); err != nil {
		return 0, fmt.Errorf("failed to flush buffer after copying range: %v", err)
	}

	r.Logger.Info().Str("table", tableName).Int("start_page", int(startPage)).Int("end_page", int(endPage)).Int64("rows_copied", copyCount).Msg("Copied table range")

	return copyCount, nil
}

// collectErrors collects errors from the error channel and returns them as a single error.
func (r *CopyAndStreamReplicator) collectErrors(errChan <-chan error) error {
	var errs []error
	for err := range errChan {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("multiple errors occurred: %v", errs)
	}
	return nil
}

// bufferWrite adds data to the buffer and flushes if necessary
func (r *CopyAndStreamReplicator) bufferWrite(data interface{}) error {
	return r.BaseReplicator.bufferWrite(data)
}
