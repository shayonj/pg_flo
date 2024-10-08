package replicator

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pglogrepl"
)

type StreamReplicator struct {
	BaseReplicator
	DDLReplicator DDLReplicator
}

// CreatePublication creates a new publication for the specified tables.
func (r *StreamReplicator) CreatePublication() error {
	return r.BaseReplicator.CreatePublication()
}

// StartReplication begins the replication process.
func (r *StreamReplicator) StartReplication() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
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
func (r *StreamReplicator) handleShutdownSignal(sigChan <-chan os.Signal, cancel context.CancelFunc) {
	sig := <-sigChan
	r.Logger.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
	cancel()
}

// getStartLSN determines the starting LSN for replication. The actual shutdown logic
// is handled in BaseReplicator.StartReplicationFromLSN
func (r *StreamReplicator) getStartLSN() (pglogrepl.LSN, error) {
	startLSN, err := r.Sink.GetLastLSN()
	if err != nil {
		r.Logger.Warn().Err(err).Msg("Failed to get last LSN from sink, starting from 0")
		return pglogrepl.LSN(0), nil
	}
	return startLSN, nil
}
