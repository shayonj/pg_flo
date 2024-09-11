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

// StartReplication begins the replication process.
func (r *StreamReplicator) StartReplication() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go r.handleShutdownSignal(sigChan, cancel)

	if err := r.BaseReplicator.CreatePublication(); err != nil {
		return fmt.Errorf("failed to create publication: %v", err)
	}

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

	if err := r.BaseReplicator.CheckReplicationSlotStatus(ctx); err != nil {
		return fmt.Errorf("failed to check replication slot status: %v", err)
	}

	startLSN, err := r.getStartLSN(ctx)
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

// getStartLSN determines the starting LSN for replication.
func (r *StreamReplicator) getStartLSN(ctx context.Context) (pglogrepl.LSN, error) {
	startLSN, err := r.BaseReplicator.GetLastState(ctx)
	if err != nil {
		r.Logger.Warn().Err(err).Msg("Failed to get last LSN, starting from 0")
		return pglogrepl.LSN(0), nil
	}
	return startLSN, nil
}
