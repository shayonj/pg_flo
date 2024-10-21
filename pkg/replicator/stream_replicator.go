package replicator

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jackc/pglogrepl"
)

type StreamReplicator struct {
	BaseReplicator
	DDLReplicator DDLReplicator
}

// StartReplication begins the replication process.
func (r *StreamReplicator) StartReplication() error {
	ctx := context.Background()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if err := r.setup(ctx); err != nil {
		return err
	}

	startLSN, err := r.getStartLSN()
	if err != nil {
		return err
	}

	r.Logger.Info().Str("startLSN", startLSN.String()).Msg("Starting replication from LSN")

	// Start DDL replication with its own cancellable context and wait group
	var ddlWg sync.WaitGroup
	var ddlCancel context.CancelFunc
	if r.Config.TrackDDL {
		ddlCtx, cancel := context.WithCancel(ctx)
		ddlCancel = cancel
		if err := r.DDLReplicator.SetupDDLTracking(ctx); err != nil {
			return fmt.Errorf("failed to set up DDL tracking: %v", err)
		}
		ddlWg.Add(1)
		go func() {
			defer ddlWg.Done()
			r.DDLReplicator.StartDDLReplication(ddlCtx)
		}()
	}

	stopChan := make(chan struct{})
	errChan := make(chan error, 1)
	go func() {
		errChan <- r.BaseReplicator.StartReplicationFromLSN(ctx, startLSN, stopChan)
	}()

	select {
	case <-sigChan:
		r.Logger.Info().Msg("Received shutdown signal")
		// Signal the replication loop to stop
		close(stopChan)
		// Wait for the replication loop to exit
		<-errChan

		// Signal DDL replication to stop and wait for it to finish
		if r.Config.TrackDDL {
			ddlCancel()
			ddlWg.Wait()
			if err := r.DDLReplicator.Shutdown(context.Background()); err != nil {
				r.Logger.Error().Err(err).Msg("Failed to shutdown DDL replicator")
			}
		}

		if err := r.BaseReplicator.GracefulShutdown(ctx); err != nil {
			r.Logger.Error().Err(err).Msg("Error during graceful shutdown")
			return err
		}
	case err := <-errChan:
		if err != nil {
			r.Logger.Error().Err(err).Msg("Replication ended with error")
			return err
		}
	}

	return nil
}

func (r *StreamReplicator) setup(ctx context.Context) error {
	if err := r.BaseReplicator.CreatePublication(); err != nil {
		return fmt.Errorf("failed to create publication: %v", err)
	}

	if err := r.BaseReplicator.CreateReplicationSlot(ctx); err != nil {
		return fmt.Errorf("failed to create replication slot: %v", err)
	}

	if err := r.BaseReplicator.CheckReplicationSlotStatus(ctx); err != nil {
		return fmt.Errorf("failed to check replication slot status: %v", err)
	}

	return nil
}

// getStartLSN determines the starting LSN for replication.
func (r *StreamReplicator) getStartLSN() (pglogrepl.LSN, error) {
	startLSN, err := r.BaseReplicator.GetLastState()
	if err != nil {
		r.Logger.Warn().Err(err).Msg("Failed to get last LSN, starting from 0")
		return pglogrepl.LSN(0), nil
	}
	return startLSN, nil
}
