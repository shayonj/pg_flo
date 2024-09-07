package replicator

import (
	"context"
	"fmt"

	"github.com/shayonj/pg_flo/pkg/pgflonats"
)

// NewReplicator creates a new Replicator based on the configuration
func NewReplicator(config Config, natsClient *pgflonats.NATSClient, copyAndStream bool, maxCopyWorkersPerTable int) (Replicator, error) {
	replicationConn := NewReplicationConnection(config)
	if err := replicationConn.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect to database for replication: %v", err)
	}

	standardConn, err := NewStandardConnection(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create standard connection: %v", err)
	}

	baseReplicator := NewBaseReplicator(config, replicationConn, standardConn, natsClient)

	var ddlReplicator *DDLReplicator
	if config.TrackDDL {
		ddlConn, err := NewStandardConnection(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create DDL connection: %v", err)
		}
		ddlReplicator, err = NewDDLReplicator(config, baseReplicator, ddlConn)
		if err != nil {
			return nil, fmt.Errorf("failed to create DDL replicator: %v", err)
		}
	}

	if copyAndStream {
		if maxCopyWorkersPerTable <= 0 {
			maxCopyWorkersPerTable = 4
		}
		copyAndStreamReplicator := &CopyAndStreamReplicator{
			BaseReplicator:         *baseReplicator,
			MaxCopyWorkersPerTable: maxCopyWorkersPerTable,
		}
		if ddlReplicator != nil {
			copyAndStreamReplicator.DDLReplicator = *ddlReplicator
		}
		return copyAndStreamReplicator, nil
	}

	streamReplicator := &StreamReplicator{
		BaseReplicator: *baseReplicator,
	}

	if ddlReplicator != nil {
		streamReplicator.DDLReplicator = *ddlReplicator
	}

	return streamReplicator, nil
}
