package replicator

import "fmt"

// ReplicationError represents an error that occurred during replication.
type ReplicationError struct {
	Op  string // The operation that caused the error
	Err error  // The underlying error
}

// Error returns a formatted error message.
func (e *ReplicationError) Error() string {
	return fmt.Sprintf("replication error during %s: %v", e.Op, e.Err)
}
