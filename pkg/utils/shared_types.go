package utils

// OperationType represents the type of database operation
type OperationType string

const (
	OperationInsert OperationType = "INSERT"
	OperationUpdate OperationType = "UPDATE"
	OperationDelete OperationType = "DELETE"
	OperationDDL    OperationType = "DDL"
)

// ReplicationKeyType represents the type of replication key
type ReplicationKeyType string

const (
	ReplicationKeyPK     ReplicationKeyType = "PRIMARY KEY"
	ReplicationKeyUnique ReplicationKeyType = "UNIQUE"
	ReplicationKeyFull   ReplicationKeyType = "FULL" // Replica identity full
)

// ReplicationKey represents a key used for replication (either PK or unique constraint)
type ReplicationKey struct {
	Type    ReplicationKeyType
	Columns []string
}
