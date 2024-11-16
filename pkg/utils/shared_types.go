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

type Logger interface {
	Debug() LogEvent
	Info() LogEvent
	Warn() LogEvent
	Error() LogEvent
	Err(err error) LogEvent
}

type LogEvent interface {
	Str(key, val string) LogEvent
	Int(key string, val int) LogEvent
	Int64(key string, val int64) LogEvent
	Uint8(key string, val uint8) LogEvent
	Uint32(key string, val uint32) LogEvent
	Interface(key string, val interface{}) LogEvent
	Err(err error) LogEvent
	Strs(key string, vals []string) LogEvent
	Any(key string, val interface{}) LogEvent
	Type(key string, val interface{}) LogEvent
	Msg(msg string)
	Msgf(format string, v ...interface{})
}
