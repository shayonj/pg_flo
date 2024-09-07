package utils

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
)

// CDCValue represents a value with its PostgreSQL type
type CDCValue struct {
	Type  uint32
	Value interface{}
}

// CDCMessage represents a full message for Change Data Capture
type CDCMessage struct {
	Type             string
	Schema           string
	Table            string
	Columns          []*pglogrepl.RelationMessageColumn
	NewTuple         *pglogrepl.TupleData
	OldTuple         *pglogrepl.TupleData
	PrimaryKeyColumn string
	LSN              pglogrepl.LSN
	CommitTimestamp  time.Time
}

// OidToTypeName maps PostgreSQL OIDs to their corresponding type names
var OidToTypeName = map[uint32]string{
	pgtype.BoolOID:             "bool",
	pgtype.ByteaOID:            "bytea",
	pgtype.Int8OID:             "int8",
	pgtype.Int2OID:             "int2",
	pgtype.Int4OID:             "int4",
	pgtype.TextOID:             "text",
	pgtype.JSONOID:             "json",
	pgtype.Float4OID:           "float4",
	pgtype.Float8OID:           "float8",
	pgtype.BoolArrayOID:        "bool[]",
	pgtype.Int2ArrayOID:        "int2[]",
	pgtype.Int4ArrayOID:        "int4[]",
	pgtype.TextArrayOID:        "text[]",
	pgtype.ByteaArrayOID:       "bytea[]",
	pgtype.Int8ArrayOID:        "int8[]",
	pgtype.Float4ArrayOID:      "float4[]",
	pgtype.Float8ArrayOID:      "float8[]",
	pgtype.BPCharOID:           "bpchar",
	pgtype.VarcharOID:          "varchar",
	pgtype.DateOID:             "date",
	pgtype.TimeOID:             "time",
	pgtype.TimestampOID:        "timestamp",
	pgtype.TimestampArrayOID:   "timestamp[]",
	pgtype.DateArrayOID:        "date[]",
	pgtype.TimestamptzOID:      "timestamptz",
	pgtype.TimestamptzArrayOID: "timestamptz[]",
	pgtype.IntervalOID:         "interval",
	pgtype.NumericArrayOID:     "numeric[]",
	pgtype.BitOID:              "bit",
	pgtype.VarbitOID:           "varbit",
	pgtype.NumericOID:          "numeric",
	pgtype.UUIDOID:             "uuid",
	pgtype.UUIDArrayOID:        "uuid[]",
	pgtype.JSONBOID:            "jsonb",
	pgtype.JSONBArrayOID:       "jsonb[]",
}

// OIDToString converts a PostgreSQL OID to its string representation
func OIDToString(oid uint32) string {
	if typeName, ok := OidToTypeName[oid]; ok {
		return typeName
	}
	return fmt.Sprintf("unknown_%d", oid)
}

// StringToOID converts a type name to its PostgreSQL OID
func StringToOID(typeName string) uint32 {
	for oid, name := range OidToTypeName {
		if name == typeName {
			return oid
		}
	}
	return 0 // or some error value
}

// init registers types with the gob package for encoding/decoding
func init() {
	gob.Register(json.RawMessage{})
	gob.Register(time.Time{})
	gob.Register(map[string]interface{}{})
	gob.Register(pglogrepl.RelationMessageColumn{})
	gob.Register(pglogrepl.LSN(0))

	gob.Register(CDCMessage{})
	gob.Register(pglogrepl.TupleData{})
	gob.Register(pglogrepl.TupleDataColumn{})
	gob.Register(time.Time{})
}

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (m CDCMessage) MarshalBinary() ([]byte, error) {
	return EncodeCDCMessage(m)
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface
func (m *CDCMessage) UnmarshalBinary(data []byte) error {
	decodedMessage, err := DecodeCDCMessage(data)
	if err != nil {
		return err
	}
	*m = *decodedMessage
	return nil
}
