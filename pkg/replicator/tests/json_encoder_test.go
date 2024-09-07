package replicator_test

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/shayonj/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestOIDToString(t *testing.T) {
	t.Run("OIDToString function", func(t *testing.T) {
		assert.Equal(t, "int4", utils.OIDToString(pgtype.Int4OID))
		assert.Equal(t, "text", utils.OIDToString(pgtype.TextOID))
		assert.Equal(t, "unknown_99999", utils.OIDToString(99999))
	})
}

func TestCDCBinaryEncoding(t *testing.T) {
	t.Run("Encode and decode preserves CDC types", func(t *testing.T) {
		testData := utils.CDCMessage{
			Type:   "INSERT",
			Schema: "public",
			Table:  "users",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: pgtype.Int4OID},
				{Name: "name", DataType: pgtype.TextOID},
			},
			NewTuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("123")},
					{Data: []byte("John Doe")},
				},
			},
		}

		encoded, err := testData.MarshalBinary()
		assert.NoError(t, err)

		var decoded utils.CDCMessage
		err = decoded.UnmarshalBinary(encoded)
		assert.NoError(t, err)

		assert.Equal(t, testData.Type, decoded.Type)
		assert.Equal(t, testData.Schema, decoded.Schema)
		assert.Equal(t, testData.Table, decoded.Table)
		assert.Equal(t, testData.Columns, decoded.Columns)
		assert.Equal(t, testData.NewTuple, decoded.NewTuple)
	})
}

func TestBinaryEncodingComplexTypes(t *testing.T) {
	t.Run("Encode and decode handles complex types", func(t *testing.T) {
		binaryData := []byte{0x01, 0x02, 0x03, 0x04}
		jsonbData := []byte(`{"key": "value", "nested": {"number": 42}}`)
		timestamp := time.Now().UTC()
		floatValue := []byte("3.14159")
		intValue := []byte("9876543210")
		boolValue := []byte("true")
		textArrayValue := []byte("{hello,world}")

		testData := utils.CDCMessage{
			Type:   "INSERT",
			Schema: "public",
			Table:  "complex_types",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "binary", DataType: pgtype.ByteaOID},
				{Name: "jsonb", DataType: pgtype.JSONBOID},
				{Name: "timestamp", DataType: pgtype.TimestamptzOID},
				{Name: "float", DataType: pgtype.Float8OID},
				{Name: "integer", DataType: pgtype.Int8OID},
				{Name: "boolean", DataType: pgtype.BoolOID},
				{Name: "text_array", DataType: pgtype.TextArrayOID},
			},
			NewTuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: binaryData},
					{Data: jsonbData},
					{Data: []byte(timestamp.Format(time.RFC3339Nano))},
					{Data: floatValue},
					{Data: intValue},
					{Data: boolValue},
					{Data: textArrayValue},
				},
			},
			OldTuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte{0x05, 0x06, 0x07, 0x08}},
					{Data: []byte(`{"old": "data"}`)},
				},
			},
		}

		encoded, err := testData.MarshalBinary()
		assert.NoError(t, err)

		var decoded utils.CDCMessage
		err = decoded.UnmarshalBinary(encoded)
		assert.NoError(t, err)

		assert.Equal(t, binaryData, decoded.NewTuple.Columns[0].Data)
		assert.Equal(t, jsonbData, decoded.NewTuple.Columns[1].Data)
		assert.Equal(t, []byte(timestamp.Format(time.RFC3339Nano)), decoded.NewTuple.Columns[2].Data)
		assert.Equal(t, floatValue, decoded.NewTuple.Columns[3].Data)
		assert.Equal(t, intValue, decoded.NewTuple.Columns[4].Data)
		assert.Equal(t, boolValue, decoded.NewTuple.Columns[5].Data)
		assert.Equal(t, textArrayValue, decoded.NewTuple.Columns[6].Data)

		assert.Equal(t, []byte{0x05, 0x06, 0x07, 0x08}, decoded.OldTuple.Columns[0].Data)
		assert.Equal(t, []byte(`{"old": "data"}`), decoded.OldTuple.Columns[1].Data)

		assert.Equal(t, testData.Type, decoded.Type)
		assert.Equal(t, testData.Schema, decoded.Schema)
		assert.Equal(t, testData.Table, decoded.Table)
		assert.Equal(t, testData.Columns, decoded.Columns)
	})
}
