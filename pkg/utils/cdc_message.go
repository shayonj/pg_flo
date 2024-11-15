package utils

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

func init() {
	gob.Register(time.Time{})
	gob.Register(pglogrepl.RelationMessageColumn{})
	gob.Register(pglogrepl.LSN(0))
	gob.Register(CDCMessage{})
	gob.Register(PostgresValue{})
	gob.Register([]byte{})
	gob.Register("")
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(bool(false))
	gob.Register(map[string]interface{}{})
	gob.Register(map[string]*PostgresValue{})
	gob.Register(map[string]bool{})
	gob.Register([]interface{}{})
	gob.Register([]string{})
	gob.Register([]int{})
	gob.Register([]int64{})
	gob.Register([]float64{})
	gob.Register([]bool{})
}

type CDCMessage struct {
	Type                   OperationType
	Schema                 string
	Table                  string
	Columns                []*pglogrepl.RelationMessageColumn
	NewValues              map[string]*PostgresValue
	OldValues              map[string]*PostgresValue
	PrimaryKeyColumn       string
	LSN                    string
	EmittedAt              time.Time
	ToastedColumns         map[string]bool
	MappedPrimaryKeyColumn string
}

func (m *CDCMessage) GetColumnIndex(columnName string) int {
	for i, col := range m.Columns {
		if col.Name == columnName {
			return i
		}
	}
	return -1
}

func (m *CDCMessage) GetDecodedColumnValue(columnName string) (interface{}, error) {
	if val, ok := m.NewValues[columnName]; ok {
		return val.Get(), nil
	}
	if val, ok := m.OldValues[columnName]; ok {
		return val.Get(), nil
	}
	return nil, fmt.Errorf("no data available for column %s", columnName)
}

func (m *CDCMessage) GetDecodedMessage() (map[string]interface{}, error) {
	result := map[string]interface{}{
		"Type":             m.Type,
		"Schema":           m.Schema,
		"Table":            m.Table,
		"PrimaryKeyColumn": m.PrimaryKeyColumn,
		"LSN":              m.LSN,
		"EmittedAt":        m.EmittedAt,
	}

	if m.NewValues != nil {
		newValues := make(map[string]interface{})
		for colName, val := range m.NewValues {
			newValues[colName] = val.Get()
		}
		result["NewValues"] = newValues
	}

	if m.OldValues != nil {
		oldValues := make(map[string]interface{})
		for colName, val := range m.OldValues {
			oldValues[colName] = val.Get()
		}
		result["OldValues"] = oldValues
	}

	return result, nil
}

func (m *CDCMessage) SetColumnValue(columnName string, value interface{}) error {
	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return fmt.Errorf("column %s not found", columnName)
	}

	oid := m.Columns[colIndex].DataType
	pgValue, err := NewPostgresValue(oid, value)
	if err != nil {
		return fmt.Errorf("failed to create PostgresValue: %w", err)
	}

	m.NewValues[columnName] = pgValue
	return nil
}

// MarshalBinary implements encoding.BinaryMarshaler
func (m *CDCMessage) MarshalBinary() ([]byte, error) {
	// Create a simplified version of CDCMessage without methods
	type CDCMessageData struct {
		Type                   OperationType
		Schema                 string
		Table                  string
		Columns                []*pglogrepl.RelationMessageColumn
		NewValues              map[string]*PostgresValue
		OldValues              map[string]*PostgresValue
		PrimaryKeyColumn       string
		LSN                    string
		EmittedAt              time.Time
		ToastedColumns         map[string]bool
		MappedPrimaryKeyColumn string
	}

	data := CDCMessageData{
		Type:                   m.Type,
		Schema:                 m.Schema,
		Table:                  m.Table,
		Columns:                m.Columns,
		NewValues:              m.NewValues,
		OldValues:              m.OldValues,
		PrimaryKeyColumn:       m.PrimaryKeyColumn,
		LSN:                    m.LSN,
		EmittedAt:              m.EmittedAt,
		ToastedColumns:         m.ToastedColumns,
		MappedPrimaryKeyColumn: m.MappedPrimaryKeyColumn,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return nil, fmt.Errorf("failed to encode message: %w", err)
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (m *CDCMessage) UnmarshalBinary(b []byte) error {
	type CDCMessageData struct {
		Type                   OperationType
		Schema                 string
		Table                  string
		Columns                []*pglogrepl.RelationMessageColumn
		NewValues              map[string]*PostgresValue
		OldValues              map[string]*PostgresValue
		PrimaryKeyColumn       string
		LSN                    string
		EmittedAt              time.Time
		ToastedColumns         map[string]bool
		MappedPrimaryKeyColumn string
	}

	var data CDCMessageData
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&data); err != nil {
		return fmt.Errorf("failed to decode message: %w", err)
	}

	m.Type = data.Type
	m.Schema = data.Schema
	m.Table = data.Table
	m.Columns = data.Columns
	m.NewValues = data.NewValues
	m.OldValues = data.OldValues
	m.PrimaryKeyColumn = data.PrimaryKeyColumn
	m.LSN = data.LSN
	m.EmittedAt = data.EmittedAt
	m.ToastedColumns = data.ToastedColumns
	m.MappedPrimaryKeyColumn = data.MappedPrimaryKeyColumn

	return nil
}
