package utils

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
)

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

func (m *CDCMessage) GetColumnIndex(columnName string) int {
	for i, col := range m.Columns {
		if col.Name == columnName {
			return i
		}
	}
	return -1
}

// GetColumnValue returns the typed value of a column
func (m *CDCMessage) GetColumnValue(columnName string) (interface{}, error) {
	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return nil, fmt.Errorf("column %s not found", columnName)
	}

	column := m.Columns[colIndex]
	data := m.NewTuple.Columns[colIndex].Data

	return DecodeValue(data, column.DataType)
}

// SetColumnValue sets the value of a column, respecting its type
func (m *CDCMessage) SetColumnValue(columnName string, value interface{}) error {
	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return fmt.Errorf("column %s not found", columnName)
	}

	column := m.Columns[colIndex]
	encodedValue, err := EncodeValue(value, column.DataType)
	if err != nil {
		return err
	}

	m.NewTuple.Columns[colIndex].Data = encodedValue
	return nil
}

// EncodeCDCMessage encodes a CDCMessage into a byte slice
func EncodeCDCMessage(m CDCMessage) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	if err := enc.Encode(m.Type); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.Schema); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.Table); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.Columns); err != nil {
		return nil, err
	}

	if err := enc.Encode(m.NewTuple != nil); err != nil {
		return nil, err
	}
	if m.NewTuple != nil {
		if err := enc.Encode(m.NewTuple); err != nil {
			return nil, err
		}
	}

	if err := enc.Encode(m.OldTuple != nil); err != nil {
		return nil, err
	}
	if m.OldTuple != nil {
		if err := enc.Encode(m.OldTuple); err != nil {
			return nil, err
		}
	}

	if err := enc.Encode(m.PrimaryKeyColumn); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.LSN); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.CommitTimestamp); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecodeCDCMessage decodes a byte slice into a CDCMessage
func DecodeCDCMessage(data []byte) (*CDCMessage, error) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	m := &CDCMessage{}

	if err := dec.Decode(&m.Type); err != nil {
		return nil, err
	}
	if err := dec.Decode(&m.Schema); err != nil {
		return nil, err
	}
	if err := dec.Decode(&m.Table); err != nil {
		return nil, err
	}
	if err := dec.Decode(&m.Columns); err != nil {
		return nil, err
	}

	var newTupleExists bool
	if err := dec.Decode(&newTupleExists); err != nil {
		return nil, err
	}
	if newTupleExists {
		m.NewTuple = &pglogrepl.TupleData{}
		if err := dec.Decode(m.NewTuple); err != nil {
			return nil, err
		}
	}

	var oldTupleExists bool
	if err := dec.Decode(&oldTupleExists); err != nil {
		return nil, err
	}
	if oldTupleExists {
		m.OldTuple = &pglogrepl.TupleData{}
		if err := dec.Decode(m.OldTuple); err != nil {
			return nil, err
		}
	}

	if err := dec.Decode(&m.PrimaryKeyColumn); err != nil {
		return nil, err
	}
	if err := dec.Decode(&m.LSN); err != nil {
		return nil, err
	}
	if err := dec.Decode(&m.CommitTimestamp); err != nil {
		return nil, err
	}

	return m, nil
}

// DecodeValue decodes a byte slice into a Go value based on the PostgreSQL data type
func DecodeValue(data []byte, dataType uint32) (interface{}, error) {
	if data == nil {
		return nil, nil
	}

	switch dataType {
	case pgtype.BoolOID:
		return strconv.ParseBool(string(data))
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		return strconv.ParseInt(string(data), 10, 64)
	case pgtype.Float4OID, pgtype.Float8OID:
		return strconv.ParseFloat(string(data), 64)
	case pgtype.NumericOID:
		return string(data), nil
	case pgtype.TextOID, pgtype.VarcharOID:
		return string(data), nil
	case pgtype.ByteaOID:
		return data, nil
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return ParseTimestamp(string(data))
	case pgtype.DateOID:
		return time.Parse("2006-01-02", string(data))
	case pgtype.JSONOID, pgtype.JSONBOID:
		var result interface{}
		err := json.Unmarshal(data, &result)
		return result, err
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID:
		return DecodeTextArray(data)
	case pgtype.Int2ArrayOID, pgtype.Int4ArrayOID, pgtype.Int8ArrayOID, pgtype.Float4ArrayOID, pgtype.Float8ArrayOID, pgtype.BoolArrayOID:
		return DecodeArray(data, dataType)
	default:
		return string(data), nil
	}
}

// DecodeTextArray decodes a PostgreSQL text array into a []string
func DecodeTextArray(data []byte) ([]string, error) {
	if len(data) < 2 || data[0] != '{' || data[len(data)-1] != '}' {
		return nil, fmt.Errorf("invalid array format")
	}
	elements := strings.Split(string(data[1:len(data)-1]), ",")
	for i, elem := range elements {
		elements[i] = strings.Trim(elem, "\"")
	}
	return elements, nil
}

// DecodeArray decodes a PostgreSQL array into a slice of the appropriate type
func DecodeArray(data []byte, dataType uint32) (interface{}, error) {
	if len(data) < 2 || data[0] != '{' || data[len(data)-1] != '}' {
		return nil, fmt.Errorf("invalid array format")
	}
	elements := strings.Split(string(data[1:len(data)-1]), ",")

	switch dataType {
	case pgtype.Int2ArrayOID, pgtype.Int4ArrayOID, pgtype.Int8ArrayOID:
		result := make([]interface{}, len(elements))
		for i, elem := range elements {
			if elem == "NULL" {
				result[i] = nil
			} else {
				val, err := strconv.ParseInt(elem, 10, 64)
				if err != nil {
					return nil, err
				}
				result[i] = val
			}
		}
		return result, nil
	case pgtype.Float4ArrayOID, pgtype.Float8ArrayOID:
		result := make([]interface{}, len(elements))
		for i, elem := range elements {
			if elem == "NULL" {
				result[i] = nil
			} else {
				val, err := strconv.ParseFloat(elem, 64)
				if err != nil {
					return nil, err
				}
				result[i] = val
			}
		}
		return result, nil
	case pgtype.BoolArrayOID:
		result := make([]interface{}, len(elements))
		for i, elem := range elements {
			if elem == "NULL" {
				result[i] = nil
			} else {
				val, err := strconv.ParseBool(elem)
				if err != nil {
					return nil, err
				}
				result[i] = val
			}
		}
		return result, nil
	default:
		return elements, nil
	}
}

// EncodeValue encodes a Go value into a byte slice based on the PostgreSQL data type
func EncodeValue(value interface{}, dataType uint32) ([]byte, error) {
	return ConvertToPgOutput(value, dataType)
}

// GetDecodedColumnValue returns the decoded value of a column
func (m *CDCMessage) GetDecodedColumnValue(columnName string) (interface{}, error) {
	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return nil, fmt.Errorf("column %s not found", columnName)
	}

	var data []byte
	if m.NewTuple != nil && colIndex < len(m.NewTuple.Columns) {
		data = m.NewTuple.Columns[colIndex].Data
	} else if m.OldTuple != nil && colIndex < len(m.OldTuple.Columns) {
		data = m.OldTuple.Columns[colIndex].Data
	} else {
		return nil, fmt.Errorf("no data available for column %s", columnName)
	}

	column := m.Columns[colIndex]
	return DecodeValue(data, column.DataType)
}

// GetDecodedMessage returns a map with decoded column values
func (m *CDCMessage) GetDecodedMessage() (map[string]interface{}, error) {
	decodedMessage := make(map[string]interface{})
	decodedMessage["Type"] = m.Type
	decodedMessage["Schema"] = m.Schema
	decodedMessage["Table"] = m.Table
	decodedMessage["PrimaryKeyColumn"] = m.PrimaryKeyColumn
	decodedMessage["LSN"] = m.LSN
	decodedMessage["CommitTimestamp"] = m.CommitTimestamp

	if m.NewTuple != nil {
		newTuple := make(map[string]interface{})
		for _, col := range m.Columns {
			value, err := m.GetDecodedColumnValue(col.Name)
			if err != nil {
				return nil, fmt.Errorf("failed to decode column %s: %v", col.Name, err)
			}
			newTuple[col.Name] = value
		}
		decodedMessage["NewTuple"] = newTuple
	}

	if m.OldTuple != nil {
		oldTuple := make(map[string]interface{})
		for _, col := range m.Columns {
			value, err := m.GetDecodedColumnValue(col.Name)
			if err != nil {
				return nil, fmt.Errorf("failed to decode column %s: %v", col.Name, err)
			}
			oldTuple[col.Name] = value
		}
		decodedMessage["OldTuple"] = oldTuple
	}

	return decodedMessage, nil
}
