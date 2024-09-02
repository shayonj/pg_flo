package utils

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/jackc/pgtype"
)

// ParseTimestamp attempts to parse a timestamp string using multiple layouts
func ParseTimestamp(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", value)
}

// ToInt64 converts an interface{} to int64
func ToInt64(v interface{}) (int64, bool) {
	switch v := v.(type) {
	case int, int8, int16, int32, int64:
		return reflect.ValueOf(v).Int(), true
	case uint, uint8, uint16, uint32, uint64:
		return int64(reflect.ValueOf(v).Uint()), true
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, true
		}
	}
	return 0, false
}

// ToFloat64 converts an interface{} to float64
func ToFloat64(v interface{}) (float64, bool) {
	switch v := v.(type) {
	case int, int8, int16, int32, int64:
		return float64(reflect.ValueOf(v).Int()), true
	case uint, uint8, uint16, uint32, uint64:
		return float64(reflect.ValueOf(v).Uint()), true
	case float32, float64:
		return reflect.ValueOf(v).Float(), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// NewCDCEncoder creates a new CDCEncoder
func NewCDCEncoder(w interface{ Write([]byte) (int, error) }) *CDCEncoder {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return &CDCEncoder{encoder: encoder}
}

// MarshalJSON is a helper function to use the CDCEncoder for marshaling
func MarshalJSON(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := NewCDCEncoder(&buf)
	err := encoder.Encode(v)
	if err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// CDCEncoder is a custom JSON encoder that preserves CDC data types
type CDCEncoder struct {
	encoder *json.Encoder
}

// Encode encodes the value v and writes it into the stream
func (e *CDCEncoder) Encode(v interface{}) error {
	return e.encoder.Encode(preserveCDCTypes(v))
}

// preserveCDCTypes recursively processes the value and preserves CDC data types
func preserveCDCTypes(v interface{}) interface{} {
	switch v := v.(type) {
	case map[string]interface{}:
		for k, val := range v {
			v[k] = preserveCDCTypes(val)
		}
	case []interface{}:
		for i, val := range v {
			v[i] = preserveCDCTypes(val)
		}
	case float64:
		return v
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return v
	case string:
		return v
	case bool:
		return v
	case nil:
		return nil
	case CDCValue:
		switch v.Type {
		case pgtype.ByteaOID:
			// Encode binary data as base64
			return CDCValue{
				Type:  v.Type,
				Value: base64.StdEncoding.EncodeToString(v.Value.([]byte)),
			}
		case pgtype.JSONBOID, pgtype.Int4ArrayOID, pgtype.TextArrayOID, pgtype.NumericOID:
			// These types can be returned as-is
			return v
		default:
			return v
		}
	case []byte:
		// Encode binary data as base64
		return base64.StdEncoding.EncodeToString(v)
	}
	return v
}
