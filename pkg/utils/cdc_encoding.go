package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgtype"
)

// ConvertToPgOutput converts a Go value to its PostgreSQL output format
func ConvertToPgOutput(value interface{}, oid uint32) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	switch oid {
	case pgtype.BoolOID:
		return []byte(strconv.FormatBool(value.(bool))), nil
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		return []byte(fmt.Sprintf("%d", value)), nil
	case pgtype.Float4OID, pgtype.Float8OID:
		return []byte(fmt.Sprintf("%g", value)), nil
	case pgtype.NumericOID:
		return []byte(value.(string)), nil
	case pgtype.TextOID, pgtype.VarcharOID:
		return []byte(value.(string)), nil
	case pgtype.ByteaOID:
		return []byte(fmt.Sprintf("\\x%x", value.([]byte))), nil
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return []byte(value.(time.Time).Format(time.RFC3339Nano)), nil
	case pgtype.DateOID:
		return []byte(value.(time.Time).Format("2006-01-02")), nil
	case pgtype.JSONOID, pgtype.JSONBOID:
		if b, ok := value.([]byte); ok {
			return b, nil
		}
		return json.Marshal(value)
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID:
		return EncodeTextArray(value)
	case pgtype.Int2ArrayOID, pgtype.Int4ArrayOID, pgtype.Int8ArrayOID, pgtype.Float4ArrayOID, pgtype.Float8ArrayOID, pgtype.BoolArrayOID:
		return EncodeArray(value)
	default:
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}

// EncodeTextArray encodes a slice of strings into a PostgreSQL text array format
func EncodeTextArray(value interface{}) ([]byte, error) {
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected slice, got %T", value)
	}

	var elements []string
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()
		str, ok := elem.(string)
		if !ok {
			return nil, fmt.Errorf("expected string element in text array, got %T", elem)
		}
		elements = append(elements, QuoteArrayElement(str))
	}

	return []byte("{" + strings.Join(elements, ",") + "}"), nil
}

// EncodeArray encodes a slice of values into a PostgreSQL array format
func EncodeArray(value interface{}) ([]byte, error) {
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected slice, got %T", value)
	}

	var elements []string
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()
		if elem == nil {
			elements = append(elements, "NULL")
		} else {
			elements = append(elements, fmt.Sprintf("%v", elem))
		}
	}

	return []byte("{" + strings.Join(elements, ",") + "}"), nil
}

// QuoteArrayElement quotes a string element for use in a PostgreSQL array
func QuoteArrayElement(s string) string {
	if strings.ContainsAny(s, `{},"\`) {
		s = strings.ReplaceAll(s, `\`, `\\`)
		s = strings.ReplaceAll(s, `"`, `\"`)
		return `"` + s + `"`
	}
	return s
}
