package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// ParseTimestamp attempts to parse a timestamp string using multiple layouts
func ParseTimestamp(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999Z07:00",
		"2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05.999999Z",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
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

// ToBool converts a value to bool
func ToBool(v interface{}) bool {
	switch v := v.(type) {
	case bool:
		return v
	case string:
		b, _ := strconv.ParseBool(v)
		return b
	}
	return false
}

// ToByteSlice converts a value to []byte
func ToByteSlice(v interface{}) []byte {
	switch v := v.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	}
	return nil
}

// ToTime converts a value to time.Time
func ToTime(v interface{}) time.Time {
	switch v := v.(type) {
	case time.Time:
		return v
	case string:
		t, err := ParseTimestamp(v)
		if err == nil {
			return t
		}
	}
	return time.Time{}
}

// ToJSON converts a value to json.RawMessage
func ToJSON(v interface{}) json.RawMessage {
	switch v := v.(type) {
	case json.RawMessage:
		return v
	case string:
		return json.RawMessage(v)
	case []byte:
		return json.RawMessage(v)
	default:
		data, _ := json.Marshal(v)
		return json.RawMessage(data)
	}
}

// ToString converts a value to string
func ToString(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
