package replicator_test

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/goccy/go-json"
	"github.com/jackc/pgtype"
	"github.com/shayonj/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestCDCEncoder(t *testing.T) {
	t.Run("Encode preserves CDC types", func(t *testing.T) {
		var buf bytes.Buffer
		encoder := utils.NewCDCEncoder(&buf)

		testData := map[string]interface{}{
			"int":     123,
			"float":   3.14,
			"string":  "hello",
			"bool":    true,
			"null":    nil,
			"cdcInt":  utils.CDCValue{Type: pgtype.Int4OID, Value: 42},
			"cdcText": utils.CDCValue{Type: pgtype.TextOID, Value: "world"},
		}

		err := encoder.Encode(testData)
		assert.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(buf.Bytes(), &result)
		assert.NoError(t, err)

		assert.Equal(t, float64(123), result["int"])
		assert.Equal(t, 3.14, result["float"])
		assert.Equal(t, "hello", result["string"])
		assert.Equal(t, true, result["bool"])
		assert.Nil(t, result["null"])

		cdcInt := result["cdcInt"].(map[string]interface{})
		assert.Equal(t, "int4", cdcInt["type"])
		assert.Equal(t, float64(42), cdcInt["value"])

		cdcText := result["cdcText"].(map[string]interface{})
		assert.Equal(t, "text", cdcText["type"])
		assert.Equal(t, "world", cdcText["value"])
	})

	t.Run("MarshalJSON function", func(t *testing.T) {
		testData := map[string]interface{}{
			"int":    123,
			"float":  3.14,
			"string": "hello",
		}

		result, err := utils.MarshalJSON(testData)
		assert.NoError(t, err)

		expected := `{"float":3.14,"int":123,"string":"hello"}`
		assert.JSONEq(t, expected, string(result))
	})

	t.Run("OIDToString function", func(t *testing.T) {
		assert.Equal(t, "int4", utils.OIDToString(pgtype.Int4OID))
		assert.Equal(t, "text", utils.OIDToString(pgtype.TextOID))
		assert.Equal(t, "unknown_99999", utils.OIDToString(99999))
	})

	t.Run("Encode preserves original data types", func(t *testing.T) {
		var buf bytes.Buffer
		encoder := utils.NewCDCEncoder(&buf)

		testData := map[string]interface{}{
			"int":         123,
			"float":       3.14,
			"string":      "hello",
			"boolTrue":    true,
			"boolFalse":   false,
			"null":        nil,
			"intString":   "12345",
			"floatString": "3.14159",
		}

		err := encoder.Encode(testData)
		assert.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(buf.Bytes(), &result)
		assert.NoError(t, err)

		assert.Equal(t, float64(123), result["int"])
		assert.Equal(t, 3.14, result["float"])
		assert.Equal(t, "hello", result["string"])
		assert.Equal(t, true, result["boolTrue"])
		assert.Equal(t, false, result["boolFalse"])
		assert.Nil(t, result["null"])
		assert.Equal(t, "12345", result["intString"])
		assert.Equal(t, "3.14159", result["floatString"])
	})

	t.Run("Encode handles nested structures", func(t *testing.T) {
		var buf bytes.Buffer
		encoder := utils.NewCDCEncoder(&buf)

		testData := map[string]interface{}{
			"nested": map[string]interface{}{
				"int":   42,
				"float": 3.14,
				"bool":  true,
			},
		}

		err := encoder.Encode(testData)
		assert.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(buf.Bytes(), &result)
		assert.NoError(t, err)

		nested := result["nested"].(map[string]interface{})
		assert.Equal(t, float64(42), nested["int"])
		assert.Equal(t, 3.14, nested["float"])
		assert.Equal(t, true, nested["bool"])
	})

	t.Run("Encode handles bytea and JSONB types", func(t *testing.T) {
		var buf bytes.Buffer
		encoder := utils.NewCDCEncoder(&buf)

		binaryData := []byte{0x01, 0x02, 0x03, 0x04}
		jsonbData := map[string]interface{}{
			"key": "value",
			"nested": map[string]interface{}{
				"number": float64(42), // Change this to float64
			},
		}

		testData := map[string]interface{}{
			"binary": utils.CDCValue{Type: pgtype.ByteaOID, Value: binaryData},
			"jsonb":  utils.CDCValue{Type: pgtype.JSONBOID, Value: jsonbData},
		}

		err := encoder.Encode(testData)
		assert.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(buf.Bytes(), &result)
		assert.NoError(t, err)

		// Check binary data
		binaryResult := result["binary"].(map[string]interface{})
		assert.Equal(t, "bytea", binaryResult["type"])
		decodedBinary, err := base64.StdEncoding.DecodeString(binaryResult["value"].(string))
		assert.NoError(t, err)
		assert.Equal(t, binaryData, decodedBinary)

		// Check JSONB data
		jsonbResult := result["jsonb"].(map[string]interface{})
		assert.Equal(t, "jsonb", jsonbResult["type"])
		assert.Equal(t, jsonbData, jsonbResult["value"])
	})

	t.Run("Encode handles raw bytea data", func(t *testing.T) {
		var buf bytes.Buffer
		encoder := utils.NewCDCEncoder(&buf)

		binaryData := []byte{0x01, 0x02, 0x03, 0x04}

		testData := map[string]interface{}{
			"raw_binary": binaryData,
		}

		err := encoder.Encode(testData)
		assert.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(buf.Bytes(), &result)
		assert.NoError(t, err)

		// Check raw binary data
		decodedBinary, err := base64.StdEncoding.DecodeString(result["raw_binary"].(string))
		assert.NoError(t, err)
		assert.Equal(t, binaryData, decodedBinary)
	})
}
