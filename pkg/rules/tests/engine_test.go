package rules_test

import (
	"log"
	"os"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	log.SetOutput(os.Stdout)
	os.Exit(m.Run())
}

func TestRuleEngine_AddRule(t *testing.T) {
	re := rules.NewRuleEngine()
	rule := &MockRule{
		TableName:  "users",
		ColumnName: "test_column",
		ApplyFunc: func(data map[string]utils.CDCValue, operation rules.OperationType) (map[string]utils.CDCValue, error) {
			return data, nil
		},
	}
	re.AddRule("users", rule)

	result, err := re.ApplyRules("users", map[string]utils.CDCValue{}, rules.OperationInsert)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestRuleEngine_ApplyRules(t *testing.T) {
	re := rules.NewRuleEngine()
	rule := &MockRule{
		TableName:  "users",
		ColumnName: "test_column",
		ApplyFunc: func(data map[string]utils.CDCValue, operation rules.OperationType) (map[string]utils.CDCValue, error) {
			data["test_column"] = utils.CDCValue{Type: pgtype.TextOID, Value: "transformed"}
			return data, nil
		},
	}
	re.AddRule("users", rule)

	input := map[string]utils.CDCValue{
		"test_column": {Type: pgtype.TextOID, Value: "original"},
	}

	result, err := re.ApplyRules("users", input, rules.OperationInsert)

	assert.NoError(t, err)
	assert.Equal(t, "transformed", result["test_column"].Value)
}

func TestRuleEngine_ApplyRules_NoRules(t *testing.T) {
	re := rules.NewRuleEngine()
	input := map[string]utils.CDCValue{
		"test_column": {Type: pgtype.TextOID, Value: "original"},
	}

	result, err := re.ApplyRules("users", input, rules.OperationInsert)

	assert.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestRuleEngine_LoadRules(t *testing.T) {
	re := rules.NewRuleEngine()
	config := rules.Config{
		Tables: map[string][]rules.RuleConfig{
			"users": {
				{
					Type:   "transform",
					Column: "test_column",
					Parameters: map[string]interface{}{
						"type":      "mask",
						"mask_char": "*",
					},
					Operations: []rules.OperationType{rules.OperationInsert, rules.OperationUpdate},
				},
				{
					Type:   "filter",
					Column: "id",
					Parameters: map[string]interface{}{
						"operator": "gt",
						"value":    100,
					},
					Operations: []rules.OperationType{rules.OperationDelete},
				},
			},
		},
	}

	err := re.LoadRules(config)
	assert.NoError(t, err)

	input := map[string]utils.CDCValue{
		"test_column": {Type: pgtype.TextOID, Value: "test"},
		"id":          {Type: pgtype.Int8OID, Value: int64(101)},
	}

	// Test INSERT operation
	result, err := re.ApplyRules("users", input, rules.OperationInsert)

	assert.NoError(t, err)
	assert.NotNil(t, result, "Result should not be nil")
	if result != nil {
		assert.Equal(t, "t**t", result["test_column"].Value, "Masked value should be 't**t'")
		assert.Equal(t, int64(101), result["id"].Value, "ID should be 101")
	}

	// Test DELETE operation
	result, err = re.ApplyRules("users", input, rules.OperationDelete)

	assert.NoError(t, err)
	assert.NotNil(t, result, "Result should not be nil")
	if result != nil {
		assert.Equal(t, "t**t", result["test_column"].Value, "Value should not be masked for DELETE")
		assert.Equal(t, int64(101), result["id"].Value, "ID should be 101")
	}

	// Test with a value that doesn't pass the filter for DELETE
	input["id"] = utils.CDCValue{Type: pgtype.Int8OID, Value: int64(99)}
	result, err = re.ApplyRules("users", input, rules.OperationDelete)

	assert.NoError(t, err)
	assert.Nil(t, result, "Result should be nil when filter doesn't pass for DELETE")
}

func TestRuleEngine_ApplyRules_FilterRule(t *testing.T) {
	re := rules.NewRuleEngine()
	config := rules.Config{
		Tables: map[string][]rules.RuleConfig{
			"users": {
				{
					Type:   "filter",
					Column: "id",
					Parameters: map[string]interface{}{
						"operator": "gt",
						"value":    100,
					},
					Operations: []rules.OperationType{rules.OperationUpdate},
				},
			},
		},
	}

	err := re.LoadRules(config)
	assert.NoError(t, err)

	// Test with a value that passes the filter for UPDATE
	input := map[string]utils.CDCValue{
		"id": {Type: pgtype.Int8OID, Value: int64(101)},
	}
	result, err := re.ApplyRules("users", input, rules.OperationUpdate)

	assert.NoError(t, err)
	assert.NotNil(t, result, "Result should not be nil when filter passes for UPDATE")
	if result != nil {
		assert.Equal(t, int64(101), result["id"].Value, "ID should be 101")
	}

	// Test with a value that doesn't pass the filter for UPDATE
	input = map[string]utils.CDCValue{
		"id": {Type: pgtype.Int8OID, Value: int64(99)},
	}
	result, err = re.ApplyRules("users", input, rules.OperationUpdate)

	assert.NoError(t, err)
	assert.Nil(t, result, "Result should be nil when filter doesn't pass for UPDATE")

	// Test with a value that passes the filter but for INSERT (should not apply)
	input = map[string]utils.CDCValue{
		"id": {Type: pgtype.Int8OID, Value: int64(101)},
	}
	result, err = re.ApplyRules("users", input, rules.OperationInsert)

	assert.NoError(t, err)
	assert.NotNil(t, result, "Result should not be nil for INSERT operation")
	if result != nil {
		assert.Equal(t, int64(101), result["id"].Value, "ID should be 101")
	}
}
