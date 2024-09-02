package rules_test

import (
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestTransformRules(t *testing.T) {
	tests := []struct {
		name           string
		rule           rules.Rule
		input          map[string]utils.CDCValue
		operation      rules.OperationType
		expectedOutput map[string]utils.CDCValue
	}{
		{
			name: "Regex Transform - Email Domain",
			rule: createRule(t, "transform", "users", "email", map[string]interface{}{
				"type":    "regex",
				"pattern": "@example\\.com$",
				"replace": "@masked.com",
			}),
			input: map[string]utils.CDCValue{
				"email": {Type: pgtype.TextOID, Value: "user@example.com"},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"email": {Type: pgtype.TextOID, Value: "user@masked.com"},
			},
		},
		{
			name: "Regex Transform - Email Domain",
			rule: createRule(t, "transform", "users", "email", map[string]interface{}{
				"type":                "regex",
				"pattern":             "@example\\.com$",
				"replace":             "@masked.com",
				"allow_empty_deletes": true,
			}),
			input: map[string]utils.CDCValue{
				"id":    {Type: pgtype.Int2OID, Value: 1},
				"email": {Type: pgtype.TextOID, Value: nil},
			},
			operation: rules.OperationDelete,
			expectedOutput: map[string]utils.CDCValue{
				"id":    {Type: pgtype.Int2OID, Value: 1},
				"email": {Type: pgtype.TextOID, Value: nil},
			},
		},
		{
			name: "Regex Transform - Phone Number Format",
			rule: createRule(t, "transform", "users", "phone", map[string]interface{}{
				"type":    "regex",
				"pattern": "(\\d{3})(\\d{3})(\\d{4})",
				"replace": "($1) $2-$3",
			}),
			input: map[string]utils.CDCValue{
				"phone": {Type: pgtype.TextOID, Value: "1234567890"},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"phone": {Type: pgtype.TextOID, Value: "(123) 456-7890"},
			},
		},
		{
			name: "Mask Transform - Full Mask",
			rule: createRule(t, "transform", "users", "ssn", map[string]interface{}{
				"type":      "mask",
				"mask_char": "*",
			}),
			input: map[string]utils.CDCValue{
				"ssn": {Type: pgtype.TextOID, Value: "123-45-6789"},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"ssn": {Type: pgtype.TextOID, Value: "1*********9"},
			},
		},
		{
			name: "Mask Transform - Partial Mask",
			rule: createRule(t, "transform", "users", "credit_card", map[string]interface{}{
				"type":      "mask",
				"mask_char": "X",
			}),
			input: map[string]utils.CDCValue{
				"credit_card": {Type: pgtype.TextOID, Value: "1234-5678-9012-3456"},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"credit_card": {Type: pgtype.TextOID, Value: "1XXXXXXXXXXXXXXXXX6"},
			},
		},
		{
			name: "Regex Transform - No Match",
			rule: createRule(t, "transform", "users", "username", map[string]interface{}{
				"type":    "regex",
				"pattern": "admin",
				"replace": "user",
			}),
			input: map[string]utils.CDCValue{
				"username": {Type: pgtype.TextOID, Value: "john_doe"},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"username": {Type: pgtype.TextOID, Value: "john_doe"},
			},
		},
		{
			name: "Mask Transform - Short String",
			rule: createRule(t, "transform", "users", "initials", map[string]interface{}{
				"type":      "mask",
				"mask_char": "*",
			}),
			input: map[string]utils.CDCValue{
				"initials": {Type: pgtype.TextOID, Value: "JD"},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"initials": {Type: pgtype.TextOID, Value: "JD"},
			},
		},
		{
			name: "Regex Transform - Email Domain (INSERT operation)",
			rule: createRule(t, "transform", "users", "email", map[string]interface{}{
				"type":       "regex",
				"pattern":    "@example\\.com$",
				"replace":    "@masked.com",
				"operations": []rules.OperationType{rules.OperationInsert},
			}),
			input: map[string]utils.CDCValue{
				"email": {Type: pgtype.TextOID, Value: "user@example.com"},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"email": {Type: pgtype.TextOID, Value: "user@masked.com"},
			},
		},
		{
			name: "Regex Transform - Email Domain (UPDATE operation, not applied)",
			rule: createRule(t, "transform", "users", "email", map[string]interface{}{
				"type":       "regex",
				"pattern":    "@example\\.com$",
				"replace":    "@masked.com",
				"operations": []rules.OperationType{rules.OperationInsert},
			}),
			input: map[string]utils.CDCValue{
				"email": {Type: pgtype.TextOID, Value: "user@example.com"},
			},
			operation: rules.OperationUpdate,
			expectedOutput: map[string]utils.CDCValue{
				"email": {Type: pgtype.TextOID, Value: "user@example.com"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := tt.rule.Apply(tt.input, tt.operation)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func TestFilterRules(t *testing.T) {
	tests := []struct {
		name           string
		rule           rules.Rule
		input          map[string]utils.CDCValue
		operation      rules.OperationType
		expectedOutput map[string]utils.CDCValue
	}{
		{
			name: "Equal Filter (Text) - Pass",
			rule: createRule(t, "filter", "orders", "status", map[string]interface{}{
				"operator": "eq",
				"value":    "completed",
			}),
			input: map[string]utils.CDCValue{
				"status": {Type: pgtype.TextOID, Value: "completed"},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"status": {Type: pgtype.TextOID, Value: "completed"},
			},
		},
		{
			name: "Equal Filter (Text) - (DELETE - allow empty delete) Pass",
			rule: createRule(t, "filter", "orders", "status", map[string]interface{}{
				"operator":            "eq",
				"value":               "completed",
				"allow_empty_deletes": true,
			}),
			input: map[string]utils.CDCValue{
				"id":     {Type: pgtype.Int8OID, Value: 1},
				"status": {Type: pgtype.Int8OID, Value: nil},
			},
			operation: rules.OperationDelete,
			expectedOutput: map[string]utils.CDCValue{
				"id":     {Type: pgtype.Int8OID, Value: 1},
				"status": {Type: pgtype.Int8OID, Value: nil},
			},
		},
		{
			name: "Equal Filter (Text) - Fail",
			rule: createRule(t, "filter", "orders", "status", map[string]interface{}{
				"operator": "eq",
				"value":    "completed",
			}),
			input: map[string]utils.CDCValue{
				"status": {Type: pgtype.TextOID, Value: "pending"},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Equal Filter (Int2) - Pass",
			rule: createRule(t, "filter", "products", "quantity", map[string]interface{}{
				"operator": "eq",
				"value":    int16(10),
			}),
			input: map[string]utils.CDCValue{
				"quantity": {Type: pgtype.Int2OID, Value: int16(10)},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"quantity": {Type: pgtype.Int2OID, Value: int16(10)},
			},
		},
		{
			name: "Equal Filter (Int2) - Fail",
			rule: createRule(t, "filter", "products", "quantity", map[string]interface{}{
				"operator": "eq",
				"value":    int16(10),
			}),
			input: map[string]utils.CDCValue{
				"quantity": {Type: pgtype.Int2OID, Value: int16(5)},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Greater Than Filter (Int4) - Pass",
			rule: createRule(t, "filter", "products", "stock", map[string]interface{}{
				"operator": "gt",
				"value":    int32(100),
			}),
			input: map[string]utils.CDCValue{
				"stock": {Type: pgtype.Int4OID, Value: int32(150)},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"stock": {Type: pgtype.Int4OID, Value: int32(150)},
			},
		},
		{
			name: "Greater Than Filter (Int4) - Fail",
			rule: createRule(t, "filter", "products", "stock", map[string]interface{}{
				"operator": "gt",
				"value":    int32(100),
			}),
			input: map[string]utils.CDCValue{
				"stock": {Type: pgtype.Int4OID, Value: int32(50)},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Less Than Filter (Int8) - Pass",
			rule: createRule(t, "filter", "orders", "total_amount", map[string]interface{}{
				"operator": "lt",
				"value":    int64(1000),
			}),
			input: map[string]utils.CDCValue{
				"total_amount": {Type: pgtype.Int8OID, Value: int64(500)},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"total_amount": {Type: pgtype.Int8OID, Value: int64(500)},
			},
		},
		{
			name: "Less Than Filter (Int8) - Fail",
			rule: createRule(t, "filter", "orders", "total_amount", map[string]interface{}{
				"operator": "lt",
				"value":    int64(1000),
			}),
			input: map[string]utils.CDCValue{
				"total_amount": {Type: pgtype.Int8OID, Value: int64(1500)},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Greater Than or Equal Filter (Float4) - Pass",
			rule: createRule(t, "filter", "products", "weight", map[string]interface{}{
				"operator": "gte",
				"value":    float32(2.5),
			}),
			input: map[string]utils.CDCValue{
				"weight": {Type: pgtype.Float4OID, Value: float32(3.0)},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"weight": {Type: pgtype.Float4OID, Value: float32(3.0)},
			},
		},
		{
			name: "Greater Than or Equal Filter (Float4) - Fail",
			rule: createRule(t, "filter", "products", "weight", map[string]interface{}{
				"operator": "gte",
				"value":    float32(2.5),
			}),
			input: map[string]utils.CDCValue{
				"weight": {Type: pgtype.Float4OID, Value: float32(2.0)},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Less Than or Equal Filter (Float8) - Pass",
			rule: createRule(t, "filter", "products", "price", map[string]interface{}{
				"operator": "lte",
				"value":    float64(99.99),
			}),
			input: map[string]utils.CDCValue{
				"price": {Type: pgtype.Float8OID, Value: float64(89.99)},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"price": {Type: pgtype.Float8OID, Value: float64(89.99)},
			},
		},
		{
			name: "Less Than or Equal Filter (Float8) - Fail",
			rule: createRule(t, "filter", "products", "price", map[string]interface{}{
				"operator": "lte",
				"value":    float64(99.99),
			}),
			input: map[string]utils.CDCValue{
				"price": {Type: pgtype.Float8OID, Value: float64(109.99)},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Not Equal Filter (Boolean) - Pass",
			rule: createRule(t, "filter", "users", "is_active", map[string]interface{}{
				"operator": "ne",
				"value":    false,
			}),
			input: map[string]utils.CDCValue{
				"is_active": {Type: pgtype.BoolOID, Value: true},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"is_active": {Type: pgtype.BoolOID, Value: true},
			},
		},
		{
			name: "Not Equal Filter (Boolean) - Fail",
			rule: createRule(t, "filter", "users", "is_active", map[string]interface{}{
				"operator": "ne",
				"value":    false,
			}),
			input: map[string]utils.CDCValue{
				"is_active": {Type: pgtype.BoolOID, Value: false},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Greater Than Filter (Timestamp) - Pass",
			rule: createRule(t, "filter", "events", "created_at", map[string]interface{}{
				"operator": "gt",
				"value":    "2023-01-01T00:00:00Z",
			}),
			input: map[string]utils.CDCValue{
				"created_at": {Type: pgtype.TimestampOID, Value: time.Date(2023, 6, 1, 12, 0, 0, 0, time.UTC)},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"created_at": {Type: pgtype.TimestampOID, Value: time.Date(2023, 6, 1, 12, 0, 0, 0, time.UTC)},
			},
		},
		{
			name: "Greater Than Filter (Timestamp) - Fail",
			rule: createRule(t, "filter", "events", "created_at", map[string]interface{}{
				"operator": "gt",
				"value":    "2023-01-01T00:00:00Z",
			}),
			input: map[string]utils.CDCValue{
				"created_at": {Type: pgtype.TimestampOID, Value: time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC)},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Less Than Filter (TimestampTZ) - Pass",
			rule: createRule(t, "filter", "logs", "timestamp", map[string]interface{}{
				"operator": "lt",
				"value":    "2023-12-31T23:59:59Z",
			}),
			input: map[string]utils.CDCValue{
				"timestamp": {Type: pgtype.TimestamptzOID, Value: time.Date(2023, 6, 1, 12, 0, 0, 0, time.UTC)},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"timestamp": {Type: pgtype.TimestamptzOID, Value: time.Date(2023, 6, 1, 12, 0, 0, 0, time.UTC)},
			},
		},
		{
			name: "Less Than Filter (TimestampTZ) - Fail",
			rule: createRule(t, "filter", "logs", "timestamp", map[string]interface{}{
				"operator": "lt",
				"value":    "2023-12-31T23:59:59Z",
			}),
			input: map[string]utils.CDCValue{
				"timestamp": {Type: pgtype.TimestamptzOID, Value: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Equal Filter (Numeric) - Pass",
			rule: createRule(t, "filter", "products", "rating", map[string]interface{}{
				"operator": "eq",
				"value":    "4.5",
			}),
			input: map[string]utils.CDCValue{
				"rating": {Type: pgtype.NumericOID, Value: "4.5"},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"rating": {Type: pgtype.NumericOID, Value: "4.5"},
			},
		},
		{
			name: "Equal Filter (Numeric) - Fail",
			rule: createRule(t, "filter", "products", "rating", map[string]interface{}{
				"operator": "eq",
				"value":    "4.5",
			}),
			input: map[string]utils.CDCValue{
				"rating": {Type: pgtype.NumericOID, Value: "3.8"},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Contains Filter (Text) - Pass",
			rule: createRule(t, "filter", "products", "description", map[string]interface{}{
				"operator": "contains",
				"value":    "organic",
			}),
			input: map[string]utils.CDCValue{
				"description": {Type: pgtype.TextOID, Value: "Fresh organic apples"},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"description": {Type: pgtype.TextOID, Value: "Fresh organic apples"},
			},
		},
		{
			name: "Contains Filter (Text) - Fail",
			rule: createRule(t, "filter", "products", "description", map[string]interface{}{
				"operator": "contains",
				"value":    "organic",
			}),
			input: map[string]utils.CDCValue{
				"description": {Type: pgtype.TextOID, Value: "Fresh apples"},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Equal Filter (Text) - Pass (DELETE operation)",
			rule: createRule(t, "filter", "orders", "status", map[string]interface{}{
				"operator":   "eq",
				"value":      "completed",
				"operations": []rules.OperationType{rules.OperationDelete},
			}),
			input: map[string]utils.CDCValue{
				"status": {Type: pgtype.TextOID, Value: "completed"},
			},
			operation: rules.OperationDelete,
			expectedOutput: map[string]utils.CDCValue{
				"status": {Type: pgtype.TextOID, Value: "completed"},
			},
		},
		{
			name: "Equal Filter (Text) - Not Applied (UPDATE operation)",
			rule: createRule(t, "filter", "orders", "status", map[string]interface{}{
				"operator":   "eq",
				"value":      "completed",
				"operations": []rules.OperationType{rules.OperationDelete},
			}),
			input: map[string]utils.CDCValue{
				"status": {Type: pgtype.TextOID, Value: "completed"},
			},
			operation: rules.OperationUpdate,
			expectedOutput: map[string]utils.CDCValue{
				"status": {Type: pgtype.TextOID, Value: "completed"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := tt.rule.Apply(tt.input, tt.operation)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func TestDateTimeFilters(t *testing.T) {
	now := time.Now()
	pastDate := now.Add(-24 * time.Hour)
	futureDate := now.Add(24 * time.Hour)

	tests := []struct {
		name           string
		rule           rules.Rule
		input          map[string]utils.CDCValue
		operation      rules.OperationType
		expectedOutput map[string]utils.CDCValue
	}{
		{
			name: "Greater Than Date Filter - Pass",
			rule: createRule(t, "filter", "events", "date", map[string]interface{}{
				"operator": "gt",
				"value":    pastDate.Format(time.RFC3339),
			}),
			input: map[string]utils.CDCValue{
				"date": {Type: pgtype.TimestamptzOID, Value: now},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"date": {Type: pgtype.TimestamptzOID, Value: now},
			},
		},
		{
			name: "Greater Than Date Filter - Fail",
			rule: createRule(t, "filter", "events", "date", map[string]interface{}{
				"operator": "gt",
				"value":    futureDate.Format(time.RFC3339),
			}),
			input: map[string]utils.CDCValue{
				"date": {Type: pgtype.TimestamptzOID, Value: now},
			},
			operation:      rules.OperationInsert,
			expectedOutput: nil,
		},
		{
			name: "Greater Than Date Filter - Pass (INSERT operation)",
			rule: createRule(t, "filter", "events", "date", map[string]interface{}{
				"operator":   "gt",
				"value":      pastDate.Format(time.RFC3339),
				"operations": []rules.OperationType{rules.OperationInsert},
			}),
			input: map[string]utils.CDCValue{
				"date": {Type: pgtype.TimestamptzOID, Value: now},
			},
			operation: rules.OperationInsert,
			expectedOutput: map[string]utils.CDCValue{
				"date": {Type: pgtype.TimestamptzOID, Value: now},
			},
		},
		{
			name: "Greater Than Date Filter - Not Applied (UPDATE operation)",
			rule: createRule(t, "filter", "events", "date", map[string]interface{}{
				"operator":   "gt",
				"value":      pastDate.Format(time.RFC3339),
				"operations": []rules.OperationType{rules.OperationInsert},
			}),
			input: map[string]utils.CDCValue{
				"date": {Type: pgtype.TimestamptzOID, Value: now},
			},
			operation: rules.OperationUpdate,
			expectedOutput: map[string]utils.CDCValue{
				"date": {Type: pgtype.TimestamptzOID, Value: now},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := tt.rule.Apply(tt.input, tt.operation)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func createRule(t *testing.T, ruleType, table, column string, params map[string]interface{}) rules.Rule {
	var rule rules.Rule
	var err error

	switch ruleType {
	case "transform":
		rule, err = rules.NewTransformRule(table, column, params)
	case "filter":
		rule, err = rules.NewFilterRule(table, column, params)
	default:
		t.Fatalf("Unknown rule type: %s", ruleType)
	}

	if err != nil {
		t.Fatalf("Failed to create rule: %v", err)
	}

	return rule
}
