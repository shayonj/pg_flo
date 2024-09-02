package rules

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/rs/zerolog"
	"github.com/shayonj/pg_flo/pkg/utils"
	"github.com/shopspring/decimal"
)

var logger zerolog.Logger

func init() {
	logger = zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
}

// NewTransformRule creates a new transform rule based on the provided parameters
func NewTransformRule(table, column string, params map[string]interface{}) (Rule, error) {
	transformType, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("transform type is required for TransformRule")
	}

	operations, _ := params["operations"].([]OperationType)
	allowEmptyDeletes, _ := params["allow_empty_deletes"].(bool)

	switch transformType {
	case "regex":
		rule, err := NewRegexTransformRule(table, column, params)
		if err != nil {
			return nil, err
		}
		rule.Operations = operations
		rule.AllowEmptyDeletes = allowEmptyDeletes
		if len(rule.Operations) == 0 {
			rule.Operations = []OperationType{OperationInsert, OperationUpdate, OperationDelete}
		}
		return rule, nil
	case "mask":
		rule, err := NewMaskTransformRule(table, column, params)
		if err != nil {
			return nil, err
		}
		rule.Operations = operations
		rule.AllowEmptyDeletes = allowEmptyDeletes
		if len(rule.Operations) == 0 {
			rule.Operations = []OperationType{OperationInsert, OperationUpdate, OperationDelete}
		}
		return rule, nil
	default:
		return nil, fmt.Errorf("unsupported transform type: %s", transformType)
	}
}

// NewRegexTransformRule creates a new regex transform rule
func NewRegexTransformRule(table, column string, params map[string]interface{}) (*TransformRule, error) {
	pattern, ok := params["pattern"].(string)
	if !ok {
		return nil, fmt.Errorf("pattern parameter is required for regex transform")
	}
	replace, ok := params["replace"].(string)
	if !ok {
		return nil, fmt.Errorf("replace parameter is required for regex transform")
	}
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}

	transform := func(v utils.CDCValue) (utils.CDCValue, error) {
		str, ok := v.Value.(string)
		if !ok {
			logger.Warn().
				Str("table", table).
				Str("column", column).
				Str("type", fmt.Sprintf("%T", v.Value)).
				Msg("Regex transform skipped: only works on string values")
			return v, nil
		}
		return utils.CDCValue{Type: v.Type, Value: regex.ReplaceAllString(str, replace)}, nil
	}

	return &TransformRule{TableName: table, ColumnName: column, Transform: transform}, nil
}

// NewMaskTransformRule creates a new mask transform rule
func NewMaskTransformRule(table, column string, params map[string]interface{}) (*TransformRule, error) {
	maskChar, ok := params["mask_char"].(string)
	if !ok {
		return nil, fmt.Errorf("mask_char parameter is required for mask transform")
	}

	transform := func(v utils.CDCValue) (utils.CDCValue, error) {
		str, ok := v.Value.(string)
		if !ok {
			logger.Warn().
				Str("table", table).
				Str("column", column).
				Str("type", fmt.Sprintf("%T", v.Value)).
				Msg("Mask transform skipped: only works on string values")
			return v, nil
		}
		if len(str) <= 2 {
			return v, nil
		}
		masked := string(str[0]) + strings.Repeat(maskChar, len(str)-2) + string(str[len(str)-1])
		return utils.CDCValue{Type: v.Type, Value: masked}, nil
	}

	return &TransformRule{TableName: table, ColumnName: column, Transform: transform}, nil
}

// NewFilterRule creates a new filter rule based on the provided parameters
func NewFilterRule(table, column string, params map[string]interface{}) (Rule, error) {
	operator, ok := params["operator"].(string)
	if !ok {
		return nil, fmt.Errorf("operator parameter is required for FilterRule")
	}
	value, ok := params["value"]
	if !ok {
		return nil, fmt.Errorf("value parameter is required for FilterRule")
	}

	operations, _ := params["operations"].([]OperationType)
	allowEmptyDeletes, _ := params["allow_empty_deletes"].(bool)

	var condition func(utils.CDCValue) bool

	switch operator {
	case "eq", "ne", "gt", "lt", "gte", "lte":
		condition = NewComparisonCondition(operator, value)
	case "contains":
		condition = NewContainsCondition(value)
	default:
		return nil, fmt.Errorf("unsupported operator: %s", operator)
	}

	rule := &FilterRule{
		TableName:         table,
		ColumnName:        column,
		Condition:         condition,
		Operations:        operations,
		AllowEmptyDeletes: allowEmptyDeletes,
	}

	if len(rule.Operations) == 0 {
		rule.Operations = []OperationType{OperationInsert, OperationUpdate, OperationDelete}
	}

	return rule, nil
}

// NewComparisonCondition creates a new comparison condition function
func NewComparisonCondition(operator string, value interface{}) func(utils.CDCValue) bool {
	return func(v utils.CDCValue) bool {
		switch v.Type {
		case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
			intVal, ok := utils.ToInt64(v.Value)
			if !ok {
				return false
			}
			compareVal, ok := utils.ToInt64(value)
			if !ok {
				return false
			}
			return compareValues(intVal, compareVal, operator)
		case pgtype.Float4OID, pgtype.Float8OID:
			floatVal, ok := utils.ToFloat64(v.Value)
			if !ok {
				return false
			}
			compareVal, ok := utils.ToFloat64(value)
			if !ok {
				return false
			}
			return compareValues(floatVal, compareVal, operator)
		case pgtype.TextOID, pgtype.VarcharOID:
			strVal, ok := v.Value.(string)
			if !ok {
				return false
			}
			compareVal, ok := value.(string)
			if !ok {
				return false
			}
			return compareValues(strVal, compareVal, operator)
		case pgtype.TimestampOID, pgtype.TimestamptzOID:
			timeVal, ok := v.Value.(time.Time)
			if !ok {
				return false
			}
			compareVal, err := utils.ParseTimestamp(fmt.Sprintf("%v", value))
			if err != nil {
				return false
			}
			return compareValues(timeVal, compareVal, operator)
		case pgtype.BoolOID:
			boolVal, ok := v.Value.(bool)
			if !ok {
				return false
			}
			compareVal, ok := value.(bool)
			if !ok {
				return false
			}
			return compareValues(boolVal, compareVal, operator)
		case pgtype.NumericOID:
			numVal, ok := v.Value.(string)
			if !ok {
				return false
			}
			compareVal, ok := value.(string)
			if !ok {
				return false
			}
			return compareNumericValues(numVal, compareVal, operator)
		default:
			return false
		}
	}
}

// NewContainsCondition creates a new contains condition function
func NewContainsCondition(value interface{}) func(utils.CDCValue) bool {
	return func(v utils.CDCValue) bool {
		str, ok := v.Value.(string)
		if !ok {
			return false
		}
		searchStr, ok := value.(string)
		if !ok {
			return false
		}
		return strings.Contains(str, searchStr)
	}
}

// compareValues compares two values based on the provided operator
func compareValues(a, b interface{}, operator string) bool {
	switch operator {
	case "eq":
		return reflect.DeepEqual(a, b)
	case "ne":
		return !reflect.DeepEqual(a, b)
	case "gt":
		return compareGreaterThan(a, b)
	case "lt":
		return compareLessThan(a, b)
	case "gte":
		return compareGreaterThan(a, b) || reflect.DeepEqual(a, b)
	case "lte":
		return compareLessThan(a, b) || reflect.DeepEqual(a, b)
	}
	return false
}

// compareGreaterThan checks if 'a' is greater than 'b'
func compareGreaterThan(a, b interface{}) bool {
	switch a := a.(type) {
	case int64:
		return a > b.(int64)
	case float64:
		return a > b.(float64)
	case string:
		return a > b.(string)
	case time.Time:
		return a.After(b.(time.Time))
	default:
		return false
	}
}

// compareLessThan checks if 'a' is less than 'b'
func compareLessThan(a, b interface{}) bool {
	switch a := a.(type) {
	case int64:
		return a < b.(int64)
	case float64:
		return a < b.(float64)
	case string:
		return a < b.(string)
	case time.Time:
		return a.Before(b.(time.Time))
	default:
		return false
	}
}

// compareNumericValues compares two numeric values based on the provided operator
func compareNumericValues(a, b string, operator string) bool {
	aNum, err1 := decimal.NewFromString(a)
	bNum, err2 := decimal.NewFromString(b)
	if err1 != nil || err2 != nil {
		return false
	}

	switch operator {
	case "eq":
		return aNum.Equal(bNum)
	case "ne":
		return !aNum.Equal(bNum)
	case "gt":
		return aNum.GreaterThan(bNum)
	case "lt":
		return aNum.LessThan(bNum)
	case "gte":
		return aNum.GreaterThanOrEqual(bNum)
	case "lte":
		return aNum.LessThanOrEqual(bNum)
	default:
		return false
	}
}

// Apply applies the transform rule to the provided data
func (r *TransformRule) Apply(data map[string]utils.CDCValue, operation OperationType) (map[string]utils.CDCValue, error) {
	if !containsOperation(r.Operations, operation) {
		return data, nil
	}

	// Don't apply rule if asked not to
	if operation == OperationDelete && r.AllowEmptyDeletes {
		return data, nil
	}

	if value, ok := data[r.ColumnName]; ok {
		transformed, err := r.Transform(value)
		if err != nil {
			return nil, err
		}
		data[r.ColumnName] = transformed
	}
	return data, nil
}

// Apply applies the filter rule to the provided data
func (r *FilterRule) Apply(data map[string]utils.CDCValue, operation OperationType) (map[string]utils.CDCValue, error) {
	if !containsOperation(r.Operations, operation) {
		return data, nil
	}

	// Don't apply rule if asked not to
	if operation == OperationDelete && r.AllowEmptyDeletes {
		return data, nil
	}

	if value, ok := data[r.ColumnName]; ok {
		passes := r.Condition(value)
		logger.Debug().
			Str("column", r.ColumnName).
			Interface("value", value).
			Any("operation", operation).
			Bool("passes", passes).
			Bool("allowEmptyDeletes", r.AllowEmptyDeletes).
			Msg("Filter condition result")
		if !passes {
			return nil, nil // filter out
		}
	} else {
		logger.Warn().
			Str("column", r.ColumnName).
			Any("operation", operation).
			Bool("allowEmptyDeletes", r.AllowEmptyDeletes).
			Msg("Column not found in data")
	}
	return data, nil
}

// containsOperation checks if the given operation is in the list of operations
func containsOperation(operations []OperationType, operation OperationType) bool {
	for _, op := range operations {
		if op == operation {
			return true
		}
	}
	return false
}
