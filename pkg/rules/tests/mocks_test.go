package rules_test

import (
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/utils"
)

type MockRule struct {
	TableName  string
	ColumnName string
	ApplyFunc  func(map[string]utils.CDCValue, rules.OperationType) (map[string]utils.CDCValue, error)
}

func (r *MockRule) Apply(data map[string]utils.CDCValue, operation rules.OperationType) (map[string]utils.CDCValue, error) {
	return r.ApplyFunc(data, operation)
}
