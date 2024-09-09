package rules_test

import (
	"github.com/shayonj/pg_flo/pkg/utils"
)

type MockRule struct {
	TableName  string
	ColumnName string
	ApplyFunc  func(*utils.CDCMessage) (*utils.CDCMessage, error)
}

func (r *MockRule) Apply(message *utils.CDCMessage) (*utils.CDCMessage, error) {
	return r.ApplyFunc(message)
}
