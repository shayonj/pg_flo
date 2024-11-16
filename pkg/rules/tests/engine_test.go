package rules_test

import (
	"log"
	"os"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/pgflo/pg_flo/pkg/rules"
	"github.com/pgflo/pg_flo/pkg/utils"
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
		ApplyFunc: func(message *utils.CDCMessage) (*utils.CDCMessage, error) {
			return message, nil
		},
	}
	re.AddRule("users", rule)

	message := &utils.CDCMessage{
		Type:   utils.OperationInsert,
		Schema: "public",
		Table:  "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "test_column", DataType: pgtype.TextOID},
		},
		NewTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{Data: []byte("original")},
			},
		},
	}

	result, err := re.ApplyRules(message)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestRuleEngine_ApplyRules(t *testing.T) {
	re := rules.NewRuleEngine()
	rule := &MockRule{
		TableName:  "users",
		ColumnName: "test_column",
		ApplyFunc: func(message *utils.CDCMessage) (*utils.CDCMessage, error) {
			message.NewTuple.Columns[0].Data = []byte("transformed")
			return message, nil
		},
	}
	re.AddRule("users", rule)

	message := &utils.CDCMessage{
		Type:   utils.OperationInsert,
		Schema: "public",
		Table:  "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "test_column", DataType: pgtype.TextOID},
		},
		NewTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{Data: []byte("original")},
			},
		},
	}

	result, err := re.ApplyRules(message)

	assert.NoError(t, err)
	value, err := result.GetColumnValue("test_column", false)
	assert.NoError(t, err)
	assert.Equal(t, "transformed", value)
}

func TestRuleEngine_ApplyRules_NoRules(t *testing.T) {
	re := rules.NewRuleEngine()
	message := &utils.CDCMessage{
		Type:   utils.OperationInsert,
		Schema: "public",
		Table:  "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "test_column", DataType: pgtype.TextOID},
		},
		NewTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{Data: []byte("original")},
			},
		},
	}

	result, err := re.ApplyRules(message)

	assert.NoError(t, err)
	assert.Equal(t, message, result)
}

func TestRuleEngine_LoadRules_Transform(t *testing.T) {
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
					Operations: []utils.OperationType{utils.OperationInsert, utils.OperationUpdate},
				},
			},
		},
	}

	err := re.LoadRules(config)
	assert.NoError(t, err)

	message := &utils.CDCMessage{
		Type:   utils.OperationInsert,
		Schema: "public",
		Table:  "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "test_column", DataType: pgtype.TextOID},
		},
		NewTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{Data: []byte("test")},
			},
		},
	}

	result, err := re.ApplyRules(message)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	value, err := result.GetColumnValue("test_column", false)
	assert.NoError(t, err)
	assert.Equal(t, "t**t", value)
}

func TestRuleEngine_LoadRules_Filter(t *testing.T) {
	re := rules.NewRuleEngine()
	config := rules.Config{
		Tables: map[string][]rules.RuleConfig{
			"users": {
				{
					Type:   "filter",
					Column: "id",
					Parameters: map[string]interface{}{
						"operator": "gt",
						"value":    int64(100),
					},
					Operations: []utils.OperationType{utils.OperationDelete},
				},
			},
		},
	}

	err := re.LoadRules(config)
	assert.NoError(t, err)

	message := &utils.CDCMessage{
		Type:   utils.OperationDelete,
		Schema: "public",
		Table:  "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int8OID},
		},
		OldTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{Data: []byte("101")},
			},
		},
	}

	result, err := re.ApplyRules(message)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	value, err := result.GetColumnValue("id", true)
	assert.NoError(t, err)
	assert.Equal(t, int64(101), value)

	message.OldTuple.Columns[0].Data = []byte("99")
	result, err = re.ApplyRules(message)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestRuleEngine_LoadRules_EmptyDeletes(t *testing.T) {
	re := rules.NewRuleEngine()
	config := rules.Config{
		Tables: map[string][]rules.RuleConfig{
			"users": {
				{
					Type:              "filter",
					Column:            "id",
					AllowEmptyDeletes: true,
					Parameters: map[string]interface{}{
						"operator": "eq",
						"value":    int64(101),
					},
					Operations: []utils.OperationType{utils.OperationDelete},
				},
			},
		},
	}

	err := re.LoadRules(config)
	assert.NoError(t, err)

	message := &utils.CDCMessage{
		Type:   utils.OperationDelete,
		Schema: "public",
		Table:  "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int8OID},
		},
		OldTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{Data: []byte("101")},
			},
		},
	}

	result, err := re.ApplyRules(message)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	value, err := result.GetColumnValue("id", true)
	assert.NoError(t, err)
	assert.Equal(t, int64(101), value)
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
						"value":    int64(100),
					},
					Operations: []utils.OperationType{utils.OperationUpdate},
				},
			},
		},
	}

	err := re.LoadRules(config)
	assert.NoError(t, err)

	message := &utils.CDCMessage{
		Type:   utils.OperationUpdate,
		Schema: "public",
		Table:  "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int8OID},
		},
		NewTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{Data: []byte("101")},
			},
		},
	}
	result, err := re.ApplyRules(message)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	idValue, err := result.GetColumnValue("id", false)
	assert.NoError(t, err)
	assert.Equal(t, int64(101), idValue)

	message.NewTuple.Columns[0].Data = []byte("99")
	result, err = re.ApplyRules(message)

	assert.NoError(t, err)
	assert.Nil(t, result)

	message.Type = utils.OperationInsert
	message.NewTuple.Columns[0].Data = []byte("101")
	result, err = re.ApplyRules(message)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	idValue, err = result.GetColumnValue("id", false)
	assert.NoError(t, err)
	assert.Equal(t, int64(101), idValue)
}
