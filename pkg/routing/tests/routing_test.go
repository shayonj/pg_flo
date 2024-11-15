package routing_test

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/shayonj/pg_flo/pkg/routing"
	"github.com/shayonj/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestRouter_ApplyRouting(t *testing.T) {
	tests := []struct {
		name           string
		routes         map[string]routing.TableRoute
		inputMessage   *utils.CDCMessage
		expectedOutput *utils.CDCMessage
		expectNil      bool
	}{
		{
			name: "Simple table routing",
			routes: map[string]routing.TableRoute{
				"source_table": {
					SourceTable:      "source_table",
					DestinationTable: "dest_table",
					Operations:       []utils.OperationType{utils.OperationInsert, utils.OperationUpdate, utils.OperationDelete},
				},
			},
			inputMessage: &utils.CDCMessage{
				Type:  utils.OperationInsert,
				Table: "source_table",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
					{Name: "name", DataType: 25},
				},
			},
			expectedOutput: &utils.CDCMessage{
				Type:  utils.OperationInsert,
				Table: "dest_table",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
					{Name: "name", DataType: 25},
				},
			},
		},
		{
			name: "Column mapping",
			routes: map[string]routing.TableRoute{
				"users": {
					SourceTable:      "users",
					DestinationTable: "customers",
					ColumnMappings: []routing.ColumnMapping{
						{Source: "user_id", Destination: "customer_id"},
						{Source: "user_name", Destination: "customer_name"},
					},
					Operations: []utils.OperationType{utils.OperationInsert, utils.OperationUpdate, utils.OperationDelete},
				},
			},
			inputMessage: &utils.CDCMessage{
				Type:  utils.OperationUpdate,
				Table: "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "user_id", DataType: 23},
					{Name: "user_name", DataType: 25},
					{Name: "email", DataType: 25},
				},
			},
			expectedOutput: &utils.CDCMessage{
				Type:  utils.OperationUpdate,
				Table: "customers",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "customer_id", DataType: 23},
					{Name: "customer_name", DataType: 25},
					{Name: "email", DataType: 25},
				},
			},
		},
		{
			name: "Operation filtering - allowed",
			routes: map[string]routing.TableRoute{
				"orders": {
					SourceTable:      "orders",
					DestinationTable: "processed_orders",
					Operations:       []utils.OperationType{utils.OperationInsert, utils.OperationUpdate},
				},
			},
			inputMessage: &utils.CDCMessage{
				Type:  utils.OperationUpdate,
				Table: "orders",
			},
			expectedOutput: &utils.CDCMessage{
				Type:  utils.OperationUpdate,
				Table: "processed_orders",
			},
		},
		{
			name: "Operation filtering - not allowed",
			routes: map[string]routing.TableRoute{
				"orders": {
					SourceTable:      "orders",
					DestinationTable: "processed_orders",
					Operations:       []utils.OperationType{utils.OperationInsert, utils.OperationUpdate},
				},
			},
			inputMessage: &utils.CDCMessage{
				Type:  utils.OperationDelete,
				Table: "orders",
			},
			expectNil: true,
		},
		{
			name:   "No route for table",
			routes: map[string]routing.TableRoute{},
			inputMessage: &utils.CDCMessage{
				Type:  utils.OperationInsert,
				Table: "unknown_table",
			},
			expectedOutput: &utils.CDCMessage{
				Type:  utils.OperationInsert,
				Table: "unknown_table",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := routing.NewRouter()
			for _, route := range tt.routes {
				router.AddRoute(route)
			}

			result, err := router.ApplyRouting(tt.inputMessage)

			assert.NoError(t, err)

			if tt.expectNil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expectedOutput.Type, result.Type)
				assert.Equal(t, tt.expectedOutput.Table, result.Table)
				assert.Equal(t, len(tt.expectedOutput.Columns), len(result.Columns))
				for i, col := range tt.expectedOutput.Columns {
					assert.Equal(t, col.Name, result.Columns[i].Name)
					assert.Equal(t, col.DataType, result.Columns[i].DataType)
				}
			}
		})
	}
}

func TestRouter_LoadRoutes(t *testing.T) {
	router := routing.NewRouter()
	config := map[string]routing.TableRoute{
		"table1": {
			SourceTable:      "table1",
			DestinationTable: "dest_table1",
			ColumnMappings: []routing.ColumnMapping{
				{Source: "col1", Destination: "dest_col1"},
			},
			Operations: []utils.OperationType{utils.OperationInsert, utils.OperationUpdate},
		},
		"table2": {
			SourceTable:      "table2",
			DestinationTable: "dest_table2",
			Operations:       []utils.OperationType{utils.OperationInsert, utils.OperationUpdate, utils.OperationDelete},
		},
	}

	err := router.LoadRoutes(config)
	assert.NoError(t, err)

	assert.Len(t, router.Routes, 2)
	assert.Contains(t, router.Routes, "table1")
	assert.Contains(t, router.Routes, "table2")

	assert.Equal(t, "dest_table1", router.Routes["table1"].DestinationTable)
	assert.Equal(t, "dest_table2", router.Routes["table2"].DestinationTable)

	assert.Len(t, router.Routes["table1"].ColumnMappings, 1)
	assert.Len(t, router.Routes["table1"].Operations, 2)
	assert.Len(t, router.Routes["table2"].Operations, 3)
}

func TestRouter_AddRoute(t *testing.T) {
	router := routing.NewRouter()
	route := routing.TableRoute{
		SourceTable:      "source",
		DestinationTable: "destination",
		ColumnMappings: []routing.ColumnMapping{
			{Source: "src_col", Destination: "dest_col"},
		},
		Operations: []utils.OperationType{utils.OperationInsert},
	}

	router.AddRoute(route)

	assert.Len(t, router.Routes, 1)
	assert.Contains(t, router.Routes, "source")
	assert.Equal(t, route, router.Routes["source"])
}

func TestContainsOperation(t *testing.T) {
	operations := []utils.OperationType{utils.OperationInsert, utils.OperationUpdate}

	assert.True(t, routing.ContainsOperation(operations, utils.OperationInsert))
	assert.True(t, routing.ContainsOperation(operations, utils.OperationUpdate))
	assert.False(t, routing.ContainsOperation(operations, utils.OperationDelete))
}

func TestGetMappedColumnName(t *testing.T) {
	mappings := []routing.ColumnMapping{
		{Source: "col1", Destination: "mapped_col1"},
		{Source: "col2", Destination: "mapped_col2"},
	}

	assert.Equal(t, "mapped_col1", routing.GetMappedColumnName(mappings, "col1"))
	assert.Equal(t, "mapped_col2", routing.GetMappedColumnName(mappings, "col2"))
	assert.Equal(t, "", routing.GetMappedColumnName(mappings, "col3"))
}
