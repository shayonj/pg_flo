package replicator_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCopyAndStreamReplicator(t *testing.T) {

	t.Run("CopyTable", func(t *testing.T) {
		mockStandardConn := new(MockStandardConnection)
		mockNATSClient := new(MockNATSClient)
		mockPoolConn := new(MockPgxPoolConn)
		mockTx := new(MockTx)
		mockRows := new(MockRows)

		mockStandardConn.On("Acquire", mock.Anything).Return(mockPoolConn, nil)
		mockStandardConn.On("QueryRow", mock.Anything, mock.MatchedBy(func(query string) bool {
			return strings.Contains(query, "SELECT relpages")
		}), mock.Anything).Return(MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*uint32) = 201
				return nil
			},
		})

		mockPoolConn.On("BeginTx", mock.Anything, mock.MatchedBy(func(txOptions pgx.TxOptions) bool {
			return txOptions.IsoLevel == pgx.Serializable && txOptions.AccessMode == pgx.ReadOnly
		})).Return(mockTx, nil)

		mockTx.On("QueryRow", mock.Anything, "SELECT schemaname FROM pg_tables WHERE tablename = $1", mock.Anything).Return(MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*string) = "public"
				return nil
			},
		})

		mockTx.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(pgconn.CommandTag{}, nil)

		mockRows.On("Next").Return(true).Once().On("Next").Return(false)
		mockRows.On("Err").Return(nil)
		mockRows.On("Close").Return()
		mockRows.On("FieldDescriptions").Return([]pgconn.FieldDescription{
			{Name: "id", DataTypeOID: 23},
			{Name: "name", DataTypeOID: 25},
		})
		mockRows.On("Values").Return([]interface{}{1, "John Doe"}, nil)

		mockTx.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(mockRows, nil)
		mockTx.On("Commit", mock.Anything).Return(nil)
		mockPoolConn.On("Release").Return()

		mockNATSClient.On("PublishMessage", "pgflo.test_group", mock.MatchedBy(func(data []byte) bool {
			var decodedMsg utils.CDCMessage
			err := decodedMsg.UnmarshalBinary(data)
			if err != nil {
				t.Logf("Failed to unmarshal binary data: %v", err)
				return false
			}

			assert.Equal(t, utils.OperationInsert, decodedMsg.Type)
			assert.Equal(t, "public", decodedMsg.Schema)
			assert.Equal(t, "users", decodedMsg.Table)

			assert.Len(t, decodedMsg.Columns, 2)
			assert.Equal(t, "id", decodedMsg.Columns[0].Name)
			assert.Equal(t, uint32(pgtype.Int4OID), decodedMsg.Columns[0].DataType)
			assert.Equal(t, "name", decodedMsg.Columns[1].Name)
			assert.Equal(t, uint32(pgtype.TextOID), decodedMsg.Columns[1].DataType)

			assert.NotNil(t, decodedMsg.NewTuple)
			assert.Len(t, decodedMsg.NewTuple.Columns, 2)
			assert.Equal(t, []byte("1"), decodedMsg.NewTuple.Columns[0].Data)
			assert.Equal(t, []byte("John Doe"), decodedMsg.NewTuple.Columns[1].Data)

			return true
		})).Return(nil)

		csr := &replicator.CopyAndStreamReplicator{
			BaseReplicator: replicator.BaseReplicator{
				StandardConn: mockStandardConn,
				NATSClient:   mockNATSClient,
				Logger:       zerolog.Nop(),
				Config: replicator.Config{
					Tables:   []string{"users"},
					Schema:   "public",
					Host:     "localhost",
					Port:     5432,
					User:     "testuser",
					Password: "testpassword",
					Database: "testdb",
					Group:    "test_group",
				},
			},
			MaxCopyWorkersPerTable: 2,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := csr.CopyTable(ctx, "users", "snapshot-1")
		assert.NoError(t, err)

		mockStandardConn.AssertExpectations(t)
		mockPoolConn.AssertExpectations(t)
		mockTx.AssertExpectations(t)
		mockRows.AssertExpectations(t)
		mockNATSClient.AssertExpectations(t)
	})

	t.Run("CopyTableRange", func(t *testing.T) {
		mockStandardConn := new(MockStandardConnection)
		mockNATSClient := new(MockNATSClient)
		mockPoolConn := new(MockPgxPoolConn)
		mockTx := new(MockTx)
		mockRows := new(MockRows)

		mockStandardConn.On("Acquire", mock.Anything).Return(mockPoolConn, nil)

		mockPoolConn.On("BeginTx", mock.Anything, mock.MatchedBy(func(txOptions pgx.TxOptions) bool {
			return txOptions.IsoLevel == pgx.Serializable && txOptions.AccessMode == pgx.ReadOnly
		})).Return(mockTx, nil)

		mockTx.On("Exec", mock.Anything, mock.MatchedBy(func(sql string) bool {
			return strings.Contains(sql, "SET TRANSACTION SNAPSHOT")
		}), mock.Anything).Return(pgconn.CommandTag{}, nil).Once()

		mockTx.On("QueryRow", mock.Anything, "SELECT schemaname FROM pg_tables WHERE tablename = $1", mock.Anything).Return(MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*string) = "public"
				return nil
			},
		})

		mockRows.On("Next").Return(true).Once().On("Next").Return(false)
		mockRows.On("Err").Return(nil)
		mockRows.On("Close").Return()
		mockRows.On("FieldDescriptions").Return([]pgconn.FieldDescription{
			{Name: "id", DataTypeOID: 23},
			{Name: "name", DataTypeOID: 25},
		})
		mockRows.On("Values").Return([]interface{}{1, "John Doe"}, nil)

		mockTx.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(mockRows, nil)
		mockTx.On("Commit", mock.Anything).Return(nil)
		mockPoolConn.On("Release").Return()

		mockNATSClient.On("PublishMessage", "pgflo.test_group", mock.Anything).Return(nil)

		csr := &replicator.CopyAndStreamReplicator{
			BaseReplicator: replicator.BaseReplicator{
				StandardConn: mockStandardConn,
				NATSClient:   mockNATSClient,
				Logger:       zerolog.Nop(),
				Config: replicator.Config{
					Tables:   []string{"users"},
					Schema:   "public",
					Host:     "localhost",
					Port:     5432,
					User:     "testuser",
					Password: "testpassword",
					Database: "testdb",
					Group:    "test_group",
				},
			},
		}

		rowsCopied, err := csr.CopyTableRange(context.Background(), "users", 0, 1000, "snapshot-1", 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rowsCopied)

		mockStandardConn.AssertExpectations(t)
		mockPoolConn.AssertExpectations(t)
		mockTx.AssertExpectations(t)
		mockRows.AssertExpectations(t)
		mockNATSClient.AssertExpectations(t)
	})
	t.Run("CopyTableRange with diverse data types", func(t *testing.T) {
		testCases := []struct {
			name           string
			relationFields []pgconn.FieldDescription
			tupleData      []interface{}
			expected       []map[string]interface{}
		}{
			{
				name: "Basic types",
				relationFields: []pgconn.FieldDescription{
					{Name: "id", DataTypeOID: pgtype.Int4OID},
					{Name: "name", DataTypeOID: pgtype.TextOID},
					{Name: "active", DataTypeOID: pgtype.BoolOID},
					{Name: "score", DataTypeOID: pgtype.Float8OID},
				},
				tupleData: []interface{}{
					int64(1), "John Doe", true, float64(9.99),
				},
				expected: []map[string]interface{}{
					{"name": "id", "type": "int4", "value": int64(1)},
					{"name": "name", "type": "text", "value": "John Doe"},
					{"name": "active", "type": "bool", "value": true},
					{"name": "score", "type": "float8", "value": float64(9.99)},
				},
			},
			{
				name: "Complex types",
				relationFields: []pgconn.FieldDescription{
					{Name: "data", DataTypeOID: pgtype.JSONBOID},
					{Name: "tags", DataTypeOID: pgtype.TextArrayOID},
					{Name: "image", DataTypeOID: pgtype.ByteaOID},
					{Name: "created_at", DataTypeOID: pgtype.TimestamptzOID},
				},
				tupleData: []interface{}{
					[]byte(`{"key": "value"}`),
					[]string{"tag1", "tag2", "tag3"},
					[]byte{0x01, 0x02, 0x03, 0x04},
					time.Date(2023, time.May, 1, 12, 34, 56, 789000000, time.UTC),
				},
				expected: []map[string]interface{}{
					{"name": "data", "type": "jsonb", "value": json.RawMessage(`{"key": "value"}`)},
					{"name": "tags", "type": "text[]", "value": "{tag1,tag2,tag3}"},
					{"name": "image", "type": "bytea", "value": []byte{0x01, 0x02, 0x03, 0x04}},
					{"name": "created_at", "type": "timestamptz", "value": time.Date(2023, time.May, 1, 12, 34, 56, 789000000, time.UTC)},
				},
			},
			{
				name: "Numeric types",
				relationFields: []pgconn.FieldDescription{
					{Name: "small_int", DataTypeOID: pgtype.Int2OID},
					{Name: "big_int", DataTypeOID: pgtype.Int8OID},
					{Name: "numeric", DataTypeOID: pgtype.NumericOID},
				},
				tupleData: []interface{}{
					int64(32767), int64(9223372036854775807), "123456.789",
				},
				expected: []map[string]interface{}{
					{"name": "small_int", "type": "int2", "value": int64(32767)},
					{"name": "big_int", "type": "int8", "value": int64(9223372036854775807)},
					{"name": "numeric", "type": "numeric", "value": "123456.789"},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockStandardConn := new(MockStandardConnection)
				mockNATSClient := new(MockNATSClient)
				mockPoolConn := new(MockPgxPoolConn)
				mockTx := new(MockTx)
				mockRows := new(MockRows)

				mockStandardConn.On("Acquire", mock.Anything).Return(mockPoolConn, nil)
				mockPoolConn.On("BeginTx", mock.Anything, mock.AnythingOfType("pgx.TxOptions")).Return(mockTx, nil)
				mockTx.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(pgconn.CommandTag{}, nil)
				mockTx.On("QueryRow", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*string) = "public"
						return nil
					},
				})

				mockRows.On("Next").Return(true).Once().On("Next").Return(false)
				mockRows.On("Err").Return(nil)
				mockRows.On("Close").Return()
				mockRows.On("FieldDescriptions").Return(tc.relationFields)
				mockRows.On("Values").Return(tc.tupleData, nil)

				mockTx.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(mockRows, nil)
				mockTx.On("Commit", mock.Anything).Return(nil)
				mockPoolConn.On("Release").Return()

				mockNATSClient.On("PublishMessage", "pgflo.test_group", mock.MatchedBy(func(data []byte) bool {
					var decodedMsg utils.CDCMessage
					err := decodedMsg.UnmarshalBinary(data)
					assert.NoError(t, err, "Failed to unmarshal binary data")

					assert.Equal(t, utils.OperationInsert, decodedMsg.Type)
					assert.Equal(t, "public", decodedMsg.Schema)
					assert.Equal(t, "test_table", decodedMsg.Table)
					assert.Equal(t, len(tc.expected), len(decodedMsg.NewTuple.Columns))

					for i, expectedValue := range tc.expected {
						actualColumn := decodedMsg.NewTuple.Columns[i]
						expectedType := expectedValue["type"].(string)
						expectedVal := expectedValue["value"]

						assert.Equal(t, expectedType, utils.OIDToString(decodedMsg.Columns[i].DataType), "Type mismatch for field %s", decodedMsg.Columns[i].Name)

						switch expectedType {
						case "int2", "int4", "int8":
							actualVal, err := strconv.ParseInt(string(actualColumn.Data), 10, 64)
							assert.NoError(t, err)
							assert.Equal(t, expectedVal, actualVal)
						case "float8":
							actualVal, err := strconv.ParseFloat(string(actualColumn.Data), 64)
							assert.NoError(t, err)
							assert.InDelta(t, expectedVal.(float64), actualVal, 0.0001)
						case "bool":
							actualVal, err := strconv.ParseBool(string(actualColumn.Data))
							assert.NoError(t, err)
							assert.Equal(t, expectedVal, actualVal)
						case "text", "varchar":
							assert.Equal(t, expectedVal, string(actualColumn.Data))
						case "jsonb":
							assert.JSONEq(t, string(expectedVal.(json.RawMessage)), string(actualColumn.Data))
						case "text[]":
							assert.Equal(t, expectedVal, string(actualColumn.Data))
						case "bytea":
							expectedBytes, ok := expectedValue["value"].([]byte)
							assert.True(t, ok, "Expected value for bytea should be []byte")
							assert.Equal(t, expectedBytes, actualColumn.Data)
						case "timestamptz":
							actualTime, err := time.Parse(time.RFC3339Nano, string(actualColumn.Data))
							assert.NoError(t, err)
							assert.Equal(t, expectedVal.(time.Time), actualTime)
						case "numeric":
							assert.Equal(t, expectedVal, string(actualColumn.Data))
						default:
							assert.Equal(t, fmt.Sprintf("%v", expectedVal), string(actualColumn.Data))
						}
					}

					return true
				})).Return(nil)

				csr := &replicator.CopyAndStreamReplicator{
					BaseReplicator: replicator.BaseReplicator{
						StandardConn: mockStandardConn,
						NATSClient:   mockNATSClient,
						Logger:       zerolog.Nop(),
						Config: replicator.Config{
							Tables:   []string{"test_table"},
							Schema:   "public",
							Host:     "localhost",
							Port:     5432,
							User:     "testuser",
							Password: "testpassword",
							Database: "testdb",
							Group:    "test_group",
						},
					},
				}

				rowsCopied, err := csr.CopyTableRange(context.Background(), "test_table", 0, 1000, "snapshot-1", 0)
				assert.NoError(t, err)
				assert.Equal(t, int64(1), rowsCopied)

				// Assert expectations
				mockStandardConn.AssertExpectations(t)
				mockPoolConn.AssertExpectations(t)
				mockTx.AssertExpectations(t)
				mockRows.AssertExpectations(t)
				mockNATSClient.AssertExpectations(t)
			})
		}
	})

}
