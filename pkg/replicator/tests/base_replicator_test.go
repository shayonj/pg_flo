package replicator_test

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"
	"github.com/shayonj/pg_flo/pkg/pgflonats"
	"github.com/shayonj/pg_flo/pkg/replicator"
	"github.com/shayonj/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestBaseReplicator(t *testing.T) {
	t.Run("NewBaseReplicator", func(t *testing.T) {
		mockReplicationConn := new(MockReplicationConnection)
		mockStandardConn := new(MockStandardConnection)
		mockNATSClient := new(MockNATSClient)

		mockRows := new(MockRows)
		mockRows.On("Next").Return(false).Once()
		mockRows.On("Err").Return(nil).Once()
		mockRows.On("Close").Return().Once()

		mockStandardConn.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(mockRows, nil).Once()

		mockPoolConn := &MockPgxPoolConn{}
		mockStandardConn.On("Acquire", mock.Anything).Return(mockPoolConn, nil).Maybe()

		config := replicator.Config{
			Host:     "localhost",
			Port:     5432,
			User:     "test_user",
			Password: "test_password",
			Database: "test_db",
			Group:    "test_group",
		}

		br := replicator.NewBaseReplicator(config, mockReplicationConn, mockStandardConn, mockNATSClient)

		assert.NotNil(t, br)
		assert.Equal(t, config, br.Config)
		assert.Equal(t, mockReplicationConn, br.ReplicationConn)
		assert.Equal(t, mockStandardConn, br.StandardConn)
		assert.Equal(t, mockNATSClient, br.NATSClient)

		mockStandardConn.AssertExpectations(t)
		mockRows.AssertExpectations(t)
	})

	t.Run("CreatePublication", func(t *testing.T) {
		t.Run("Publication already exists", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*bool) = true
						return nil
					},
				})

			br := &replicator.BaseReplicator{
				Config:       replicator.Config{Group: "existing_pub"},
				StandardConn: mockStandardConn,
				Logger:       zerolog.Nop(),
			}

			err := br.CreatePublication()
			assert.NoError(t, err)
			mockStandardConn.AssertExpectations(t)
		})

		t.Run("Publication created for all tables", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*bool) = false
						return nil
					},
				})
			expectedQuery := `CREATE PUBLICATION "pg_flo_new_pub_publication" FOR ALL TABLES`
			mockStandardConn.On("Exec", mock.Anything, expectedQuery, mock.Anything).
				Return(pgconn.CommandTag{}, nil)

			br := &replicator.BaseReplicator{
				Config:       replicator.Config{Group: "new_pub"},
				StandardConn: mockStandardConn,
				Logger:       zerolog.Nop(),
			}

			err := br.CreatePublication()
			assert.NoError(t, err)
			mockStandardConn.AssertExpectations(t)
		})

		t.Run("Publication created for specific tables", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)

			// Mock the check for existing publication
			mockStandardConn.On("QueryRow", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*bool) = false
						return nil
					},
				})

			// Mock the creation of the publication
			expectedQuery := `CREATE PUBLICATION "pg_flo_new_pub_publication" FOR TABLE "public"."users", "public"."orders"`
			mockStandardConn.On("Exec", mock.Anything, expectedQuery, mock.Anything).
				Return(pgconn.CommandTag{}, nil)

			br := &replicator.BaseReplicator{
				Config: replicator.Config{
					Group:  "new_pub",
					Schema: "public",
					Tables: []string{"users", "orders"},
				},
				StandardConn: mockStandardConn,
				Logger:       zerolog.Nop(),
			}

			err := br.CreatePublication()
			assert.NoError(t, err)
			mockStandardConn.AssertExpectations(t)
		})

		t.Run("Error checking if publication exists", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						return errors.New("database error")
					},
				})

			br := &replicator.BaseReplicator{
				Config:       replicator.Config{Group: "error_pub"},
				StandardConn: mockStandardConn,
				Logger:       zerolog.Nop(),
			}

			err := br.CreatePublication()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to check if publication exists")
			mockStandardConn.AssertExpectations(t)
		})

		t.Run("Error creating publication", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*bool) = false
						return nil
					},
				})
			expectedQuery := `CREATE PUBLICATION "pg_flo_new_pub_publication" FOR ALL TABLES`
			mockStandardConn.On("Exec", mock.Anything, expectedQuery, mock.Anything).
				Return(pgconn.CommandTag{}, errors.New("creation error"))

			br := &replicator.BaseReplicator{
				Config:       replicator.Config{Group: "new_pub"},
				StandardConn: mockStandardConn,
				Logger:       zerolog.Nop(),
			}

			err := br.CreatePublication()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to create publication")
			mockStandardConn.AssertExpectations(t)
		})
	})

	t.Run("StartReplicationFromLSN", func(t *testing.T) {
		t.Run("Successful start of replication", func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
			mockStandardConn := new(MockStandardConnection)
			mockNATSClient := new(MockNATSClient)

			mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

			keepaliveMsg := &pgproto3.CopyData{
				Data: []byte{
					pglogrepl.PrimaryKeepaliveMessageByteID,
					0, 0, 0, 0, 0, 0, 0, 8, // WAL end: 8
					0, 0, 0, 0, 0, 0, 0, 0, // ServerTime: 0
					0, // ReplyRequested: false
				},
			}

			xLogData := &pgproto3.CopyData{
				Data: []byte{
					pglogrepl.XLogDataByteID,
					0, 0, 0, 0, 0, 0, 0, 1, // WAL start: 1
					0, 0, 0, 0, 0, 0, 0, 2, // WAL end: 2
					0, 0, 0, 0, 0, 0, 0, 0, // ServerTime: 0
					'B',                    // Type
					0, 0, 0, 0, 0, 0, 0, 0, // LSN
					0, 0, 0, 0, 0, 0, 0, 0, // End LSN
					0, 0, 0, 0, 0, 0, 0, 0, // Timestamp
					0, 0, 0, 0, 0, 0, 0, 0, // XID
				},
			}

			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(keepaliveMsg, nil).Once()
			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(xLogData, nil).Once()
			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(nil, context.Canceled).Maybe()

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
				StandardConn:    mockStandardConn,
				NATSClient:      mockNATSClient,
				Config:          replicator.Config{Group: "test_pub"},
				Logger:          zerolog.Nop(),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			stopChan := make(chan struct{})
			err := br.StartReplicationFromLSN(ctx, pglogrepl.LSN(0), stopChan)
			assert.NoError(t, err, "Expected no error for graceful shutdown")
			mockReplicationConn.AssertExpectations(t)
			mockStandardConn.AssertExpectations(t)
			mockNATSClient.AssertExpectations(t)
		})

		t.Run("Error occurs while starting replication", func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
			mockStandardConn := new(MockStandardConnection)

			mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("start replication error"))

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
				StandardConn:    mockStandardConn,
				Config:          replicator.Config{Group: "test_pub"},
				Logger:          zerolog.Nop(),
			}

			stopChan := make(chan struct{})
			err := br.StartReplicationFromLSN(context.Background(), pglogrepl.LSN(0), stopChan)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to start replication")
			mockReplicationConn.AssertExpectations(t)
			mockStandardConn.AssertExpectations(t)
		})
	})

	t.Run("HandleInsertMessage", func(t *testing.T) {
		t.Run("Successful handling of InsertMessage", func(t *testing.T) {
			mockNATSClient := new(MockNATSClient)
			br := &replicator.BaseReplicator{
				NATSClient: mockNATSClient,
				Relations: map[uint32]*pglogrepl.RelationMessage{
					1: {
						RelationID:   1,
						Namespace:    "public",
						RelationName: "users",
						Columns: []*pglogrepl.RelationMessageColumn{
							{Name: "id", DataType: pgtype.Int4OID},
							{Name: "name", DataType: pgtype.TextOID},
						},
					},
				},
				Config: replicator.Config{Group: "test_pub"},
			}

			msg := &pglogrepl.InsertMessage{
				RelationID: 1,
				Tuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("1")},
						{Data: []byte("John Doe")},
					},
				},
			}

			mockNATSClient.On("PublishMessage", mock.Anything, "pgflo.test_pub", mock.MatchedBy(func(data []byte) bool {
				var decodedMsg utils.CDCMessage
				err := decodedMsg.UnmarshalBinary(data)
				if err != nil {
					t.Logf("Failed to unmarshal binary data: %v", err)
					return false
				}

				assert.Equal(t, "INSERT", decodedMsg.Type)
				assert.Equal(t, "public", decodedMsg.Schema)
				assert.Equal(t, "users", decodedMsg.Table)
				assert.Equal(t, msg.Tuple, decodedMsg.NewTuple)
				assert.Nil(t, decodedMsg.OldTuple)

				return true
			})).Return(nil)

			ctx := context.Background()
			err := br.HandleInsertMessage(ctx, msg)
			assert.NoError(t, err)

			mockNATSClient.AssertExpectations(t)

		})

		t.Run("Unknown relation ID", func(t *testing.T) {
			br := &replicator.BaseReplicator{
				Relations: make(map[uint32]*pglogrepl.RelationMessage),
			}

			msg := &pglogrepl.InsertMessage{RelationID: 999}

			ctx := context.Background()
			err := br.HandleInsertMessage(ctx, msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "unknown relation ID: 999")
		})

		t.Run("Diverse data types", func(t *testing.T) {
			testCases := []struct {
				name           string
				relationFields []struct {
					name     string
					dataType uint32
				}
				tupleData [][]byte
				expected  []map[string]interface{}
			}{
				{
					name: "Basic types",
					relationFields: []struct {
						name     string
						dataType uint32
					}{
						{"id", pgtype.Int4OID},
						{"name", pgtype.TextOID},
						{"active", pgtype.BoolOID},
						{"score", pgtype.Float8OID},
					},
					tupleData: [][]byte{
						[]byte("1"),
						[]byte("John Doe"),
						[]byte("true"),
						[]byte("9.99"),
					},
					expected: []map[string]interface{}{
						{"name": "id", "type": "int4", "value": int64(1)},
						{"name": "name", "type": "text", "value": "John Doe"},
						{"name": "active", "type": "bool", "value": true},
						{"name": "score", "type": "float8", "value": 9.990000},
					},
				},
				{
					name: "Complex types",
					relationFields: []struct {
						name     string
						dataType uint32
					}{
						{"data", pgtype.JSONBOID},
						{"tags", pgtype.TextArrayOID},
						{"image", pgtype.ByteaOID},
						{"created_at", pgtype.TimestamptzOID},
					},
					tupleData: [][]byte{
						[]byte(`{"key": "value"}`),
						[]byte("{tag1,tag2,tag3}"),
						[]byte{0x01, 0x02, 0x03, 0x04},
						[]byte("2023-05-01 12:34:56.789Z"),
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
					relationFields: []struct {
						name     string
						dataType uint32
					}{
						{"small_int", pgtype.Int2OID},
						{"big_int", pgtype.Int8OID},
						{"numeric", pgtype.Float8ArrayOID},
					},
					tupleData: [][]byte{
						[]byte("32767"),
						[]byte("9223372036854775807"),
						[]byte("123456.789"),
					},
					expected: []map[string]interface{}{
						{"name": "small_int", "type": "int2", "value": int64(32767)},
						{"name": "big_int", "type": "int8", "value": int64(9223372036854775807)},
						{"name": "numeric", "type": "float8[]", "value": "123456.789"},
					},
				},
				{
					name: "Date and time types",
					relationFields: []struct {
						name     string
						dataType uint32
					}{
						{"date", pgtype.DateOID},
						{"time", pgtype.TimeOID},
						{"interval", pgtype.IntervalOID},
					},
					tupleData: [][]byte{
						[]byte("2023-05-01"),
						[]byte("15:04:05"),
						[]byte("1 year 2 months 3 days 4 hours 5 minutes 6 seconds"),
					},
					expected: []map[string]interface{}{
						{"name": "date", "type": "date", "value": "2023-05-01"},
						{"name": "time", "type": "time", "value": "15:04:05"},
						{"name": "interval", "type": "interval", "value": "1 year 2 months 3 days 4 hours 5 minutes 6 seconds"},
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					mockNATSClient := new(MockNATSClient)

					br := &replicator.BaseReplicator{
						NATSClient: mockNATSClient,
						Relations: map[uint32]*pglogrepl.RelationMessage{
							1: {
								RelationID:   1,
								Namespace:    "public",
								RelationName: "test_table",
								Columns:      make([]*pglogrepl.RelationMessageColumn, len(tc.relationFields)),
							},
						},
						Config: replicator.Config{Group: "test_pub"},
					}

					for i, field := range tc.relationFields {
						br.Relations[1].Columns[i] = &pglogrepl.RelationMessageColumn{
							Name:     field.name,
							DataType: field.dataType,
						}
					}

					msg := &pglogrepl.InsertMessage{
						RelationID: 1,
						Tuple: &pglogrepl.TupleData{
							Columns: make([]*pglogrepl.TupleDataColumn, len(tc.tupleData)),
						},
					}

					for i, data := range tc.tupleData {
						msg.Tuple.Columns[i] = &pglogrepl.TupleDataColumn{Data: data}
					}

					mockNATSClient.On("PublishMessage", mock.Anything, "pgflo.test_pub", mock.MatchedBy(func(data []byte) bool {
						var decodedMsg utils.CDCMessage
						err := decodedMsg.UnmarshalBinary(data)
						if err != nil {
							t.Logf("Failed to unmarshal binary data: %v", err)
							return false
						}

						assert.Equal(t, "INSERT", decodedMsg.Type)
						assert.Equal(t, "public", decodedMsg.Schema)
						assert.Equal(t, "test_table", decodedMsg.Table)

						assert.Equal(t, len(tc.expected), len(decodedMsg.NewTuple.Columns))
						for i, expectedValue := range tc.expected {
							actualColumn := decodedMsg.NewTuple.Columns[i]
							expectedType := expectedValue["type"].(string)
							expectedVal := expectedValue["value"]

							assert.Equal(t, expectedType, utils.OIDToString(decodedMsg.Columns[i].DataType), "Type mismatch for field %s", decodedMsg.Columns[i].Name)

							switch expectedType {
							case "int4", "int8":
								assert.Equal(t, []byte(fmt.Sprintf("%d", expectedVal)), actualColumn.Data)
							case "float8":
								expectedFloat, _ := strconv.ParseFloat(string(actualColumn.Data), 64)
								assert.InDelta(t, expectedVal, expectedFloat, 0.000001, "Float value mismatch for field %s", decodedMsg.Columns[i].Name)
							case "bool":
								if expectedVal.(bool) {
									assert.Equal(t, []byte("true"), actualColumn.Data)
								} else {
									assert.Equal(t, []byte("false"), actualColumn.Data)
								}
							case "text", "varchar":
								assert.Equal(t, []byte(expectedVal.(string)), actualColumn.Data)
							case "jsonb":
								assert.JSONEq(t, string(expectedVal.(json.RawMessage)), string(actualColumn.Data))
							case "bytea":
								assert.Equal(t, expectedVal, actualColumn.Data)
							case "timestamptz":
								expectedTime := expectedVal.(time.Time)
								assert.Equal(t, []byte(expectedTime.Format("2006-01-02 15:04:05.999999Z07:00")), actualColumn.Data)
							default:
								assert.Equal(t, []byte(fmt.Sprintf("%v", expectedVal)), actualColumn.Data)
							}
						}

						return true
					})).Return(nil)

					ctx := context.Background()
					err := br.HandleInsertMessage(ctx, msg)
					assert.NoError(t, err)

					mockNATSClient.AssertExpectations(t)
				})
			}
		})
	})

	t.Run("HandleUpdateMessage", func(t *testing.T) {
		t.Run("Successful handling of UpdateMessage", func(t *testing.T) {
			mockNATSClient := new(MockNATSClient)
			br := &replicator.BaseReplicator{
				NATSClient: mockNATSClient,
				Relations: map[uint32]*pglogrepl.RelationMessage{
					1: {
						RelationID:   1,
						Namespace:    "public",
						RelationName: "users",
						Columns: []*pglogrepl.RelationMessageColumn{
							{Name: "id", DataType: pgtype.Int4OID},
							{Name: "name", DataType: pgtype.TextOID},
						},
					},
				},
				Config: replicator.Config{Group: "test_pub"},
			}

			msg := &pglogrepl.UpdateMessage{
				RelationID: 1,
				OldTuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("1")},
						{Data: []byte("John Doe")},
					},
				},
				NewTuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("1")},
						{Data: []byte("Jane Doe")},
					},
				},
			}

			mockNATSClient.On("PublishMessage", mock.Anything, "pgflo.test_pub", mock.MatchedBy(func(data []byte) bool {
				var decodedMsg utils.CDCMessage
				err := decodedMsg.UnmarshalBinary(data)
				if err != nil {
					t.Logf("Failed to unmarshal binary data: %v", err)
					return false
				}

				assert.Equal(t, "UPDATE", decodedMsg.Type)
				assert.Equal(t, "public", decodedMsg.Schema)
				assert.Equal(t, "users", decodedMsg.Table)
				assert.Equal(t, msg.OldTuple, decodedMsg.OldTuple)
				assert.Equal(t, msg.NewTuple, decodedMsg.NewTuple)

				return true
			})).Return(nil)

			ctx := context.Background()
			err := br.HandleUpdateMessage(ctx, msg)
			assert.NoError(t, err)

			mockNATSClient.AssertExpectations(t)
		})

		t.Run("Unknown relation ID", func(t *testing.T) {
			br := &replicator.BaseReplicator{
				Relations: make(map[uint32]*pglogrepl.RelationMessage),
			}

			msg := &pglogrepl.UpdateMessage{RelationID: 999}

			ctx := context.Background()
			err := br.HandleUpdateMessage(ctx, msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "unknown relation ID: 999")
		})

		t.Run("HandleUpdateMessage with nil OldTuple", func(t *testing.T) {
			mockNATSClient := new(MockNATSClient)

			br := &replicator.BaseReplicator{
				NATSClient: mockNATSClient,
				Relations: map[uint32]*pglogrepl.RelationMessage{
					1: {
						RelationID:   1,
						Namespace:    "public",
						RelationName: "users",
						Columns: []*pglogrepl.RelationMessageColumn{
							{Name: "id", DataType: pgtype.Int4OID},
							{Name: "name", DataType: pgtype.TextOID},
						},
					},
				},
				Config: replicator.Config{Group: "test_pub"},
			}

			msg := &pglogrepl.UpdateMessage{
				RelationID: 1,
				OldTuple:   nil,
				NewTuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("1")},
						{Data: []byte("John Doe")},
					},
				},
			}

			mockNATSClient.On("PublishMessage", mock.Anything, "pgflo.test_pub", mock.MatchedBy(func(data []byte) bool {
				var decodedMsg utils.CDCMessage
				err := decodedMsg.UnmarshalBinary(data)
				if err != nil {
					t.Logf("Failed to unmarshal binary data: %v", err)
					return false
				}

				assert.Equal(t, "UPDATE", decodedMsg.Type)
				assert.Equal(t, "public", decodedMsg.Schema)
				assert.Equal(t, "users", decodedMsg.Table)
				assert.Nil(t, decodedMsg.OldTuple)
				assert.NotNil(t, decodedMsg.NewTuple)
				assert.Equal(t, msg.NewTuple, decodedMsg.NewTuple)

				assert.Len(t, decodedMsg.Columns, 2)
				assert.Equal(t, "id", decodedMsg.Columns[0].Name)
				assert.Equal(t, uint32(pgtype.Int4OID), decodedMsg.Columns[0].DataType)
				assert.Equal(t, "name", decodedMsg.Columns[1].Name)
				assert.Equal(t, uint32(pgtype.TextOID), decodedMsg.Columns[1].DataType)

				return true
			})).Return(nil)

			ctx := context.Background()
			err := br.HandleUpdateMessage(ctx, msg)
			assert.NoError(t, err)

			mockNATSClient.AssertExpectations(t)
		})
	})

	t.Run("HandleDeleteMessage", func(t *testing.T) {
		t.Run("Successful handling of DeleteMessage", func(t *testing.T) {
			mockNATSClient := new(MockNATSClient)
			br := &replicator.BaseReplicator{
				NATSClient: mockNATSClient,
				Relations: map[uint32]*pglogrepl.RelationMessage{
					1: {
						RelationID:   1,
						Namespace:    "public",
						RelationName: "users",
						Columns: []*pglogrepl.RelationMessageColumn{
							{Name: "id", DataType: pgtype.Int4OID},
							{Name: "name", DataType: pgtype.TextOID},
						},
					},
				},
				Config: replicator.Config{Group: "test_pub"},
			}

			msg := &pglogrepl.DeleteMessage{
				RelationID: 1,
				OldTuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("1")},
						{Data: []byte("John Doe")},
					},
				},
			}

			mockNATSClient.On("PublishMessage", mock.Anything, "pgflo.test_pub", mock.MatchedBy(func(data []byte) bool {
				var decodedMsg utils.CDCMessage
				err := decodedMsg.UnmarshalBinary(data)
				if err != nil {
					t.Logf("Failed to unmarshal binary data: %v", err)
					return false
				}

				assert.Equal(t, "DELETE", decodedMsg.Type)
				assert.Equal(t, "public", decodedMsg.Schema)
				assert.Equal(t, "users", decodedMsg.Table)
				assert.Equal(t, msg.OldTuple, decodedMsg.OldTuple)
				assert.Nil(t, decodedMsg.NewTuple)

				return true
			})).Return(nil)

			ctx := context.Background()
			err := br.HandleDeleteMessage(ctx, msg)
			assert.NoError(t, err)

			mockNATSClient.AssertExpectations(t)
		})

		t.Run("Unknown relation ID", func(t *testing.T) {
			br := &replicator.BaseReplicator{
				Relations: make(map[uint32]*pglogrepl.RelationMessage),
			}

			msg := &pglogrepl.DeleteMessage{RelationID: 999}

			ctx := context.Background()
			err := br.HandleDeleteMessage(ctx, msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "unknown relation ID: 999")
		})
	})

	t.Run("HandleCommitMessage", func(t *testing.T) {
		t.Run("Successful handling of CommitMessage", func(t *testing.T) {
			mockNATSClient := new(MockNATSClient)

			br := &replicator.BaseReplicator{
				NATSClient: mockNATSClient,
				Logger:     zerolog.Nop(),
			}

			msg := &pglogrepl.CommitMessage{
				CommitTime: time.Now(),
				CommitLSN:  12345,
			}

			mockNATSClient.On("GetState", mock.Anything).Return(pgflonats.State{}, nil)

			mockNATSClient.On("SaveState", mock.Anything, pgflonats.State{LSN: pglogrepl.LSN(12345)}).Return(nil)

			ctx := context.Background()
			err := br.HandleCommitMessage(ctx, msg)
			assert.NoError(t, err)

			mockNATSClient.AssertExpectations(t)
		})
	})

	t.Run("PublishToNATS", func(t *testing.T) {
		t.Run("Successful publishing to NATS", func(t *testing.T) {
			mockNATSClient := new(MockNATSClient)

			br := &replicator.BaseReplicator{
				NATSClient: mockNATSClient,
				Config: replicator.Config{
					Group: "test_group",
				},
			}

			data := utils.CDCMessage{
				Type:   "INSERT",
				Schema: "public",
				Table:  "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "name", DataType: pgtype.TextOID},
				},
				NewTuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte{0, 0, 0, 1}}, // int4 representation of 1
						{Data: []byte("John Doe")},
					},
				},
			}

			mockNATSClient.On("PublishMessage", mock.Anything, "pgflo.test_group", mock.MatchedBy(func(data []byte) bool {
				var decodedMsg utils.CDCMessage
				err := decodedMsg.UnmarshalBinary(data)
				if err != nil {
					t.Logf("Failed to unmarshal binary data: %v", err)
					return false
				}

				assert.Equal(t, "INSERT", decodedMsg.Type)
				assert.Equal(t, "public", decodedMsg.Schema)
				assert.Equal(t, "users", decodedMsg.Table)

				assert.Len(t, decodedMsg.Columns, 2)
				assert.Equal(t, "id", decodedMsg.Columns[0].Name)
				assert.Equal(t, uint32(pgtype.Int4OID), decodedMsg.Columns[0].DataType)
				assert.Equal(t, "name", decodedMsg.Columns[1].Name)
				assert.Equal(t, uint32(pgtype.TextOID), decodedMsg.Columns[1].DataType)

				assert.NotNil(t, decodedMsg.NewTuple)
				assert.Len(t, decodedMsg.NewTuple.Columns, 2)
				assert.Equal(t, []byte{0, 0, 0, 1}, decodedMsg.NewTuple.Columns[0].Data)
				assert.Equal(t, []byte("John Doe"), decodedMsg.NewTuple.Columns[1].Data)

				return true
			})).Return(nil)

			ctx := context.Background()
			err := br.PublishToNATS(ctx, data)
			assert.NoError(t, err)

			mockNATSClient.AssertExpectations(t)
		})

		t.Run("Error publishing to NATS", func(t *testing.T) {
			mockNATSClient := new(MockNATSClient)

			br := &replicator.BaseReplicator{
				NATSClient: mockNATSClient,
				Config: replicator.Config{
					Group: "test_group",
				},
			}

			data := utils.CDCMessage{
				Type:   "INSERT",
				Schema: "public",
				Table:  "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "name", DataType: pgtype.TextOID},
				},
				NewTuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte{0, 0, 0, 1}}, // int4 representation of 1
						{Data: []byte("John Doe")},
					},
				},
			}

			mockNATSClient.On("PublishMessage", mock.Anything, "pgflo.test_group", mock.Anything).Return(errors.New("failed to publish message"))

			ctx := context.Background()
			err := br.PublishToNATS(ctx, data)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to publish message")

			mockNATSClient.AssertExpectations(t)
		})
	})

	t.Run("AddPrimaryKeyInfo", func(t *testing.T) {
		t.Run("Successful addition of primary key info", func(t *testing.T) {
			br := &replicator.BaseReplicator{
				TableDetails: map[string][]string{
					"public.users": {"id"},
				},
			}

			message := &utils.CDCMessage{
				Schema: "public",
				Table:  "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "name", DataType: pgtype.TextOID},
				},
				NewTuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte{0, 0, 0, 1}}, // int4 representation of 1
						{Data: []byte("John Doe")},
					},
				},
			}

			expected := &utils.CDCMessage{
				Schema: "public",
				Table:  "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "name", DataType: pgtype.TextOID},
				},
				NewTuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte{0, 0, 0, 1}}, // int4 representation of 1
						{Data: []byte("John Doe")},
					},
				},
				PrimaryKeyColumn: "id",
			}

			br.AddPrimaryKeyInfo(message, "public.users")
			assert.Equal(t, expected, message)
		})
	})

	t.Run("SendStandbyStatusUpdate", func(t *testing.T) {
		t.Run("Successful sending of standby status update", func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
			mockReplicationConn.On("SendStandbyStatusUpdate", mock.Anything, mock.Anything).Return(nil)

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
			}

			err := br.SendStandbyStatusUpdate(context.Background())
			assert.NoError(t, err)

			mockReplicationConn.AssertExpectations(t)
		})

		t.Run("Error occurs while sending status update", func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
				LastLSN:         pglogrepl.LSN(100),
				Logger:          zerolog.New(ioutil.Discard),
			}

			mockReplicationConn.On("SendStandbyStatusUpdate",
				mock.Anything,
				mock.Anything,
			).Return(errors.New("send error"))

			err := br.SendStandbyStatusUpdate(context.Background())
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to send standby status update")
			mockReplicationConn.AssertExpectations(t)
		})
	})

	t.Run("CreateReplicationSlot", func(t *testing.T) {
		t.Run("Slot already exists", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockReplicationConn := new(MockReplicationConnection)

			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*bool) = true
						return nil
					},
				})

			br := &replicator.BaseReplicator{
				StandardConn:    mockStandardConn,
				ReplicationConn: mockReplicationConn,
				Config: replicator.Config{
					Group: "test_group",
				},
			}

			err := br.CreateReplicationSlot(context.Background())
			assert.NoError(t, err)

			mockStandardConn.AssertExpectations(t)
			mockReplicationConn.AssertExpectations(t)
		})

		t.Run("Slot created successfully", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockReplicationConn := new(MockReplicationConnection)

			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*bool) = false
						return nil
					},
				})

			mockReplicationConn.On("CreateReplicationSlot", mock.Anything, mock.Anything).
				Return(pglogrepl.CreateReplicationSlotResult{
					SlotName:        "test_group_publication",
					ConsistentPoint: "0/0",
					SnapshotName:    "snapshot_name",
				}, nil)

			br := &replicator.BaseReplicator{
				StandardConn:    mockStandardConn,
				ReplicationConn: mockReplicationConn,
				Config: replicator.Config{
					Group: "test_group",
				},
				Logger: zerolog.Nop(),
			}

			err := br.CreateReplicationSlot(context.Background())
			assert.NoError(t, err)

			mockStandardConn.AssertExpectations(t)
			mockReplicationConn.AssertExpectations(t)
		})

		t.Run("Error creating slot", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockReplicationConn := new(MockReplicationConnection)

			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*bool) = false
						return nil
					},
				})

			mockReplicationConn.On("CreateReplicationSlot", mock.Anything, mock.Anything).
				Return(pglogrepl.CreateReplicationSlotResult{}, errors.New("create error"))

			br := &replicator.BaseReplicator{
				StandardConn:    mockStandardConn,
				ReplicationConn: mockReplicationConn,
				Config: replicator.Config{
					Group: "test_group",
				},
				Logger: zerolog.Nop(),
			}

			err := br.CreateReplicationSlot(context.Background())
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to create replication slot")

			mockStandardConn.AssertExpectations(t)
			mockReplicationConn.AssertExpectations(t)
		})
	})

	t.Run("CheckReplicationSlotExists", func(t *testing.T) {
		t.Run("Slot exists", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(MockRow{
				scanFunc: func(dest ...interface{}) error {
					*dest[0].(*bool) = true
					return nil
				},
			})

			br := &replicator.BaseReplicator{
				StandardConn: mockStandardConn,
			}

			exists, err := br.CheckReplicationSlotExists("test_slot")
			assert.NoError(t, err)
			assert.True(t, exists)

			mockStandardConn.AssertExpectations(t)
		})

		t.Run("Slot does not exist", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(MockRow{
				scanFunc: func(dest ...interface{}) error {
					*dest[0].(*bool) = false
					return nil
				},
			})

			br := &replicator.BaseReplicator{
				StandardConn: mockStandardConn,
			}

			exists, err := br.CheckReplicationSlotExists("test_slot")
			assert.NoError(t, err)
			assert.False(t, exists)

			mockStandardConn.AssertExpectations(t)
		})

		t.Run("Error checking slot existence", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(MockRow{
				scanFunc: func(dest ...interface{}) error {
					return errors.New("query error")
				},
			})

			br := &replicator.BaseReplicator{
				StandardConn: mockStandardConn,
			}

			_, err := br.CheckReplicationSlotExists("test_slot")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "error checking replication slot")

			mockStandardConn.AssertExpectations(t)
		})
	})
}
