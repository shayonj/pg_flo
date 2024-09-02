package replicator_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"
	"github.com/shayonj/pg_flo/pkg/replicator"
	"github.com/shayonj/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCopyAndStreamReplicator(t *testing.T) {
	t.Run("CreatePublication", func(t *testing.T) {
		mockStandardConn := new(MockStandardConnection)

		mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", mock.Anything).Return(MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*bool) = false
				return nil
			},
		})

		mockStandardConn.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(pgconn.CommandTag{}, nil)

		csr := &replicator.CopyAndStreamReplicator{
			BaseReplicator: replicator.BaseReplicator{
				StandardConn: mockStandardConn,
				Config: replicator.Config{
					Group:  "test_publication",
					Tables: []string{"users"},
					Schema: "public",
				},
				Logger: zerolog.Nop(),
			},
		}

		err := csr.CreatePublication()
		assert.NoError(t, err)
		mockStandardConn.AssertExpectations(t)
	})

	t.Run("StartReplication", func(t *testing.T) {
		mockReplicationConn := new(MockReplicationConnection)
		mockStandardConn := new(MockStandardConnection)
		mockSink := new(MockSink)
		mockTx := new(MockTx)

		mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", mock.Anything).Return(MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*bool) = false
				return nil
			},
		})

		mockReplicationConn.On("CreateReplicationSlot", mock.Anything, mock.Anything).Return(pglogrepl.CreateReplicationSlotResult{}, nil)
		mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
			context.Canceled,
		)

		mockStandardConn.On("BeginTx", mock.Anything, mock.AnythingOfType("pgx.TxOptions")).Return(mockTx, nil)

		mockStandardConn.On("QueryRow", mock.Anything, mock.MatchedBy(func(query string) bool {
			return strings.Contains(query, "SELECT relpages")
		}), mock.Anything).Return(MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*uint32) = 1 // Mock 1 page for simplicity
				return nil
			},
		})

		mockTx.On("QueryRow", mock.Anything, mock.MatchedBy(func(query string) bool {
			return strings.Contains(query, "pg_export_snapshot()") && strings.Contains(query, "pg_current_wal_lsn()")
		})).Return(MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*string) = "mock-snapshot-id"
				*dest[1].(*pglogrepl.LSN) = pglogrepl.LSN(100)
				return nil
			},
		})

		mockStandardConn.On("Exec",
			mock.Anything,
			mock.MatchedBy(func(query string) bool {
				return strings.HasPrefix(query, "ANALYZE")
			}),
			mock.Anything,
		).Return(pgconn.CommandTag{}, nil)

		mockTx.On("Commit", mock.Anything).Return(nil)

		csr := &replicator.CopyAndStreamReplicator{
			BaseReplicator: replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
				StandardConn:    mockStandardConn,
				Sink:            mockSink,
				Logger:          zerolog.Nop(),
				Config:          replicator.Config{Group: "test_publication", Tables: []string{"users"}, Schema: "public"},
			},
		}

		errChan := make(chan error)
		go func() {
			errChan <- csr.StartReplication()
		}()

		time.Sleep(100 * time.Millisecond)

		err := <-errChan

		assert.NoError(t, err)
		mockReplicationConn.AssertExpectations(t)
		mockStandardConn.AssertExpectations(t)
		mockSink.AssertExpectations(t)
		mockTx.AssertExpectations(t)
	})

	t.Run("CopyTable", func(t *testing.T) {
		mockStandardConn := new(MockStandardConnection)
		mockSink := new(MockSink)
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

		var capturedData []interface{}
		mockSink.On("WriteBatch", mock.Anything).Run(func(args mock.Arguments) {
			capturedData = args.Get(0).([]interface{})
		}).Return(nil)
		mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

		csr := &replicator.CopyAndStreamReplicator{
			BaseReplicator: replicator.BaseReplicator{
				StandardConn: mockStandardConn,
				Sink:         mockSink,
				Logger:       zerolog.Nop(),
				Config: replicator.Config{
					Tables:   []string{"users"},
					Schema:   "public",
					Host:     "localhost",
					Port:     5432,
					User:     "testuser",
					Password: "testpassword",
					Database: "testdb",
				},
				Buffer: replicator.NewBuffer(1000, 5*time.Second),
			},
			MaxCopyWorkersPerTable: 2,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := csr.CopyTable(ctx, "users", "snapshot-1")
		assert.NoError(t, err)

		time.Sleep(500 * time.Millisecond)

		assert.NotEmpty(t, capturedData, "WriteBatch should have been called")
		if len(capturedData) > 0 {
			var actualData map[string]interface{}
			actualData = capturedData[0].(map[string]interface{})
			assert.NoError(t, err)

			newRow := actualData["new_row"].(map[string]utils.CDCValue)
			convertedNewRow := make(map[string]interface{})
			for k, v := range newRow {
				convertedNewRow[k] = map[string]interface{}{
					"type":  utils.OIDToString(v.Type),
					"value": v.Value,
				}
			}
			actualData["new_row"] = convertedNewRow

			expectedData := map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "users",
				"new_row": map[string]interface{}{
					"id":   map[string]interface{}{"type": "int4", "value": int(1)},
					"name": map[string]interface{}{"type": "text", "value": "John Doe"},
				},
			}

			assert.Equal(t, expectedData, actualData)
		}

		mockStandardConn.AssertExpectations(t)
		mockSink.AssertExpectations(t)
		mockPoolConn.AssertExpectations(t)
		mockTx.AssertExpectations(t)
		mockRows.AssertExpectations(t)
	})

	t.Run("CopyTableRange", func(t *testing.T) {
		mockStandardConn := new(MockStandardConnection)
		mockSink := new(MockSink)
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

		var capturedData []interface{}
		mockSink.On("WriteBatch", mock.Anything).Run(func(args mock.Arguments) {
			capturedData = args.Get(0).([]interface{})
		}).Return(nil)

		mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

		csr := &replicator.CopyAndStreamReplicator{
			BaseReplicator: replicator.BaseReplicator{
				StandardConn: mockStandardConn,
				Sink:         mockSink,
				Logger:       zerolog.Nop(),
				Config: replicator.Config{
					Tables:   []string{"users"},
					Schema:   "public",
					Host:     "localhost",
					Port:     5432,
					User:     "testuser",
					Password: "testpassword",
					Database: "testdb",
				},
				Buffer: replicator.NewBuffer(1000, 5*time.Second),
			},
		}

		rowsCopied, err := csr.CopyTableRange(context.Background(), "users", 0, 1000, "snapshot-1", 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rowsCopied)
		time.Sleep(100 * time.Millisecond)

		assert.NotEmpty(t, capturedData, "WriteBatch should have been called")

		if len(capturedData) > 0 {
			var actualData map[string]interface{}
			actualData = capturedData[0].(map[string]interface{})
			assert.NoError(t, err)

			newRow := actualData["new_row"].(map[string]utils.CDCValue)
			convertedNewRow := make(map[string]interface{})
			for k, v := range newRow {
				convertedNewRow[k] = map[string]interface{}{
					"type":  utils.OIDToString(v.Type),
					"value": v.Value,
				}
			}
			actualData["new_row"] = convertedNewRow

			expectedData := map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "users",
				"new_row": map[string]interface{}{
					"id":   map[string]interface{}{"type": "int4", "value": int(1)},
					"name": map[string]interface{}{"type": "text", "value": "John Doe"},
				},
			}

			assert.Equal(t, expectedData, actualData)
		}

		mockStandardConn.AssertExpectations(t)
		mockPoolConn.AssertExpectations(t)
		mockTx.AssertExpectations(t)
		mockRows.AssertExpectations(t)
		mockSink.AssertExpectations(t)
	})
}
