package replicator_test

import (
	"context"
	"errors"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"
	"github.com/shayonj/pg_flo/pkg/replicator"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/mock"
)

func newTestRuleEngine() *rules.RuleEngine {
	ruleEngine := rules.NewRuleEngine()

	// Add a dummy rule that always passes
	dummyRule := &rules.FilterRule{
		TableName:  "*",
		ColumnName: "*",
		Condition:  func(utils.CDCValue) bool { return true },
		Operations: []rules.OperationType{rules.OperationInsert, rules.OperationUpdate, rules.OperationDelete},
	}

	ruleEngine.AddRule("*", dummyRule)

	return ruleEngine
}

func TestBaseReplicator(t *testing.T) {
	t.Run("NewBaseReplicator", func(t *testing.T) {
		mockReplicationConn := new(MockReplicationConnection)
		mockStandardConn := new(MockStandardConnection)
		mockSink := new(MockSink)
		ruleEngine := newTestRuleEngine()

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

		br := replicator.NewBaseReplicator(config, mockSink, mockReplicationConn, mockStandardConn, ruleEngine)

		assert.NotNil(t, br)
		assert.Equal(t, config, br.Config)
		assert.Equal(t, mockSink, br.Sink)
		assert.Equal(t, mockReplicationConn, br.ReplicationConn)
		assert.Equal(t, mockStandardConn, br.StandardConn)
		assert.Equal(t, ruleEngine, br.RuleEngine)

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
				Logger:       zerolog.New(ioutil.Discard),
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
			mockStandardConn.On("Exec", mock.Anything, "CREATE PUBLICATION new_pub_publication FOR ALL TABLES", mock.Anything).
				Return(pgconn.CommandTag{}, nil)

			br := &replicator.BaseReplicator{
				Config:       replicator.Config{Group: "new_pub"},
				StandardConn: mockStandardConn,
				Logger:       zerolog.New(ioutil.Discard),
			}

			err := br.CreatePublication()
			assert.NoError(t, err)
			mockStandardConn.AssertExpectations(t)
		})

		t.Run("Publication created for specific tables", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*bool) = false
						return nil
					},
				})
			mockStandardConn.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
				Return(pgconn.CommandTag{}, nil)

			br := &replicator.BaseReplicator{
				Config: replicator.Config{
					Group:  "new_pub",
					Tables: []string{"users", "orders"},
				},
				StandardConn: mockStandardConn,
				Logger:       zerolog.New(ioutil.Discard),
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
				Logger:       zerolog.New(ioutil.Discard),
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
			mockStandardConn.On("Exec", mock.Anything, "CREATE PUBLICATION new_pub_publication FOR ALL TABLES", mock.Anything).
				Return(pgconn.CommandTag{}, errors.New("creation error"))

			br := &replicator.BaseReplicator{
				Config:       replicator.Config{Group: "new-pub"},
				StandardConn: mockStandardConn,
				Logger:       zerolog.New(ioutil.Discard),
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
					0, 0, 0, 0, // XID
				},
			}

			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(keepaliveMsg, nil).Once()
			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(xLogData, nil).Once()
			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(nil, context.Canceled).Maybe()

			mockSink := new(MockSink)
			mockSink.On("GetLastLSN").Return(pglogrepl.LSN(0), nil).Maybe()
			mockSink.On("SetLastLSN", mock.Anything).Return(nil).Maybe()
			mockSink.On("WriteBatch", mock.AnythingOfType("[][]byte")).Return(nil).Maybe()
			mockSink.On("Commit", mock.Anything).Return(nil).Maybe()

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
				Sink:            mockSink,
				Config:          replicator.Config{Group: "test_pub"},
				Logger:          zerolog.New(ioutil.Discard),
				Buffer:          replicator.NewBuffer(1000, 5*time.Second),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			err := br.StartReplicationFromLSN(ctx, pglogrepl.LSN(0))
			assert.NoError(t, err, "Expected no error for graceful shutdown")
			mockReplicationConn.AssertExpectations(t)
			mockSink.AssertExpectations(t)
		})

		t.Run("Error occurs while starting replication", func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
			mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("start replication error"))

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
				Config:          replicator.Config{Group: "test_pub"},
			}

			err := br.StartReplicationFromLSN(context.Background(), pglogrepl.LSN(0))
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to start replication")
			mockReplicationConn.AssertExpectations(t)
		})
	})

	t.Run("StreamChanges", func(t *testing.T) {
		t.Run("Successful processing of messages", func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
			keepaliveMsg := &pgproto3.CopyData{
				Data: []byte{
					pglogrepl.PrimaryKeepaliveMessageByteID,
					0, 0, 0, 0, 0, 0, 0, 8, // WAL end: 8
					0, 0, 0, 0, 0, 0, 0, 0, // ServerTime: 0
					0, // ReplyRequested: false
				},
			}
			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(keepaliveMsg, nil).Once()
			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(nil, context.Canceled).Once()

			mockSink := new(MockSink)
			mockSink.On("GetLastLSN").Return(pglogrepl.LSN(0), nil).Maybe()
			mockSink.On("SetLastLSN", mock.Anything).Return(nil).Maybe()

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
				Sink:            mockSink,
				Logger:          zerolog.New(ioutil.Discard),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := br.StreamChanges(ctx)
			assert.NoError(t, err, "Expected no error for graceful shutdown")
			mockReplicationConn.AssertExpectations(t)
			mockSink.AssertExpectations(t)
		})

		t.Run("Context cancellation", func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
			mockSink := new(MockSink)
			mockSink.On("GetLastLSN").Return(pglogrepl.LSN(0), nil).Maybe()
			mockSink.On("SetLastLSN", mock.Anything).Return(nil).Maybe()

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
				Sink:            mockSink,
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := br.StreamChanges(ctx)
			assert.NoError(t, err, "Expected no error for graceful shutdown")
			mockReplicationConn.AssertExpectations(t)
			mockSink.AssertExpectations(t)
		})
	})

	t.Run("ProcessNextMessage", func(t *testing.T) {
		t.Run("Successful processing of CopyData message", func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
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
					0, 0, 0, 0, // XID
				},
			}
			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(xLogData, nil)

			mockSink := new(MockSink)

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
				Sink:            mockSink,
				Logger:          zerolog.New(ioutil.Discard),
				Buffer:          replicator.NewBuffer(1000, 5*time.Second),
			}

			lastStatusUpdate := time.Now()
			err := br.ProcessNextMessage(context.Background(), &lastStatusUpdate, time.Second)
			assert.NoError(t, err)
			mockReplicationConn.AssertExpectations(t)
			mockSink.AssertExpectations(t)

			assert.True(t, lastStatusUpdate.After(time.Now().Add(-time.Second)), "lastStatusUpdate should have been updated")
		})

		t.Run("Successful processing of other message types", func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(&pgproto3.NoticeResponse{}, nil)

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
			}

			lastStatusUpdate := time.Now()
			err := br.ProcessNextMessage(context.Background(), &lastStatusUpdate, time.Second)
			assert.NoError(t, err)
			mockReplicationConn.AssertExpectations(t)
		})

		t.Run("Error occurs while receiving message", func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
			mockReplicationConn.On("ReceiveMessage", mock.Anything).Return(nil, errors.New("receive error"))

			br := &replicator.BaseReplicator{
				ReplicationConn: mockReplicationConn,
			}

			lastStatusUpdate := time.Now()
			err := br.ProcessNextMessage(context.Background(), &lastStatusUpdate, time.Second)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to receive message")
			mockReplicationConn.AssertExpectations(t)
		})
	})

	t.Run("HandleInsertMessage", func(t *testing.T) {
		t.Run("Successful handling of InsertMessage", func(t *testing.T) {
			mockSink := new(MockSink)

			br := &replicator.BaseReplicator{
				Sink: mockSink,
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
				Buffer:     replicator.NewBuffer(1000, 5*time.Second),
				RuleEngine: newTestRuleEngine(),
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

			expected := map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "users",
				"new_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(1)},
					"name": {Type: pgtype.TextOID, Value: "John Doe"},
				},
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], expected)
			})).Return(nil)

			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err := br.HandleInsertMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)
		})

		t.Run("Successful handling of InsertMessage with rule engine", func(t *testing.T) {
			mockSink := new(MockSink)
			ruleEngine := newTestRuleEngine()

			br := &replicator.BaseReplicator{
				Sink:       mockSink,
				RuleEngine: ruleEngine,
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
				Buffer: replicator.NewBuffer(1000, 5*time.Second),
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

			expected := map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "users",
				"new_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(1)},
					"name": {Type: pgtype.TextOID, Value: "John Doe"},
				},
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], expected)
			})).Return(nil)

			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err := br.HandleInsertMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)

		})

		t.Run("Unknown relation ID", func(t *testing.T) {
			br := &replicator.BaseReplicator{
				Relations: make(map[uint32]*pglogrepl.RelationMessage),
			}

			msg := &pglogrepl.InsertMessage{RelationID: 999}

			err := br.HandleInsertMessage(msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "unknown relation ID: 999")
		})
	})

	t.Run("HandleUpdateMessage", func(t *testing.T) {
		t.Run("Successful handling of UpdateMessage", func(t *testing.T) {
			mockSink := new(MockSink)

			br := &replicator.BaseReplicator{
				Sink: mockSink,
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
				Buffer:     replicator.NewBuffer(1000, 5*time.Second),
				RuleEngine: newTestRuleEngine(),
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

			expected := map[string]interface{}{
				"type":   "UPDATE",
				"schema": "public",
				"table":  "users",
				"old_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(1)},
					"name": {Type: pgtype.TextOID, Value: "John Doe"},
				},
				"new_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(1)},
					"name": {Type: pgtype.TextOID, Value: "Jane Doe"},
				},
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], expected)
			})).Return(nil)

			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err := br.HandleUpdateMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)
		})

		t.Run("Successful handling of UpdateMessage with rule engine", func(t *testing.T) {
			mockSink := new(MockSink)
			ruleEngine := newTestRuleEngine()

			br := &replicator.BaseReplicator{
				Sink:       mockSink,
				RuleEngine: ruleEngine,
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
				Buffer: replicator.NewBuffer(1000, 5*time.Second),
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

			expected := map[string]interface{}{
				"type":   "UPDATE",
				"schema": "public",
				"table":  "users",
				"old_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(1)},
					"name": {Type: pgtype.TextOID, Value: "John Doe"},
				},
				"new_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(1)},
					"name": {Type: pgtype.TextOID, Value: "Jane Doe"},
				},
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], expected)
			})).Return(nil)

			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err := br.HandleUpdateMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)

		})

		t.Run("Unknown relation ID", func(t *testing.T) {
			br := &replicator.BaseReplicator{
				Relations:  make(map[uint32]*pglogrepl.RelationMessage),
				RuleEngine: newTestRuleEngine(),
			}

			msg := &pglogrepl.UpdateMessage{RelationID: 999}

			err := br.HandleUpdateMessage(msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "unknown relation ID: 999")
		})

		t.Run("Update with nil OldTuple", func(t *testing.T) {
			mockSink := new(MockSink)

			br := &replicator.BaseReplicator{
				Sink: mockSink,
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
				Buffer:     replicator.NewBuffer(1000, 5*time.Second),
				RuleEngine: newTestRuleEngine(),
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

			expected := map[string]interface{}{
				"type":    "UPDATE",
				"schema":  "public",
				"table":   "users",
				"old_row": map[string]utils.CDCValue{},
				"new_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(1)},
					"name": {Type: pgtype.TextOID, Value: "John Doe"},
				},
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], expected)
			})).Return(nil)
			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err := br.HandleUpdateMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)
		})

		t.Run("Update with nil NewTuple", func(t *testing.T) {
			mockSink := new(MockSink)

			br := &replicator.BaseReplicator{
				Sink: mockSink,
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
				Buffer:     replicator.NewBuffer(1000, 5*time.Second),
				RuleEngine: newTestRuleEngine(),
			}

			msg := &pglogrepl.UpdateMessage{
				RelationID: 1,
				OldTuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("1")},
						{Data: []byte("John Doe")},
					},
				},
				NewTuple: nil,
			}

			expected := map[string]interface{}{
				"type":   "UPDATE",
				"schema": "public",
				"table":  "users",
				"old_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(1)},
					"name": {Type: pgtype.TextOID, Value: "John Doe"},
				},
				"new_row": map[string]utils.CDCValue{},
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], expected)
			})).Return(nil)
			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err := br.HandleUpdateMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)
		})
	})

	t.Run("HandleDeleteMessage", func(t *testing.T) {
		t.Run("Successful handling of DeleteMessage", func(t *testing.T) {
			mockSink := new(MockSink)

			br := &replicator.BaseReplicator{
				Sink: mockSink,
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
				Buffer:     replicator.NewBuffer(1000, 5*time.Second),
				RuleEngine: newTestRuleEngine(),
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

			expected := map[string]interface{}{
				"type":   "DELETE",
				"schema": "public",
				"table":  "users",
				"old_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(1)},
					"name": {Type: pgtype.TextOID, Value: "John Doe"},
				},
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], expected)
			})).Return(nil)
			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err := br.HandleDeleteMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)
		})

		t.Run("Successful handling of DeleteMessage with rule engine", func(t *testing.T) {
			mockSink := new(MockSink)
			ruleEngine := newTestRuleEngine()

			br := &replicator.BaseReplicator{
				Sink:       mockSink,
				RuleEngine: ruleEngine,
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
				Buffer: replicator.NewBuffer(1000, 5*time.Second),
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

			expected := map[string]interface{}{
				"type":   "DELETE",
				"schema": "public",
				"table":  "users",
				"old_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(1)},
					"name": {Type: pgtype.TextOID, Value: "John Doe"},
				},
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], expected)
			})).Return(nil)
			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err := br.HandleDeleteMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)

		})

		t.Run("Unknown relation ID", func(t *testing.T) {
			br := &replicator.BaseReplicator{
				Relations: make(map[uint32]*pglogrepl.RelationMessage),
			}

			msg := &pglogrepl.DeleteMessage{RelationID: 999}

			err := br.HandleDeleteMessage(msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "unknown relation ID: 999")
		})
	})

	t.Run("ConvertTupleDataToMap", func(t *testing.T) {
		br := &replicator.BaseReplicator{
			Logger: zerolog.New(ioutil.Discard),
		}

		testCases := []struct {
			name     string
			relation *pglogrepl.RelationMessage
			tuple    *pglogrepl.TupleData
			expected map[string]utils.CDCValue
		}{
			{
				name: "With various data types",
				relation: &pglogrepl.RelationMessage{
					Columns: []*pglogrepl.RelationMessageColumn{
						{Name: "id", DataType: pgtype.Int4OID},
						{Name: "name", DataType: pgtype.TextOID},
						{Name: "active", DataType: pgtype.BoolOID},
						{Name: "score", DataType: pgtype.Float8OID},
						{Name: "created_at", DataType: pgtype.TimestamptzOID},
						{Name: "data", DataType: pgtype.JSONBOID},
					},
				},
				tuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("1")},
						{Data: []byte("John Doe")},
						{Data: []byte("t")},
						{Data: []byte("9.5")},
						{Data: []byte("2023-04-13 10:30:00+00")},
						{Data: []byte(`{"key": "value"}`)},
					},
				},
				expected: map[string]utils.CDCValue{
					"id":         utils.CDCValue{Type: pgtype.Int4OID, Value: int64(1)},
					"name":       utils.CDCValue{Type: pgtype.TextOID, Value: "John Doe"},
					"active":     utils.CDCValue{Type: pgtype.BoolOID, Value: "t"},
					"score":      utils.CDCValue{Type: pgtype.Float8OID, Value: "9.5"},
					"created_at": utils.CDCValue{Type: pgtype.TimestamptzOID, Value: "2023-04-13T10:30:00Z"},
					"data":       utils.CDCValue{Type: pgtype.JSONBOID, Value: `{"key": "value"}`},
				},
			},
			{
				name: "With various timestamp formats",
				relation: &pglogrepl.RelationMessage{
					Columns: []*pglogrepl.RelationMessageColumn{
						{Name: "timestamp_with_tz", DataType: pgtype.TimestamptzOID},
						{Name: "timestamp_without_tz", DataType: pgtype.TimestampOID},
					},
				},
				tuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("2024-08-17 17:11:24.259862+00")},
						{Data: []byte("2024-08-17 17:11:24.259862")},
					},
				},
				expected: map[string]utils.CDCValue{
					"timestamp_with_tz":    utils.CDCValue{Type: pgtype.TimestamptzOID, Value: "2024-08-17T17:11:24.259862Z"},
					"timestamp_without_tz": utils.CDCValue{Type: pgtype.TimestampOID, Value: "2024-08-17T17:11:24.259862Z"},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := br.ConvertTupleDataToMap(tc.relation, tc.tuple)
				assert.Equal(t, tc.expected, result)
			})
		}
	})

	t.Run("SendStandbyStatusUpdate", func(t *testing.T) {
		t.Run("Successful sending of status update", func(t *testing.T) {
			mockSink := new(MockSink)
			mockReplicationConn := new(MockReplicationConnection)

			br := &replicator.BaseReplicator{
				Sink:            mockSink,
				ReplicationConn: mockReplicationConn,
				LastLSN:         pglogrepl.LSN(100),
				Buffer:          replicator.NewBuffer(1000, 5*time.Second),
				Logger:          zerolog.New(ioutil.Discard),
			}

			mockReplicationConn.On("SendStandbyStatusUpdate",
				mock.Anything,
				mock.MatchedBy(func(update pglogrepl.StandbyStatusUpdate) bool {
					return update.WALWritePosition == pglogrepl.LSN(101) &&
						update.WALFlushPosition == pglogrepl.LSN(101) &&
						update.WALApplyPosition == pglogrepl.LSN(101) &&
						!update.ReplyRequested
				}),
			).Return(nil)

			err := br.SendStandbyStatusUpdate(context.Background())
			assert.NoError(t, err)
			mockReplicationConn.AssertExpectations(t)
		})

		t.Run("Error occurs while sending status update", func(t *testing.T) {
			mockSink := new(MockSink)
			mockReplicationConn := new(MockReplicationConnection)

			br := &replicator.BaseReplicator{
				Sink:            mockSink,
				ReplicationConn: mockReplicationConn,
				LastLSN:         pglogrepl.LSN(100),
				Buffer:          replicator.NewBuffer(1000, 5*time.Second),
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
		t.Run("Replication slot already exists", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*bool) = true
						return nil
					},
				})

			br := &replicator.BaseReplicator{
				StandardConn: mockStandardConn,
				Buffer:       replicator.NewBuffer(1000, 5*time.Second),
				Config:       replicator.Config{Group: "existing_slot"},
			}

			err := br.CreateReplicationSlot(context.Background())
			assert.NoError(t, err)
			mockStandardConn.AssertExpectations(t)
		})

		t.Run("Replication slot doesn't exist and is created successfully", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*bool) = false
						return nil
					},
				})

			mockReplicationConn := new(MockReplicationConnection)
			mockReplicationConn.On("CreateReplicationSlot", mock.Anything, mock.Anything).Return(pglogrepl.CreateReplicationSlotResult{}, nil)

			br := &replicator.BaseReplicator{
				StandardConn:    mockStandardConn,
				ReplicationConn: mockReplicationConn,
				Buffer:          replicator.NewBuffer(1000, 5*time.Second),
				Config:          replicator.Config{Group: "new_slot"},
			}

			err := br.CreateReplicationSlot(context.Background())
			assert.NoError(t, err)
			mockStandardConn.AssertExpectations(t)
			mockReplicationConn.AssertExpectations(t)
		})

		t.Run("Error occurs while checking replication slot existence", func(t *testing.T) {
			mockStandardConn := new(MockStandardConnection)
			mockStandardConn.On("QueryRow", mock.Anything, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", mock.Anything).
				Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						return errors.New("query error")
					},
				})

			br := &replicator.BaseReplicator{
				StandardConn: mockStandardConn,
				Config:       replicator.Config{Group: "error_slot"},
				Buffer:       replicator.NewBuffer(1000, 5*time.Second),
			}

			err := br.CreateReplicationSlot(context.Background())
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to check replication slot")
			mockStandardConn.AssertExpectations(t)
		})
	})

	t.Run("Integration with RuleEngine", func(t *testing.T) {
		t.Run("Transform Rule", func(t *testing.T) {
			mockSink := new(MockSink)
			ruleEngine := newTestRuleEngine()

			config := rules.Config{

				Tables: map[string][]rules.RuleConfig{
					"users": {
						{
							Type:   "transform",
							Column: "email",
							Parameters: map[string]interface{}{
								"type":      "mask",
								"mask_char": "*",
							},
						},
					},
				},
			}
			err := ruleEngine.LoadRules(config)
			assert.NoError(t, err)

			br := &replicator.BaseReplicator{
				Sink:       mockSink,
				RuleEngine: ruleEngine,
				Relations: map[uint32]*pglogrepl.RelationMessage{
					1: {
						RelationID:   1,
						Namespace:    "public",
						RelationName: "users",
						Columns: []*pglogrepl.RelationMessageColumn{
							{Name: "id", DataType: pgtype.Int4OID},
							{Name: "name", DataType: pgtype.TextOID},
							{Name: "email", DataType: pgtype.TextOID},
						},
					},
				},
				Buffer: replicator.NewBuffer(1000, 5*time.Second),
			}

			msg := &pglogrepl.InsertMessage{
				RelationID: 1,
				Tuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("1")},
						{Data: []byte("John Doe")},
						{Data: []byte("john@example.com")},
					},
				},
			}
			mockSink.On("WriteBatch", mock.AnythingOfType("[]interface {}")).Return(nil).Run(func(args mock.Arguments) {
				data := args.Get(0).([]interface{})
				assert.Len(t, data, 1, "Expected one batch of data")

				result, ok := data[0].(map[string]interface{})
				assert.True(t, ok, "Expected data to be a map[string]interface{}")

				assert.Equal(t, "INSERT", result["type"])
				assert.Equal(t, "public", result["schema"])
				assert.Equal(t, "users", result["table"])

				newRow, ok := result["new_row"].(map[string]utils.CDCValue)
				assert.True(t, ok, "new_row should be a map")

				email, ok := newRow["email"]
				assert.True(t, ok, "email should be present in new_row")
				assert.Equal(t, pgtype.TextOID, int(email.Type), "email type should be text")

				maskedEmail, ok := email.Value.(string)
				assert.True(t, ok, "email value should be a string")
				assert.Equal(t, "j**************m", maskedEmail, "Email should be masked")
			})
			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err = br.HandleInsertMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)
		})

		t.Run("Filter Rule", func(t *testing.T) {
			mockSink := new(MockSink)
			ruleEngine := newTestRuleEngine()

			config := rules.Config{
				Tables: map[string][]rules.RuleConfig{
					"users": {
						{
							Type:   "filter",
							Column: "age",
							Parameters: map[string]interface{}{
								"operator": "gt",
								"value":    18,
							},
						},
					},
				},
			}
			err := ruleEngine.LoadRules(config)
			assert.NoError(t, err)

			br := &replicator.BaseReplicator{
				Sink:       mockSink,
				RuleEngine: ruleEngine,
				Relations: map[uint32]*pglogrepl.RelationMessage{
					1: {
						RelationID:   1,
						Namespace:    "public",
						RelationName: "users",
						Columns: []*pglogrepl.RelationMessageColumn{
							{Name: "id", DataType: pgtype.Int4OID},
							{Name: "name", DataType: pgtype.TextOID},
							{Name: "age", DataType: pgtype.Int4OID},
						},
					},
				},
				Buffer: replicator.NewBuffer(1000, 5*time.Second),
			}

			// This message should be filtered out
			msg1 := &pglogrepl.InsertMessage{
				RelationID: 1,
				Tuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("1")},
						{Data: []byte("John Doe")},
						{Data: []byte("17")},
					},
				},
			}

			// This message should pass the filter
			msg2 := &pglogrepl.InsertMessage{
				RelationID: 1,
				Tuple: &pglogrepl.TupleData{
					Columns: []*pglogrepl.TupleDataColumn{
						{Data: []byte("2")},
						{Data: []byte("Jane Doe")},
						{Data: []byte("21")},
					},
				},
			}

			expected := map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "users",
				"new_row": map[string]utils.CDCValue{
					"id":   {Type: pgtype.Int4OID, Value: int64(2)},
					"name": {Type: pgtype.TextOID, Value: "Jane Doe"},
					"age":  {Type: pgtype.Int4OID, Value: int64(21)},
				},
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], expected)
			})).Return(nil)

			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err = br.HandleInsertMessage(msg1)
			assert.NoError(t, err)

			err = br.HandleInsertMessage(msg2)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)
		})
	})
}

func TestConvertTupleDataToMap(t *testing.T) {
	testCases := []struct {
		name     string
		relation *pglogrepl.RelationMessage
		tuple    *pglogrepl.TupleData
		expected map[string]utils.CDCValue
	}{
		{
			name: "With various data types",
			relation: &pglogrepl.RelationMessage{
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "name", DataType: pgtype.TextOID},
					{Name: "active", DataType: pgtype.BoolOID},
					{Name: "score", DataType: pgtype.Float8OID},
					{Name: "created_at", DataType: pgtype.TimestamptzOID},
					{Name: "data", DataType: pgtype.JSONBOID},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte("John Doe")},
					{Data: []byte("t")},
					{Data: []byte("9.5")},
					{Data: []byte("2023-04-13 10:30:00+00")},
					{Data: []byte(`{"key": "value"}`)},
				},
			},
			expected: map[string]utils.CDCValue{
				"id":         utils.CDCValue{Type: pgtype.Int4OID, Value: int64(1)},
				"name":       utils.CDCValue{Type: pgtype.TextOID, Value: "John Doe"},
				"active":     utils.CDCValue{Type: pgtype.BoolOID, Value: "t"},
				"score":      utils.CDCValue{Type: pgtype.Float8OID, Value: "9.5"},
				"created_at": utils.CDCValue{Type: pgtype.TimestamptzOID, Value: "2023-04-13T10:30:00Z"},
				"data":       utils.CDCValue{Type: pgtype.JSONBOID, Value: `{"key": "value"}`},
			},
		},
		{
			name: "With various timestamp formats",
			relation: &pglogrepl.RelationMessage{
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "timestamp_with_tz", DataType: pgtype.TimestamptzOID},
					{Name: "timestamp_without_tz", DataType: pgtype.TimestampOID},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("2024-08-17 17:11:24.259862+00")},
					{Data: []byte("2024-08-17 17:11:24.259862")},
				},
			},
			expected: map[string]utils.CDCValue{
				"timestamp_with_tz":    utils.CDCValue{Type: pgtype.TimestamptzOID, Value: "2024-08-17T17:11:24.259862Z"},
				"timestamp_without_tz": utils.CDCValue{Type: pgtype.TimestampOID, Value: "2024-08-17T17:11:24.259862Z"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			br := &replicator.BaseReplicator{
				Logger: zerolog.New(ioutil.Discard),
			}

			result := br.ConvertTupleDataToMap(tc.relation, tc.tuple)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestHandleInsertMessageWithDifferentDataTypes(t *testing.T) {
	testCases := []struct {
		name     string
		relation *pglogrepl.RelationMessage
		tuple    *pglogrepl.TupleData
		expected map[string]interface{}
	}{
		{
			name: "Basic types",
			relation: &pglogrepl.RelationMessage{
				RelationID:   1,
				Namespace:    "public",
				RelationName: "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "name", DataType: pgtype.TextOID},
					{Name: "active", DataType: pgtype.BoolOID},
					{Name: "score", DataType: pgtype.Float8OID},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte("John Doe")},
					{Data: []byte("t")},
					{Data: []byte("9.5")},
				},
			},
			expected: map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "users",
				"new_row": map[string]utils.CDCValue{
					"id":     {Type: pgtype.Int4OID, Value: int64(1)},
					"name":   {Type: pgtype.TextOID, Value: "John Doe"},
					"active": {Type: pgtype.BoolOID, Value: "t"},
					"score":  {Type: pgtype.Float8OID, Value: "9.5"},
				},
			},
		},
		{
			name: "With JSONB and binary data",
			relation: &pglogrepl.RelationMessage{
				RelationID:   2,
				Namespace:    "public",
				RelationName: "complex_data",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "data", DataType: pgtype.JSONBOID},
					{Name: "binary_data", DataType: pgtype.ByteaOID},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte(`{"key": "value", "nested": {"num": 42}}`)},
					{Data: []byte("Hello")},
				},
			},
			expected: map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "complex_data",
				"new_row": map[string]utils.CDCValue{
					"id":          {Type: pgtype.Int4OID, Value: int64(1)},
					"data":        {Type: pgtype.JSONBOID, Value: `{"key": "value", "nested": {"num": 42}}`},
					"binary_data": {Type: pgtype.ByteaOID, Value: "SGVsbG8="},
				},
			},
		},
		{
			name: "With null values",
			relation: &pglogrepl.RelationMessage{
				RelationID:   4,
				Namespace:    "public",
				RelationName: "nullable_data",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "nullable_text", DataType: pgtype.TextOID},
					{Name: "nullable_int", DataType: pgtype.Int4OID},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: nil},
					{Data: nil},
				},
			},
			expected: map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "nullable_data",
				"new_row": map[string]utils.CDCValue{
					"id":            {Type: pgtype.Int4OID, Value: int64(1)},
					"nullable_text": {Type: pgtype.TextOID, Value: nil},
					"nullable_int":  {Type: pgtype.Int4OID, Value: nil},
				},
			},
		},
		{
			name: "With various timestamp formats",
			relation: &pglogrepl.RelationMessage{
				RelationID:   3,
				Namespace:    "public",
				RelationName: "timestamp_data",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "timestamp_with_tz", DataType: pgtype.TimestamptzOID},
					{Name: "timestamp_without_tz", DataType: pgtype.TimestampOID},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("2024-08-17 17:11:24.259862+00")},
					{Data: []byte("2024-08-17 17:11:24.259862")},
				},
			},
			expected: map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "timestamp_data",
				"new_row": map[string]utils.CDCValue{
					"timestamp_with_tz":    {Type: pgtype.TimestamptzOID, Value: "2024-08-17T17:11:24.259862Z"},
					"timestamp_without_tz": {Type: pgtype.TimestampOID, Value: "2024-08-17T17:11:24.259862Z"},
				},
			},
		},
		{
			name: "With extreme integer values",
			relation: &pglogrepl.RelationMessage{
				RelationID:   5,
				Namespace:    "public",
				RelationName: "extreme_integers",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "big_int", DataType: pgtype.Int8OID},
					{Name: "small_int", DataType: pgtype.Int2OID},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte("9223372036854775807")}, // Max int64
					{Data: []byte("-32768")},              // Min int16
				},
			},
			expected: map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "extreme_integers",
				"new_row": map[string]utils.CDCValue{
					"id":        {Type: pgtype.Int4OID, Value: int64(1)},
					"big_int":   {Type: pgtype.Int8OID, Value: int64(9223372036854775807)},
					"small_int": {Type: pgtype.Int2OID, Value: int64(-32768)},
				},
			},
		},
		{
			name: "With complex JSON data",
			relation: &pglogrepl.RelationMessage{
				RelationID:   7,
				Namespace:    "public",
				RelationName: "complex_json",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "json_data", DataType: pgtype.JSONBOID},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte(`{"array": [1, 2, 3], "nested": {"key": "value"}, "null": null}`)},
				},
			},
			expected: map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "complex_json",
				"new_row": map[string]utils.CDCValue{
					"id":        {Type: pgtype.Int4OID, Value: int64(1)},
					"json_data": {Type: pgtype.JSONBOID, Value: `{"array": [1, 2, 3], "nested": {"key": "value"}, "null": null}`},
				},
			},
		},
		{
			name: "With invalid JSON data",
			relation: &pglogrepl.RelationMessage{
				RelationID:   8,
				Namespace:    "public",
				RelationName: "invalid_json",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "json_data", DataType: pgtype.JSONBOID},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte(`{"invalid": json}`)},
				},
			},
			expected: map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "invalid_json",
				"new_row": map[string]utils.CDCValue{
					"id":        {Type: pgtype.Int4OID, Value: int64(1)},
					"json_data": {Type: pgtype.JSONBOID, Value: `{"invalid": json}`},
				},
			},
		},
		{
			name: "With special float values",
			relation: &pglogrepl.RelationMessage{
				RelationID:   6,
				Namespace:    "public",
				RelationName: "special_floats",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "float_normal", DataType: pgtype.Float8OID},
					{Name: "float_nan", DataType: pgtype.Float8OID},
					{Name: "float_inf", DataType: pgtype.Float8OID},
					{Name: "float_neg_inf", DataType: pgtype.Float8OID},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte("3.14159")},
					{Data: []byte("NaN")},
					{Data: []byte("Infinity")},
					{Data: []byte("-Infinity")},
				},
			},
			expected: map[string]interface{}{
				"type":   "INSERT",
				"schema": "public",
				"table":  "special_floats",
				"new_row": map[string]utils.CDCValue{
					"id":            {Type: pgtype.Int4OID, Value: int64(1)},
					"float_normal":  {Type: pgtype.Float8OID, Value: "3.14159"},
					"float_nan":     {Type: pgtype.Float8OID, Value: "NaN"},
					"float_inf":     {Type: pgtype.Float8OID, Value: "Infinity"},
					"float_neg_inf": {Type: pgtype.Float8OID, Value: "-Infinity"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockSink := new(MockSink)
			br := &replicator.BaseReplicator{
				Logger: zerolog.New(ioutil.Discard),
				Relations: map[uint32]*pglogrepl.RelationMessage{
					tc.relation.RelationID: tc.relation,
				},
				Sink:       mockSink,
				Buffer:     replicator.NewBuffer(1000, 5*time.Second),
				RuleEngine: newTestRuleEngine(),
			}

			msg := &pglogrepl.InsertMessage{
				RelationID: tc.relation.RelationID,
				Tuple:      tc.tuple,
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], tc.expected)
			})).Return(nil)
			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err := br.HandleInsertMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)
		})
	}
}
func TestHandleUpdateMessageWithDifferentDataTypes(t *testing.T) {
	testCases := []struct {
		name     string
		relation *pglogrepl.RelationMessage
		oldTuple *pglogrepl.TupleData
		newTuple *pglogrepl.TupleData
		expected map[string]interface{}
	}{
		{
			name: "Basic types update",
			relation: &pglogrepl.RelationMessage{
				RelationID:   1,
				Namespace:    "public",
				RelationName: "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "name", DataType: pgtype.TextOID},
					{Name: "active", DataType: pgtype.BoolOID},
					{Name: "score", DataType: pgtype.Float8OID},
				},
			},
			oldTuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte("John Doe")},
					{Data: []byte("t")},
					{Data: []byte("9.5")},
				},
			},
			newTuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte("John Smith")},
					{Data: []byte("f")},
					{Data: []byte("8.5")},
				},
			},
			expected: map[string]interface{}{
				"type":   "UPDATE",
				"schema": "public",
				"table":  "users",
				"old_row": map[string]utils.CDCValue{
					"id":     {Type: pgtype.Int4OID, Value: int64(1)},
					"name":   {Type: pgtype.TextOID, Value: "John Doe"},
					"active": {Type: pgtype.BoolOID, Value: "t"},
					"score":  {Type: pgtype.Float8OID, Value: "9.5"},
				},
				"new_row": map[string]utils.CDCValue{
					"id":     {Type: pgtype.Int4OID, Value: int64(1)},
					"name":   {Type: pgtype.TextOID, Value: "John Smith"},
					"active": {Type: pgtype.BoolOID, Value: "f"},
					"score":  {Type: pgtype.Float8OID, Value: "8.5"},
				},
			},
		},
		{
			name: "Update with special float values",
			relation: &pglogrepl.RelationMessage{
				RelationID:   2,
				Namespace:    "public",
				RelationName: "special_floats",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "float_value", DataType: pgtype.Float8OID},
				},
			},
			oldTuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte("3.14159")},
				},
			},
			newTuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("1")},
					{Data: []byte("NaN")},
				},
			},
			expected: map[string]interface{}{
				"type":   "UPDATE",
				"schema": "public",
				"table":  "special_floats",
				"old_row": map[string]utils.CDCValue{
					"id":          {Type: pgtype.Int4OID, Value: int64(1)},
					"float_value": {Type: pgtype.Float8OID, Value: "3.14159"},
				},
				"new_row": map[string]utils.CDCValue{
					"id":          {Type: pgtype.Int4OID, Value: int64(1)},
					"float_value": {Type: pgtype.Float8OID, Value: "NaN"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockSink := new(MockSink)
			br := &replicator.BaseReplicator{
				Logger: zerolog.New(ioutil.Discard),
				Relations: map[uint32]*pglogrepl.RelationMessage{
					tc.relation.RelationID: tc.relation,
				},
				Sink:       mockSink,
				Buffer:     replicator.NewBuffer(1000, 5*time.Second),
				RuleEngine: newTestRuleEngine(),
			}

			msg := &pglogrepl.UpdateMessage{
				RelationID: tc.relation.RelationID,
				OldTuple:   tc.oldTuple,
				NewTuple:   tc.newTuple,
			}

			mockSink.On("WriteBatch", mock.MatchedBy(func(data []interface{}) bool {
				return len(data) == 1 && reflect.DeepEqual(data[0], tc.expected)
			})).Return(nil)

			mockSink.On("SetLastLSN", mock.AnythingOfType("pglogrepl.LSN")).Return(nil)

			err := br.HandleUpdateMessage(msg)
			assert.NoError(t, err)

			err = br.FlushBuffer()
			assert.NoError(t, err)

			mockSink.AssertExpectations(t)
		})
	}
}
