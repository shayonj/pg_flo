package replicator_test

import (
	"context"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/nats-io/nats.go"
	"github.com/shayonj/pg_flo/pkg/replicator"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/utils"
	"github.com/stretchr/testify/mock"
)

type MockReplicationConnection struct {
	mock.Mock
}

func (m *MockReplicationConnection) Connect(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockReplicationConnection) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockReplicationConnection) CreateReplicationSlot(ctx context.Context, slotName string) (pglogrepl.CreateReplicationSlotResult, error) {
	args := m.Called(ctx, slotName)
	return args.Get(0).(pglogrepl.CreateReplicationSlotResult), args.Error(1)
}

func (m *MockReplicationConnection) StartReplication(ctx context.Context, slotName string, startLSN pglogrepl.LSN, options pglogrepl.StartReplicationOptions) error {
	args := m.Called(ctx, slotName, startLSN, options)
	return args.Error(0)
}

func (m *MockReplicationConnection) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	args := m.Called(ctx)
	msg := args.Get(0)
	if msg == nil {
		return nil, args.Error(1)
	}
	return msg.(pgproto3.BackendMessage), args.Error(1)
}

func (m *MockReplicationConnection) SendStandbyStatusUpdate(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error {
	args := m.Called(ctx, status)
	return args.Error(0)
}

type MockStandardConnection struct {
	mock.Mock
}

func (m *MockStandardConnection) Connect(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockStandardConnection) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockStandardConnection) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	args := m.Called(ctx, sql, arguments)
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

func (m *MockStandardConnection) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(pgx.Rows), mockArgs.Error(1)
}

func (m *MockStandardConnection) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(pgx.Row)
}

func (m *MockStandardConnection) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	args := m.Called(ctx, txOptions)
	return args.Get(0).(pgx.Tx), args.Error(1)
}

func (m *MockStandardConnection) Acquire(ctx context.Context) (replicator.PgxPoolConn, error) {
	args := m.Called(ctx)
	return args.Get(0).(replicator.PgxPoolConn), args.Error(1)
}

type MockSink struct {
	mock.Mock
}

func (m *MockSink) WriteBatch(data []interface{}) error {
	args := m.Called(data)
	return args.Error(0)
}

func (m *MockSink) GetLastLSN() (pglogrepl.LSN, error) {
	args := m.Called()
	return args.Get(0).(pglogrepl.LSN), args.Error(1)
}

func (m *MockSink) SetLastLSN(lsn pglogrepl.LSN) error {
	args := m.Called(lsn)
	return args.Error(0)
}

func (m *MockSink) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockPgxPoolConn struct {
	mock.Mock
}

func (m *MockPgxPoolConn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	args := m.Called(ctx, txOptions)
	return args.Get(0).(pgx.Tx), args.Error(1)
}

func (m *MockPgxPoolConn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	args := m.Called(ctx, sql, arguments)
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

func (m *MockPgxPoolConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(pgx.Rows), mockArgs.Error(1)
}

func (m *MockPgxPoolConn) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(pgx.Row)
}

func (m *MockPgxPoolConn) Release() {
	m.Called()
}

type MockTx struct {
	mock.Mock
}

func (m *MockTx) Begin(ctx context.Context) (pgx.Tx, error) {
	args := m.Called(ctx)
	return args.Get(0).(pgx.Tx), args.Error(1)
}

func (m *MockTx) Commit(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	args := m.Called(ctx, tableName, columnNames, rowSrc)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	args := m.Called(ctx, b)
	return args.Get(0).(pgx.BatchResults)
}

func (m *MockTx) LargeObjects() pgx.LargeObjects {
	args := m.Called()
	return args.Get(0).(pgx.LargeObjects)
}

func (m *MockTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	args := m.Called(ctx, name, sql)
	return args.Get(0).(*pgconn.StatementDescription), args.Error(1)
}

func (m *MockTx) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	args := []interface{}{ctx, sql}
	args = append(args, arguments...)
	callArgs := m.Called(args...)
	return callArgs.Get(0).(pgconn.CommandTag), callArgs.Error(1)
}

func (m *MockTx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(pgx.Rows), mockArgs.Error(1)
}

func (m *MockTx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	callArgs := []interface{}{ctx, sql}
	callArgs = append(callArgs, args...)
	mockArgs := m.Called(callArgs...)
	return mockArgs.Get(0).(pgx.Row)
}

func (m *MockTx) Conn() *pgx.Conn {
	args := m.Called()
	return args.Get(0).(*pgx.Conn)
}

func (m *MockTx) Rollback(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockRow struct {
	scanFunc func(dest ...interface{}) error
}

func (m MockRow) Scan(dest ...interface{}) error {
	return m.scanFunc(dest...)
}

type MockRows struct {
	mock.Mock
}

func (m *MockRows) Next() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockRows) Scan(dest ...interface{}) error {
	args := m.Called(dest...)
	return args.Error(0)
}

func (m *MockRows) Err() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockRows) Close() {
	m.Called()
}

func (m *MockRows) CommandTag() pgconn.CommandTag {
	args := m.Called()
	return args.Get(0).(pgconn.CommandTag)
}

func (m *MockRows) FieldDescriptions() []pgconn.FieldDescription {
	args := m.Called()
	return args.Get(0).([]pgconn.FieldDescription)
}

func (m *MockRows) Values() ([]interface{}, error) {
	args := m.Called()
	return args.Get(0).([]interface{}), args.Error(1)
}

func (m *MockRows) RawValues() [][]byte {
	args := m.Called()
	return args.Get(0).([][]byte)
}

func (m *MockRows) Conn() *pgx.Conn {
	args := m.Called()
	return args.Get(0).(*pgx.Conn)
}

// Add this new mock implementation for RuleEngine
type MockRuleEngine struct {
	mock.Mock
}

func (m *MockRuleEngine) ApplyRules(tableName string, data map[string]utils.CDCValue, operation rules.OperationType) (map[string]utils.CDCValue, error) {
	args := m.Called(tableName, data, operation)
	result := args.Get(0)
	if result == nil {
		return nil, args.Error(1)
	}
	return result.(map[string]utils.CDCValue), args.Error(1)
}

func (m *MockRuleEngine) AddRule(tableName string, rule rules.Rule) {
	m.Called(tableName, rule)
}

func (m *MockRuleEngine) LoadRules(config rules.Config) error {
	args := m.Called(config)
	return args.Error(0)
}

// MockNATSClient mocks the NATSClient
type MockNATSClient struct {
	mock.Mock
}

// PublishMessage mocks the PublishMessage method
func (m *MockNATSClient) PublishMessage(subject string, data []byte) error {
	args := m.Called(subject, data)
	if len(args) == 0 {
		return nil
	}
	return args.Error(0)
}

// Close mocks the Close method
func (m *MockNATSClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// GetStreamInfo mocks the GetStreamInfo method
func (m *MockNATSClient) GetStreamInfo() (*nats.StreamInfo, error) {
	args := m.Called()
	return args.Get(0).(*nats.StreamInfo), args.Error(1)
}

// PurgeStream mocks the PurgeStream method
func (m *MockNATSClient) PurgeStream() error {
	args := m.Called()
	return args.Error(0)
}

// DeleteStream mocks the DeleteStream method
func (m *MockNATSClient) DeleteStream() error {
	args := m.Called()
	return args.Error(0)
}

// SaveState mocks the SaveState method
func (m *MockNATSClient) SaveState(lsn pglogrepl.LSN) error {
	args := m.Called(lsn)
	return args.Error(0)
}

// GetLastState mocks the GetLastState method
func (m *MockNATSClient) GetLastState() (pglogrepl.LSN, error) {
	args := m.Called()
	return args.Get(0).(pglogrepl.LSN), args.Error(1)
}
