package replicator_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
	"github.com/shayonj/pg_flo/pkg/replicator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDDLReplicator(t *testing.T) {
	t.Run("NewDDLReplicator", func(t *testing.T) {
		mockBaseReplicator := &replicator.BaseReplicator{
			Logger: zerolog.Logger{},
		}
		mockStandardConn := &MockStandardConnection{}
		config := replicator.Config{}

		ddlReplicator, err := replicator.NewDDLReplicator(config, mockBaseReplicator, mockStandardConn)

		assert.NoError(t, err)
		assert.NotNil(t, ddlReplicator)
		assert.Equal(t, config, ddlReplicator.Config)
		assert.Equal(t, mockStandardConn, ddlReplicator.DDLConn)
	})

	t.Run("SetupDDLTracking", func(t *testing.T) {
		mockStandardConn := &MockStandardConnection{}
		mockBaseRepl := &replicator.BaseReplicator{
			Logger:       zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger(),
			StandardConn: mockStandardConn,
			Config: replicator.Config{
				Schema: "public",
				Tables: []string{"test_table"},
			},
		}

		ddlReplicator := &replicator.DDLReplicator{
			DDLConn:  mockStandardConn,
			BaseRepl: mockBaseRepl,
		}

		ctx := context.Background()

		mockStandardConn.On("Exec", ctx, mock.AnythingOfType("string"), mock.Anything).Return(pgconn.CommandTag{}, nil).
			Run(func(args mock.Arguments) {
				sql := args.Get(1).(string)
				assert.Contains(t, sql, "CREATE SCHEMA IF NOT EXISTS internal_pg_flo")
				assert.Contains(t, sql, "CREATE TABLE IF NOT EXISTS internal_pg_flo.ddl_log")
				assert.Contains(t, sql, "CREATE OR REPLACE FUNCTION internal_pg_flo.ddl_trigger()")
				assert.Contains(t, sql, "CREATE EVENT TRIGGER pg_flo_ddl_trigger")
			})

		err := ddlReplicator.SetupDDLTracking(ctx)

		assert.NoError(t, err)
		mockStandardConn.AssertExpectations(t)
	})

	t.Run("StartDDLReplication", func(t *testing.T) {
		mockStandardConn := &MockStandardConnection{}
		mockBaseReplicator := &replicator.BaseReplicator{
			Logger: zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger(),
		}
		ddlReplicator := &replicator.DDLReplicator{
			DDLConn:  mockStandardConn,
			BaseRepl: mockBaseReplicator,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		mockRows := &MockRows{}
		mockRows.On("Next").Return(false)
		mockRows.On("Err").Return(nil)
		mockRows.On("Close").Return()

		mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(sql string) bool {
			expectedParts := []string{
				"SELECT id, event_type, object_type, object_identity, table_name, ddl_command, created_at",
				"FROM internal_pg_flo.ddl_log",
				"ORDER BY created_at ASC",
			}
			for _, part := range expectedParts {
				if !strings.Contains(sql, part) {
					return false
				}
			}
			return true
		}), mock.Anything).Return(mockRows, nil)

		mockStandardConn.On("QueryRow", mock.Anything, mock.MatchedBy(func(sql string) bool {
			return strings.Contains(sql, "SELECT COUNT(*) FROM internal_pg_flo.ddl_log")
		}), mock.Anything).Return(&MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*int) = 1
				return nil
			},
		})

		go ddlReplicator.StartDDLReplication(ctx)

		time.Sleep(100 * time.Millisecond)

		hasPending, err := ddlReplicator.HasPendingDDLEvents(ctx)
		assert.NoError(t, err)
		assert.True(t, hasPending)

		cancel()

		time.Sleep(100 * time.Millisecond)

		mockStandardConn.AssertExpectations(t)
		mockRows.AssertExpectations(t)
	})
}
