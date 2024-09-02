package replicator_test

import (
	"testing"
	"time"

	"github.com/shayonj/pg_flo/pkg/replicator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestBuffer(t *testing.T) {
	t.Run("NewBuffer", func(t *testing.T) {
		buffer := replicator.NewBuffer(10, 5*time.Second)
		assert.NotNil(t, buffer)
	})

	t.Run("Add and Flush", func(t *testing.T) {
		buffer := replicator.NewBuffer(10, 5*time.Second)

		// Add items
		for i := 0; i < 5; i++ {
			shouldFlush := buffer.Add([]byte("test"))
			assert.False(t, shouldFlush)
		}

		// Flush
		data := buffer.Flush()
		assert.Len(t, data, 5)
		assert.Equal(t, []byte("test"), data[0])

		// Buffer should be empty after flush
		emptyData := buffer.Flush()
		assert.Nil(t, emptyData)
	})

	t.Run("Flush on MaxRows", func(t *testing.T) {
		buffer := replicator.NewBuffer(3, 5*time.Second)

		buffer.Add([]byte("test1"))
		buffer.Add([]byte("test2"))
		shouldFlush := buffer.Add([]byte("test3"))

		assert.True(t, shouldFlush)

		data := buffer.Flush()
		assert.Len(t, data, 3)
	})

	t.Run("Flush on Timeout", func(t *testing.T) {
		buffer := replicator.NewBuffer(10, 100*time.Millisecond)

		buffer.Add([]byte("test"))
		time.Sleep(150 * time.Millisecond)

		shouldFlush := buffer.Add([]byte("test"))
		assert.True(t, shouldFlush)

		data := buffer.Flush()
		assert.Len(t, data, 2)
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		buffer := replicator.NewBuffer(100, 5*time.Second)

		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 10; j++ {
					buffer.Add([]byte("test"))
				}
				done <- true
			}()
		}

		for i := 0; i < 10; i++ {
			<-done
		}

		data := buffer.Flush()
		assert.Len(t, data, 100)
	})

	t.Run("BufferFlush", func(t *testing.T) {
		mockSink := new(MockSink)
		buffer := replicator.NewBuffer(5, 1*time.Second)

		mockSink.On("WriteBatch", mock.Anything).Return(nil)

		for i := 0; i < 5; i++ {
			shouldFlush := buffer.Add(i)
			if shouldFlush {
				data := buffer.Flush()
				err := mockSink.WriteBatch(data)
				assert.NoError(t, err)
			}
		}

		mockSink.AssertNumberOfCalls(t, "WriteBatch", 1)
		mockSink.AssertExpectations(t)
	})
}
