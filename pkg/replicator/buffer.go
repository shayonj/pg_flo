package replicator

import (
	"sync"
	"time"
)

// Buffer is a structure that holds data to be flushed periodically or when certain conditions are met
type Buffer struct {
	data         []interface{}
	maxRows      int
	flushTimeout time.Duration
	lastFlush    time.Time
	mutex        sync.Mutex
}

// NewBuffer creates a new Buffer instance
func NewBuffer(maxRows int, flushTimeout time.Duration) *Buffer {
	return &Buffer{
		data:         make([]interface{}, 0, maxRows),
		maxRows:      maxRows,
		flushTimeout: flushTimeout,
		lastFlush:    time.Now(),
	}
}

// Add adds an item to the buffer and returns true if the buffer should be flushed
func (b *Buffer) Add(item interface{}) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.data = append(b.data, item)

	return b.shouldFlush()
}

// shouldFlush checks if the buffer should be flushed based on row count, or timeout
func (b *Buffer) shouldFlush() bool {
	return len(b.data) >= b.maxRows || time.Since(b.lastFlush) >= b.flushTimeout
}

// Flush flushes the buffer and returns the data
func (b *Buffer) Flush() []interface{} {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(b.data) == 0 {
		return nil
	}

	data := b.data
	b.data = make([]interface{}, 0, b.maxRows)
	b.lastFlush = time.Now()

	return data
}
