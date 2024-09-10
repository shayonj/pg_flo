package sinks

import (
	"fmt"

	"github.com/goccy/go-json"
	"github.com/shayonj/pg_flo/pkg/utils"
)

// StdoutSink represents a sink that writes data to standard output
type StdoutSink struct{}

// NewStdoutSink creates a new StdoutSink instance
func NewStdoutSink() (*StdoutSink, error) {
	sink := &StdoutSink{}

	return sink, nil
}

// WriteBatch writes a batch of data to standard output
func (s *StdoutSink) WriteBatch(messages []*utils.CDCMessage) error {
	for _, message := range messages {
		jsonData, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("failed to marshal data to JSON: %v", err)
		}
		if _, err := fmt.Println(string(jsonData)); err != nil {
			return err
		}
	}
	return nil
}
