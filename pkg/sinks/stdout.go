package sinks

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/goccy/go-json"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog/log"
)

// StdoutSink represents a sink that writes data to standard output
type StdoutSink struct {
	lastLSN   pglogrepl.LSN
	statusDir string
	lsnFile   string
}

// NewStdoutSink creates a new StdoutSink instance
func NewStdoutSink(statusDir string) (*StdoutSink, error) {
	sink := &StdoutSink{
		statusDir: statusDir,
		lsnFile:   filepath.Join(statusDir, "pg_flo_stdout_last_lsn.json"),
	}

	if err := os.MkdirAll(statusDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create status directory: %v", err)
	}

	if err := sink.loadStatus(); err != nil {
		log.Warn().Err(err).Msg("Failed to load status, starting from scratch")
	}

	return sink, nil
}

// loadStatus loads the last known LSN from the status file
func (s *StdoutSink) loadStatus() error {
	data, err := os.ReadFile(s.lsnFile)
	if os.IsNotExist(err) {
		log.Info().Msg("No existing LSN file found, starting from scratch")
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to read LSN file: %v", err)
	}

	var status Status
	if err := json.Unmarshal(data, &status); err != nil {
		return fmt.Errorf("failed to unmarshal status: %v", err)
	}

	s.lastLSN = status.LastLSN
	log.Info().Str("lsn", s.lastLSN.String()).Msg("Loaded last LSN from file")
	return nil
}

// saveStatus saves the current LSN to the status file
func (s *StdoutSink) saveStatus() error {
	status := Status{
		LastLSN: s.lastLSN,
	}

	data, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to marshal status: %v", err)
	}

	if err := os.WriteFile(s.lsnFile, data, 0600); err != nil {
		return fmt.Errorf("failed to write status file: %v", err)
	}

	return nil
}

// WriteBatch writes a batch of data to standard output
func (s *StdoutSink) WriteBatch(data []interface{}) error {
	for _, item := range data {
		jsonData, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("failed to marshal data to JSON: %v", err)
		}
		if _, err := fmt.Println(string(jsonData)); err != nil {
			return err
		}
	}
	return nil
}

// GetLastLSN returns the last processed LSN
func (s *StdoutSink) GetLastLSN() (pglogrepl.LSN, error) {
	return s.lastLSN, nil
}

// SetLastLSN sets the last processed LSN and saves it to the status file
func (s *StdoutSink) SetLastLSN(lsn pglogrepl.LSN) error {
	s.lastLSN = lsn
	return s.saveStatus()
}

// Close performs any necessary cleanup (no-op for StdoutSink)
func (s *StdoutSink) Close() error {
	return nil
}
