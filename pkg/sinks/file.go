package sinks

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog/log"
)

// FileSink represents a sink that writes data to files
type FileSink struct {
	lastLSN        pglogrepl.LSN
	statusDir      string
	outputDir      string
	lsnFile        string
	currentFile    *os.File
	currentSize    int64
	maxFileSize    int64
	rotateInterval time.Duration
	lastRotation   time.Time
	mutex          sync.Mutex
}

// NewFileSink creates a new FileSink instance
func NewFileSink(statusDir, outputDir string) (*FileSink, error) {
	sink := &FileSink{
		statusDir:      statusDir,
		outputDir:      outputDir,
		lsnFile:        filepath.Join(statusDir, "pg_flo_file_last_lsn.json"),
		maxFileSize:    100 * 1024 * 1024, // 100 MB
		rotateInterval: time.Hour,         // Rotate every hour if size limit not reached
	}

	if err := os.MkdirAll(statusDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create status directory: %v", err)
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	if err := sink.loadStatus(); err != nil {
		log.Warn().Err(err).Msg("Failed to load status, starting from scratch")
	}

	if err := sink.rotateFile(); err != nil {
		return nil, fmt.Errorf("failed to create initial log file: %v", err)
	}

	return sink, nil
}

// rotateFile creates a new log file and updates the current file pointer
func (s *FileSink) rotateFile() error {
	if s.currentFile != nil {
		err := s.currentFile.Close()
		if err != nil {
			return err
		}
		s.currentFile = nil
	}

	timestamp := time.Now().UTC().Format("20060102T150405Z")
	filename := fmt.Sprintf("pg_flo_log_%s.jsonl", timestamp)
	filepath := filepath.Join(s.outputDir, filename)

	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new log file: %v", err)
	}

	s.currentFile = file
	s.currentSize = 0
	s.lastRotation = time.Now()

	log.Info().Str("file", filepath).Msg("Rotated to new log file")
	return nil
}

// WriteBatch writes a batch of data to the current log file
func (s *FileSink) WriteBatch(data []interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, item := range data {
		jsonData, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("failed to marshal data to JSON: %v", err)
		}

		if s.currentFile == nil || s.currentSize >= s.maxFileSize || time.Since(s.lastRotation) >= s.rotateInterval {
			if err := s.rotateFile(); err != nil {
				return err
			}
		}

		jsonData = append(jsonData, '\n') // Add newline for JSONL format
		n, err := s.currentFile.Write(jsonData)
		if err != nil {
			return fmt.Errorf("failed to write to log file: %v", err)
		}

		s.currentSize += int64(n)
	}

	return nil
}

// loadStatus loads the last known LSN from the status file
func (s *FileSink) loadStatus() error {
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
func (s *FileSink) saveStatus() error {
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

// GetLastLSN returns the last processed LSN
func (s *FileSink) GetLastLSN() (pglogrepl.LSN, error) {
	return s.lastLSN, nil
}

// SetLastLSN sets the last processed LSN and saves it to the status file
func (s *FileSink) SetLastLSN(lsn pglogrepl.LSN) error {
	s.lastLSN = lsn
	return s.saveStatus()
}

// Close closes the current log file and performs any necessary cleanup
func (s *FileSink) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.currentFile != nil {
		err := s.currentFile.Close()
		s.currentFile = nil
		return err
	}
	return nil
}
