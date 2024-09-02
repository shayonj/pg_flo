package sinks

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/goccy/go-json"
	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog/log"
)

type WebhookSink struct {
	lastLSN    pglogrepl.LSN
	statusDir  string
	lsnFile    string
	webhookURL string
	client     *http.Client
}

func NewWebhookSink(statusDir, webhookURL string) (*WebhookSink, error) {
	sink := &WebhookSink{
		statusDir:  statusDir,
		lsnFile:    filepath.Join(statusDir, "pg_flo_webhook_last_lsn.json"),
		webhookURL: webhookURL,
		client:     &http.Client{},
	}

	if err := os.MkdirAll(statusDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create status directory: %v", err)
	}

	if err := sink.loadStatus(); err != nil {
		log.Warn().Err(err).Msg("Failed to load status, starting from scratch")
	}

	return sink, nil
}

func (s *WebhookSink) loadStatus() error {
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

func (s *WebhookSink) saveStatus() error {
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

func (s *WebhookSink) WriteBatch(data []interface{}) error {
	for _, item := range data {
		jsonData, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("failed to marshal data to JSON: %v", err)
		}

		err = s.sendWithRetry(jsonData)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *WebhookSink) sendWithRetry(jsonData []byte) error {
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("POST", s.webhookURL, bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("failed to create request: %v", err)
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := s.client.Do(req)
		if err != nil {
			if attempt == maxRetries {
				return fmt.Errorf("failed to send webhook after %d attempts: %v", maxRetries, err)
			}
			log.Warn().Err(err).Int("attempt", attempt).Msg("Webhook request failed, retrying...")
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		if attempt == maxRetries {
			return fmt.Errorf("webhook request failed with status code: %d after %d attempts", resp.StatusCode, maxRetries)
		}
		log.Warn().Int("statusCode", resp.StatusCode).Int("attempt", attempt).Msg("Received non-2xx status code, retrying...")
	}
	return nil
}

func (s *WebhookSink) GetLastLSN() (pglogrepl.LSN, error) {
	return s.lastLSN, nil
}

func (s *WebhookSink) SetLastLSN(lsn pglogrepl.LSN) error {
	s.lastLSN = lsn
	return s.saveStatus()
}

func (s *WebhookSink) Close() error {
	return nil
}
