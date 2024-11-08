package utils

import (
	"context"
	"time"
)

type RetryConfig struct {
	MaxAttempts int
	InitialWait time.Duration
	MaxWait     time.Duration
}

func WithRetry(ctx context.Context, cfg RetryConfig, operation func() error) error {
	wait := cfg.InitialWait
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		err := operation()
		if err == nil {
			return nil
		}

		if attempt == cfg.MaxAttempts {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
			// Exponential backoff with max wait
			wait *= 2
			if wait > cfg.MaxWait {
				wait = cfg.MaxWait
			}
		}
	}
	return nil
}
