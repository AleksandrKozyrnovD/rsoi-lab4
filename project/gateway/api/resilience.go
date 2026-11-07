package api

import (
	"context"
	"time"
	"github.com/cenkalti/backoff/v4"
)

type RetryConfig struct {
	MaxRetries          int           // number of retry attempts (not counting the initial attempt)
	InitialInterval     time.Duration // initial backoff interval
	MaxInterval         time.Duration // maximum backoff interval cap
	Multiplier          float64       // exponential growth multiplier
	RandomizationFactor float64       // jitter factor (0.0 to 1.0)
	MaxElapsedTime      time.Duration // total time budget for retries (0 = infinite)
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:          3,
		InitialInterval:     200 * time.Millisecond,
		MaxInterval:         10 * time.Second,
		Multiplier:          2.0,
		RandomizationFactor: 0.5,
		MaxElapsedTime:      30 * time.Second,
	}
}

type CircuitBreakerConfig struct {
	MaxRequests      uint32        // max requests allowed in half-open state
	Interval         time.Duration // cyclic period for clearing counts in closed state
	Timeout          time.Duration // open state duration before half-open
	FailureThreshold uint32        // consecutive failures to trip the breaker
}

func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxRequests:      3,
		Interval:         30 * time.Second,
		Timeout:          15 * time.Second,
		FailureThreshold: 5,
	}
}

func newCircuitBreaker[T any](name string, cfg CircuitBreakerConfig) *CircuitBreaker[T] {
	return NewCircuitBreaker[T](Settings{
		Name:        name,
		MaxRequests: cfg.MaxRequests,
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= cfg.FailureThreshold
		},
	})
}

type limitedBackOff struct {
	backoff.BackOff
	maxRetries int
	attempt    int
}

func newLimitedBackOff(cfg RetryConfig) *limitedBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = cfg.InitialInterval
	b.MaxInterval = cfg.MaxInterval
	b.Multiplier = cfg.Multiplier
	b.RandomizationFactor = cfg.RandomizationFactor
	b.MaxElapsedTime = cfg.MaxElapsedTime
	return &limitedBackOff{BackOff: b, maxRetries: cfg.MaxRetries}
}

func (l *limitedBackOff) NextBackOff() time.Duration {
	if l.attempt >= l.maxRetries {
		return backoff.Stop
	}
	l.attempt++
	return l.BackOff.NextBackOff()
}

func (l *limitedBackOff) Reset() {
	l.attempt = 0
	l.BackOff.Reset()
}

func retryExecute(ctx context.Context, cfg RetryConfig, fn func() error) error {
	b := newLimitedBackOff(cfg)
	return backoff.Retry(fn, backoff.WithContext(b, ctx))
}
