package retryspool

import (
	"time"
)

// ExponentialRetryPolicy implements exponential backoff retry policy
type ExponentialRetryPolicy struct {
	maxDuration time.Duration
	minInterval time.Duration
}

// NewExponentialRetryPolicy creates a new exponential retry policy
func NewExponentialRetryPolicy(maxDuration, minInterval time.Duration) *ExponentialRetryPolicy {
	return &ExponentialRetryPolicy{
		maxDuration: maxDuration,
		minInterval: minInterval,
	}
}

// DefaultExponentialRetryPolicy creates a default exponential retry policy
// with Postfix-like settings: retry for 5 days with 15-minute minimum interval
func DefaultExponentialRetryPolicy() *ExponentialRetryPolicy {
	return NewExponentialRetryPolicy(5*24*time.Hour, 15*time.Minute)
}

// NextRetry calculates the next retry time using exponential backoff
func (p *ExponentialRetryPolicy) NextRetry(attempts int, lastError error) time.Time {
	// Exponential backoff intervals similar to Postfix:
	// 15min, 30min, 1h, 2h, 4h, 8h, 16h, 24h, 24h, 24h
	intervals := []time.Duration{
		15 * time.Minute,
		30 * time.Minute,
		1 * time.Hour,
		2 * time.Hour,
		4 * time.Hour,
		8 * time.Hour,
		16 * time.Hour,
		24 * time.Hour,
		24 * time.Hour,
		24 * time.Hour,
	}

	interval := 24 * time.Hour // Default to 24 hours for attempts beyond the array
	if attempts < len(intervals) {
		interval = intervals[attempts]
	}

	// Ensure minimum interval
	if interval < p.minInterval {
		interval = p.minInterval
	}

	return time.Now().Add(interval)
}

// ShouldRetry determines if a message should be retried based on attempts and error type
func (p *ExponentialRetryPolicy) ShouldRetry(attempts int, maxAttempts int, lastError error) bool {
	if attempts >= maxAttempts {
		return false
	}

	// Don't retry permanent errors
	if IsPermanent(lastError) {
		return false
	}

	// Retry retryable errors (explicit retryable errors or unknown errors)
	return IsRetryable(lastError) || (!IsPermanent(lastError) && !IsRetryable(lastError))
}

// SimpleRetryPolicy implements a simple fixed-interval retry policy
type SimpleRetryPolicy struct {
	interval time.Duration
}

// NewSimpleRetryPolicy creates a new simple retry policy with fixed interval
func NewSimpleRetryPolicy(interval time.Duration) *SimpleRetryPolicy {
	return &SimpleRetryPolicy{
		interval: interval,
	}
}

// NextRetry returns the next retry time using a fixed interval
func (p *SimpleRetryPolicy) NextRetry(attempts int, lastError error) time.Time {
	return time.Now().Add(p.interval)
}

// ShouldRetry determines if a message should be retried (up to max attempts)
func (p *SimpleRetryPolicy) ShouldRetry(attempts int, maxAttempts int, lastError error) bool {
	return attempts < maxAttempts
}
