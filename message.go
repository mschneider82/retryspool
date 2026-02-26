package retryspool

import (
	"context"
	"io"
	"time"
)

// QueueState represents the state of a message in the queue
type QueueState int

const (
	// StateIncoming represents new messages awaiting processing
	StateIncoming QueueState = iota
	// StateActive represents messages currently being processed
	StateActive
	// StateDeferred represents messages that failed and are waiting for retry
	StateDeferred
	// StateHold represents messages manually held (admin intervention)
	StateHold
	// StateBounce represents messages that permanently failed
	StateBounce
	// StateArchived represents completed messages (success or permanent failure)
	StateArchived
)

// String returns the string representation of the queue state
func (s QueueState) String() string {
	switch s {
	case StateIncoming:
		return "incoming"
	case StateActive:
		return "active"
	case StateDeferred:
		return "deferred"
	case StateHold:
		return "hold"
	case StateBounce:
		return "bounce"
	case StateArchived:
		return "archived"
	default:
		return "unknown"
	}
}

// Message represents a queued message with metadata
type Message struct {
	ID       string
	Metadata MessageMetadata
}

// MessageMetadata contains metadata about a message
type MessageMetadata struct {
	State           QueueState
	Attempts        int
	MaxAttempts     int
	NextRetry       time.Time
	Created         time.Time
	Updated         time.Time
	LastError       string
	Size            int64
	Priority        int
	Headers         map[string]string
	RetryPolicyName string
}

// MessageReader provides access to message data for reading
type MessageReader interface {
	io.ReadCloser
	Size() int64
}

// MessageWriter provides access to message data for writing
type MessageWriter interface {
	io.WriteCloser
	Size() int64
}

type headerCollectorKey struct{}

// NewHeaderCollectorContext returns a context that collects header updates
// from handlers. The queue calls this internally before invoking a handler.
// Returns the enriched context and the map that will be populated by the
// handler.
func NewHeaderCollectorContext(ctx context.Context) (context.Context, map[string]string) {
	headers := make(map[string]string)
	return context.WithValue(ctx, headerCollectorKey{}, headers), headers
}

// SetContextHeaders merges the given headers into the context's header
// collector. Handlers call this to set or overwrite message headers after
// processing. Safe no-op if no collector is present in the context.
func SetContextHeaders(ctx context.Context, headers map[string]string) {
	collector, ok := ctx.Value(headerCollectorKey{}).(map[string]string)
	if !ok || collector == nil {
		return
	}
	for k, v := range headers {
		collector[k] = v
	}
}

// SetContextHeader sets a single header in the context's header collector.
// Safe no-op if no collector is present in the context.
func SetContextHeader(ctx context.Context, key, value string) {
	collector, ok := ctx.Value(headerCollectorKey{}).(map[string]string)
	if !ok || collector == nil {
		return
	}
	collector[key] = value
}
