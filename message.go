package retryspool

import (
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
