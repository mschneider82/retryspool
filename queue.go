package retryspool

import (
	"context"
	"io"
	"time"
)

// Queue represents a message queue with retry capabilities
type Queue interface {
	// Enqueue adds a new message to the queue
	Enqueue(ctx context.Context, headers map[string]string, data io.Reader) (string, error)

	// GetMessage retrieves message metadata and data reader
	GetMessage(ctx context.Context, id string) (Message, MessageReader, error)

	// GetMessageMetadata retrieves only message metadata
	GetMessageMetadata(ctx context.Context, id string) (MessageMetadata, error)

	// UpdateMessageMetadata updates message metadata
	UpdateMessageMetadata(ctx context.Context, id string, metadata MessageMetadata) error

	// MoveToState moves a message from one queue state to another atomically
	MoveToState(ctx context.Context, id string, fromState, toState QueueState) error

	// GetMessagesInState returns all messages in a specific state
	GetMessagesInState(ctx context.Context, state QueueState) ([]Message, error)

	// GetStateCount returns the number of messages in a specific state (fast operation when supported)
	GetStateCount(ctx context.Context, state QueueState) (int64, error)

	// ProcessMessage processes a message with the given handler
	ProcessMessage(ctx context.Context, id string, handler Handler) error

	// DeleteMessage removes a message from the queue
	DeleteMessage(ctx context.Context, id string) error

	// SetMessageRetryPolicy updates the retry policy for an existing message
	SetMessageRetryPolicy(ctx context.Context, id string, policyName string) error

	// Recover recovers queue state after restart (moves active messages back to incoming)
	Recover(ctx context.Context) error

	// Start starts the queue processing
	Start(ctx context.Context) error

	// Stop stops the queue processing
	Stop(ctx context.Context) error

	// Close closes the queue and releases resources
	Close() error
}

type retryPolicyKey struct{}

// ContextWithRetryPolicy returns a context with a named retry policy
func ContextWithRetryPolicy(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, retryPolicyKey{}, name)
}

// RetryPolicyFromContext returns the named retry policy from the context
func RetryPolicyFromContext(ctx context.Context) string {
	if name, ok := ctx.Value(retryPolicyKey{}).(string); ok {
		return name
	}
	return ""
}

// Handler processes messages
type Handler interface {
	// Handle processes a message with streaming data
	Handle(ctx context.Context, message Message, data MessageReader) error

	// Name returns the handler name
	Name() string

	// CanHandle checks if this handler can process the message
	CanHandle(headers map[string]string) bool
}

// RetryPolicy defines retry behavior
type RetryPolicy interface {
	// NextRetry calculates the next retry time
	NextRetry(attempts int, lastError error) time.Time

	// ShouldRetry determines if a message should be retried
	ShouldRetry(attempts int, maxAttempts int, lastError error) bool
}
