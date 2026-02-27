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

	// BeginEnqueue starts a streaming enqueue transaction.
	// Data can be written to the returned transaction, headers can be
	// added/modified at any time, and the message is only visible in
	// the queue after Commit() is called.
	BeginEnqueue(ctx context.Context) (EnqueueTransaction, error)

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

// EnqueueTransaction represents an in-progress message enqueue.
// The transaction implements io.Writer for streaming body data.
// Headers can be set at any point before Commit().
// The message only becomes visible in the queue after Commit().
type EnqueueTransaction interface {
	// io.Writer – Body-Daten streamen
	io.Writer

	// MessageID returns the pre-allocated message ID.
	// Available immediately after BeginEnqueue().
	MessageID() string

	// SetHeader sets or overwrites a single metadata header.
	// Can be called at any time before Commit(), even after body writes.
	SetHeader(key, value string)

	// SetHeaders merges multiple headers. Same rules as SetHeader.
	SetHeaders(headers map[string]string)

	// Commit finalizes the enqueue: closes the data stream, writes
	// metadata with all collected headers, and makes the message
	// visible in the queue (state = INCOMING).
	// After Commit(), the transaction must not be used anymore.
	Commit() (string, error)

	// Abort cancels the transaction: stops the data stream and
	// cleans up any partially written data.
	// Safe to call multiple times. Safe to call after Commit (no-op).
	Abort() error
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

type messageIDKey struct{}

// ContextWithMessageID returns a context with a message ID
func ContextWithMessageID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, messageIDKey{}, id)
}

// MessageIDFromContext returns the message ID from the context
func MessageIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(messageIDKey{}).(string); ok {
		return id
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
