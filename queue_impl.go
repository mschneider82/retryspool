package retryspool

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	datafs "schneider.vip/retryspool/storage/data/filesystem"
	metastorage "schneider.vip/retryspool/storage/meta"
	metafs "schneider.vip/retryspool/storage/meta/filesystem"
)

// queueImpl implements the Queue interface
type queueImpl struct {
	config    *Config
	storage   *combinedStorage  // Combined storage
	scheduler *messageScheduler // Centralized scheduler

	// Runtime state
	running atomic.Bool
	stopCh  chan struct{}
	mu      sync.RWMutex
}

// New creates a new queue with the given options
func New(options ...Option) Queue {
	config := defaultConfig()
	for _, opt := range options {
		if err := opt(config); err != nil {
			panic(fmt.Sprintf("failed to apply option: %v", err))
		}
	}

	// Set default storage factories if not specified
	if config.DataFactory == nil {
		// Default to filesystem storage in ./retryspool/data
		defaultDataDir := filepath.Join(".", "retryspool", "data")
		if err := os.MkdirAll(defaultDataDir, 0o755); err != nil {
			panic(fmt.Sprintf("failed to create default data directory: %v", err))
		}
		config.DataFactory = datafs.NewFactory(defaultDataDir)
	}

	if config.MetaFactory == nil {
		// Default to filesystem storage in ./retryspool/meta
		defaultMetaDir := filepath.Join(".", "retryspool", "meta")
		if err := os.MkdirAll(defaultMetaDir, 0o755); err != nil {
			panic(fmt.Sprintf("failed to create default meta directory: %v", err))
		}
		config.MetaFactory = metafs.NewFactory(defaultMetaDir)
	}

	// Create storage backends
	dataBackend, err := config.DataFactory.Create()
	if err != nil {
		panic(fmt.Sprintf("failed to create data storage backend: %v", err))
	}

	metaBackend, err := config.MetaFactory.Create()
	if err != nil {
		dataBackend.Close()
		panic(fmt.Sprintf("failed to create meta storage backend: %v", err))
	}

	// Create combined storage with middleware
	storage := &combinedStorage{
		dataStorage:     dataBackend,
		metaStorage:     metaBackend,
		dataMiddlewares: config.DataMiddlewares,
		metaMiddlewares: config.MetaMiddlewares,
	}

	// Create queue instance
	queue := &queueImpl{
		config:  config,
		storage: storage,
		stopCh:  make(chan struct{}),
	}

	// Create scheduler with queue-specific settings
	schedulerConfig := defaultSchedulerConfig()
	schedulerConfig.MaxActiveMessages = config.MaxActiveMessages
	schedulerConfig.WorkerCount = config.MaxConcurrency
	queue.scheduler = newMessageScheduler(schedulerConfig, storage, queue)

	return queue
}

// Enqueue adds a new message to the queue
func (q *queueImpl) Enqueue(ctx context.Context, headers map[string]string, data io.Reader) (string, error) {
	// Generate unique message ID
	id := q.generateMessageID()

	// Create message metadata
	// Initialize headers if nil to prevent nil pointer issues
	if headers == nil {
		headers = make(map[string]string)
	}

	// Resolve retry policy name from context or headers
	policyName := RetryPolicyFromContext(ctx)
	if policyName == "" {
		policyName = headers["x-retry-policy"]
	}

	metadata := MessageMetadata{
		State:           StateIncoming,
		Attempts:        0,
		MaxAttempts:     q.config.MaxAttempts,
		NextRetry:       time.Now(),
		Created:         time.Now(),
		Updated:         time.Now(),
		Priority:        q.config.DefaultPriority,
		Headers:         headers,
		RetryPolicyName: policyName,
	}

	// Set priority from headers if specified
	if priority := metadata.Headers["priority"]; priority != "" {
		// Parse priority (simple implementation)
		switch priority {
		case "1":
			metadata.Priority = 1
		case "2":
			metadata.Priority = 2
		case "3":
			metadata.Priority = 3
		case "4":
			metadata.Priority = 4
		case "5":
			metadata.Priority = 5
		}
	}

	// Store message in storage
	err := q.storage.StoreMessage(ctx, id, headers, data, q.config.MaxAttempts, q.config.DefaultPriority, policyName)
	if err != nil {
		return "", fmt.Errorf("failed to store message: %w", err)
	}

	q.config.Logger.Info("Message enqueued", "message_id", id, "state", metadata.State)

	// Trigger immediate scheduling (unless disabled)
	if q.scheduler != nil && q.running.Load() && !q.config.DisableImmediateTrigger {
		q.scheduler.TriggerImmediate()
	}

	return id, nil
}

// GetMessage retrieves message metadata and data reader
func (q *queueImpl) GetMessage(ctx context.Context, id string) (Message, MessageReader, error) {
	metadata, err := q.storage.GetMessageMetadata(ctx, id)
	if err != nil {
		return Message{}, nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	reader, err := q.storage.GetMessageReader(ctx, id)
	if err != nil {
		return Message{}, nil, fmt.Errorf("failed to get reader: %w", err)
	}

	message := Message{
		ID:       id,
		Metadata: q.convertFromMetaStorageMetadata(metadata),
	}

	return message, &messageReader{ReadCloser: reader, size: metadata.Size}, nil
}

// GetMessageMetadata retrieves only message metadata
func (q *queueImpl) GetMessageMetadata(ctx context.Context, id string) (MessageMetadata, error) {
	metadata, err := q.storage.GetMessageMetadata(ctx, id)
	if err != nil {
		return MessageMetadata{}, fmt.Errorf("failed to get metadata: %w", err)
	}

	return q.convertFromMetaStorageMetadata(metadata), nil
}

// UpdateMessageMetadata updates message metadata
func (q *queueImpl) UpdateMessageMetadata(ctx context.Context, id string, metadata MessageMetadata) error {
	metadata.Updated = time.Now()
	err := q.storage.UpdateMessageMetadata(ctx, id, q.convertToMetaStorageMetadata(id, metadata))
	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}
	return nil
}

// MoveToState moves a message from one queue state to another atomically.
// The storage layer guarantees CAS semantics — no read-check-write needed.
func (q *queueImpl) MoveToState(ctx context.Context, id string, fromState, toState QueueState) error {
	err := q.storage.MoveToState(ctx, id, q.convertToMetaStorageState(fromState), q.convertToMetaStorageState(toState))
	if err != nil {
		return fmt.Errorf("failed to move message %s from %s to %s: %w", id, fromState, toState, err)
	}

	q.config.Logger.Info("Message moved",
		"message_id", id,
		"from_state", fromState,
		"to_state", toState)
	return nil
}

// GetMessagesInState returns all messages in a specific state
// WARNING: This method loads all messages into memory. For large queues, consider using
// GetMessagesInStateStream or the iterator directly for better memory efficiency.
func (q *queueImpl) GetMessagesInState(ctx context.Context, state QueueState) ([]Message, error) {
	// Use iterator to stream messages instead of loading all into memory
	iterator, err := q.storage.NewMessageIterator(ctx, q.convertToMetaStorageState(state), 100)
	if err != nil {
		return nil, fmt.Errorf("failed to create message iterator: %w", err)
	}
	defer iterator.Close()

	var messages []Message
	// Set a reasonable limit to prevent excessive memory usage
	const maxMessages = 10000
	messageCount := 0

	for {
		// Check context cancellation periodically
		select {
		case <-ctx.Done():
			q.config.Logger.Debug("Context cancelled during GetMessagesInState", "state", state, "loaded", messageCount)
			return messages, ctx.Err()
		default:
			// Continue processing
		}

		metadata, hasMore, err := iterator.Next(ctx)
		if err != nil {
			q.config.Logger.Warn("Failed to get next message from iterator", "error", err)
			continue
		}

		// Check if iterator is exhausted (empty message ID indicates end)
		if metadata.ID == "" {
			q.config.Logger.Debug("Iterator exhausted", "messageCount", messageCount)
			break
		}

		// Safety check: prevent excessive memory usage
		if messageCount >= maxMessages {
			q.config.Logger.Warn("Reached maximum message limit in GetMessagesInState", "state", state, "limit", maxMessages)
			break
		}

		// Convert metadata to Message
		message := Message{
			ID: metadata.ID,
			Metadata: MessageMetadata{
				State:       q.convertFromMetaStorageState(metadata.State),
				Attempts:    metadata.Attempts,
				MaxAttempts: metadata.MaxAttempts,
				NextRetry:   metadata.NextRetry,
				Created:     metadata.Created,
				Updated:     metadata.Updated,
				LastError:   metadata.LastError,
				Size:        metadata.Size,
				Priority:    metadata.Priority,
				Headers:     metadata.Headers,
			},
		}

		messages = append(messages, message)
		messageCount++

		// Break if this was the last message
		if !hasMore {
			q.config.Logger.Debug("No more messages after this one", "messageCount", messageCount)
			break
		}
	}

	q.config.Logger.Debug("GetMessagesInState completed", "state", state, "count", messageCount)
	return messages, nil
}

// GetStateCount returns the number of messages in a specific state
func (q *queueImpl) GetStateCount(ctx context.Context, state QueueState) (int64, error) {
	// Check if the combined storage supports fast state counting
	if count, supported := q.storage.GetStateCount(q.convertToMetaStorageState(state)); supported {
		return count, nil
	}

	// Fallback: use Iterator to count (memory efficient)
	iterator, err := q.storage.NewMessageIterator(ctx, q.convertToMetaStorageState(state), 100)
	if err != nil {
		return 0, fmt.Errorf("failed to create message iterator for counting: %w", err)
	}
	defer iterator.Close()

	var count int64
	for {
		// Check context cancellation periodically during counting
		select {
		case <-ctx.Done():
			q.config.Logger.Debug("Context cancelled during message counting", "state", state, "partial_count", count)
			return count, ctx.Err()
		default:
			// Continue counting
		}

		metadata, hasMore, err := iterator.Next(ctx)
		if err != nil {
			q.config.Logger.Warn("Error during message counting", "error", err)
			continue
		}

		// Check if iterator is exhausted (empty message ID indicates end)
		if metadata.ID == "" {
			break
		}

		count++

		// Break if this was the last message
		if !hasMore {
			break
		}
	}

	q.config.Logger.Debug("Message counting completed", "state", state, "count", count)
	return count, nil
}

// ProcessMessage processes a message with the given handler
func (q *queueImpl) ProcessMessage(ctx context.Context, id string, handler Handler) error {
	// Get current message metadata
	message, reader, err := q.GetMessage(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get message: %w", err)
	}

	// IMPORTANT: Close reader immediately after handler processing to prevent race conditions
	// The reader must be closed before any metadata updates to avoid "message data not found" errors
	var handlerErr error
	func() {
		defer reader.Close() // Ensure reader is closed in this scope

		// Only move to active if not already in active state
		if message.Metadata.State != StateActive {
			// Atomic state transition: only move if still in expected state
			err = q.MoveToState(ctx, id, message.Metadata.State, StateActive)
			if err != nil {
				// If state changed (e.g., bounced by another goroutine), skip processing
				q.config.Logger.Warn("Message state transition failed, skipping processing", "message_id", id, "error", err)
				handlerErr = nil // Not a handler error, just skip processing
				return
			}

			// Update message state (already moved to active in scheduler)
			message.Metadata.State = StateActive
		}

		q.config.Logger.Info("Processing message", "message_id", id, "handler", handler.Name())

		// Add message ID to context automatically so handlers can use it
		ctx = ContextWithMessageID(ctx, id)

		// Process message with handler - reader will be closed after this
		handlerErr = handler.Handle(ctx, message, reader)
	}()

	// Reader is now guaranteed to be closed before any metadata operations

	if handlerErr != nil {
		// Update metadata with error
		message.Metadata.LastError = handlerErr.Error()
		message.Metadata.Attempts++

		q.config.Logger.Error("Message processing failed",
			"message_id", id,
			"attempt", message.Metadata.Attempts,
			"max_attempts", message.Metadata.MaxAttempts,
			"error", handlerErr)

		// Check if error is retryable and we haven't exceeded max attempts
		isRetryable := q.isRetryableError(handlerErr)

		// Resolve retry policy
		policy := q.config.RetryPolicy
		if message.Metadata.RetryPolicyName != "" {
			if p, ok := q.config.NamedPolicies[message.Metadata.RetryPolicyName]; ok {
				policy = p
			}
		}

		canRetry := policy != nil && policy.ShouldRetry(message.Metadata.Attempts, message.Metadata.MaxAttempts, handlerErr)

		if isRetryable && canRetry {
			// Move to deferred state for retry
			message.Metadata.State = StateDeferred
			message.Metadata.NextRetry = policy.NextRetry(message.Metadata.Attempts, handlerErr)
			q.config.Logger.Info("Message deferred for retry",
				"message_id", id,
				"attempt", message.Metadata.Attempts,
				"max_attempts", message.Metadata.MaxAttempts,
				"next_retry", message.Metadata.NextRetry)
		} else {
			// Move to bounce state
			message.Metadata.State = StateBounce
			if !isRetryable {
				q.config.Logger.Error("Message bounced due to permanent failure", "message_id", id, "error", handlerErr)
			} else {
				q.config.Logger.Error("Message bounced after max attempts exceeded", "message_id", id, "attempts", message.Metadata.Attempts)
			}
		}

		return q.UpdateMessageMetadata(ctx, id, message.Metadata)
	}

	// Success - delete message
	q.config.Logger.Info("Message processed successfully", "message_id", id)
	return q.DeleteMessage(ctx, id)
}

// DeleteMessage removes a message from the queue
func (q *queueImpl) DeleteMessage(ctx context.Context, id string) error {
	err := q.storage.DeleteMessage(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	q.config.Logger.Info("Message deleted", "message_id", id)
	return nil
}

// SetMessageRetryPolicy updates the retry policy for an existing message
func (q *queueImpl) SetMessageRetryPolicy(ctx context.Context, id string, policyName string) error {
	metadata, err := q.GetMessageMetadata(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get message metadata: %w", err)
	}

	metadata.RetryPolicyName = policyName
	return q.UpdateMessageMetadata(ctx, id, metadata)
}

// Recover recovers queue state after restart (Postfix-style recovery)
func (q *queueImpl) Recover(ctx context.Context) error {
	q.config.Logger.Info("Starting queue recovery")

	// Move active messages back to incoming state for reprocessing
	// This ensures proper recovery behavior where interrupted messages get requeued
	activeMessages, err := q.GetMessagesInState(ctx, StateActive)
	if err != nil {
		return fmt.Errorf("failed to get active messages: %w", err)
	}

	movedCount := 0
	for _, message := range activeMessages {
		err := q.MoveToState(ctx, message.ID, StateActive, StateIncoming)
		if err != nil {
			q.config.Logger.Warn("Failed to move active message to incoming during recovery", "message_id", message.ID, "error", err)
			continue
		}
		movedCount++
	}

	if movedCount > 0 {
		q.config.Logger.Info("Moved active messages to incoming for reprocessing", "count", movedCount)
	}

	// Check for deferred messages that are ready for retry
	deferredMessages, err := q.GetMessagesInState(ctx, StateDeferred)
	if err != nil {
		return fmt.Errorf("failed to get deferred messages: %w", err)
	}

	now := time.Now()
	readyCount := 0
	for _, message := range deferredMessages {
		if message.Metadata.NextRetry.Before(now) {
			// Move ready deferred messages to active (not incoming)
			err := q.MoveToState(ctx, message.ID, StateDeferred, StateActive)
			if err != nil {
				q.config.Logger.Warn("Failed to requeue message", "message_id", message.ID, "error", err)
				continue
			}
			q.config.Logger.Info("Requeued deferred message for retry", "message_id", message.ID)
			readyCount++
		}
	}

	if readyCount > 0 {
		q.config.Logger.Info("Moved deferred messages to active for retry", "count", readyCount)
	}

	q.config.Logger.Info("Queue recovery completed")
	return nil
}

// Start starts the queue processing
func (q *queueImpl) Start(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.running.Swap(true) {
		return fmt.Errorf("queue is already running")
	}

	// Run recovery first
	err := q.Recover(ctx)
	if err != nil {
		q.running.Store(false)
		return fmt.Errorf("failed to recover queue: %w", err)
	}

	// Start the centralized scheduler
	err = q.scheduler.Start(ctx)
	if err != nil {
		q.running.Store(false)
		return fmt.Errorf("failed to start scheduler: %w", err)
	}

	q.config.Logger.Info("Queue started with centralized scheduler")
	return nil
}

// Stop stops the queue processing
func (q *queueImpl) Stop(ctx context.Context) error {
	if !q.running.Swap(false) {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Stop the centralized scheduler
	err := q.scheduler.Stop(ctx)
	if err != nil {
		q.config.Logger.Error("Error stopping scheduler", "error", err)
	}

	close(q.stopCh)

	q.config.Logger.Info("Queue stopped")
	return nil
}

// Close closes the queue and releases resources
func (q *queueImpl) Close() error {
	q.Stop(context.Background())
	return q.storage.Close()
}

// Helper methods

func (q *queueImpl) generateMessageID() string {
	return uuid.New().String()
}

func (q *queueImpl) convertToMetaStorageMetadata(id string, m MessageMetadata) metastorage.MessageMetadata {
	return metastorage.MessageMetadata{
		ID:              id,
		State:           q.convertToMetaStorageState(m.State),
		Attempts:        m.Attempts,
		MaxAttempts:     m.MaxAttempts,
		NextRetry:       m.NextRetry,
		Created:         m.Created,
		Updated:         m.Updated,
		LastError:       m.LastError,
		Size:            m.Size,
		Priority:        m.Priority,
		Headers:         m.Headers,
		RetryPolicyName: m.RetryPolicyName,
	}
}

func (q *queueImpl) convertFromMetaStorageMetadata(m metastorage.MessageMetadata) MessageMetadata {
	return MessageMetadata{
		State:           q.convertFromMetaStorageState(m.State),
		Attempts:        m.Attempts,
		MaxAttempts:     m.MaxAttempts,
		NextRetry:       m.NextRetry,
		Created:         m.Created,
		Updated:         m.Updated,
		LastError:       m.LastError,
		Size:            m.Size,
		Priority:        m.Priority,
		Headers:         m.Headers,
		RetryPolicyName: m.RetryPolicyName,
	}
}

func (q *queueImpl) convertToMetaStorageState(s QueueState) metastorage.QueueState {
	return metastorage.QueueState(s)
}

func (q *queueImpl) convertFromMetaStorageState(s metastorage.QueueState) QueueState {
	return QueueState(s)
}

// messageReader implements MessageReader
type messageReader struct {
	io.ReadCloser
	size int64
}

func (r *messageReader) Size() int64 {
	return r.size
}

// isRetryableError checks if an error should be retried
func (q *queueImpl) isRetryableError(err error) bool {
	// Check for RetryableError wrapper
	if IsRetryable(err) {
		return true
	}

	// Check if handler implements RetryableHandler interface
	if retryableHandler, ok := err.(interface{ IsRetryable(error) bool }); ok {
		return retryableHandler.IsRetryable(err)
	}

	// Default: assume non-retryable
	return false
}
