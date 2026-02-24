package retryspool

import (
	"context"
	"fmt"
	"io"
	"time"

	"schneider.vip/retryspool/middleware"
	datastorage "schneider.vip/retryspool/storage/data"
	metastorage "schneider.vip/retryspool/storage/meta"
)

// combinedStorage combines data and metadata storage with atomic operations
type combinedStorage struct {
	dataStorage     datastorage.Backend
	metaStorage     metastorage.Backend
	dataMiddlewares []middleware.Middleware
	metaMiddlewares []middleware.Middleware
}

// writerWithClosers holds a writer and all its middleware closers
type writerWithClosers struct {
	writer  io.Writer
	closers []io.Closer
}

// wrapWriterWithMiddlewares wraps a writer with middleware in the specified order
func (c *combinedStorage) wrapWriterWithMiddlewares(w io.Writer, middlewares []middleware.Middleware) *writerWithClosers {
	wrapped := w
	var closers []io.Closer

	// Apply middlewares in order (first middleware wraps the original writer, second wraps the first, etc.)
	for _, mw := range middlewares {
		wrapped = mw.Writer(wrapped)
		// If the wrapped writer implements io.Closer, add it to our closers list
		if closer, ok := wrapped.(io.Closer); ok {
			closers = append(closers, closer)
		}
	}

	return &writerWithClosers{
		writer:  wrapped,
		closers: closers,
	}
}

// Write implements io.Writer
func (w *writerWithClosers) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

// Close closes all middleware writers in reverse order (outermost first)
func (w *writerWithClosers) Close() error {
	var firstErr error
	// Close in reverse order (outermost first)
	for i := len(w.closers) - 1; i >= 0; i-- {
		if err := w.closers[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// wrapReaderWithMiddlewares wraps a reader with middleware in the same order as writing
func (c *combinedStorage) wrapReaderWithMiddlewares(r io.Reader, middlewares []middleware.Middleware) io.Reader {
	wrapped := r
	// Apply middlewares in SAME order for decoding to match the write order
	//
	// Write path creates this chain: data → encryption → compression → storage
	// If middlewares = [compression, encryption], write order is:
	// - compression.Writer(pipe) creates compression_writer
	// - encryption.Writer(compression_writer) creates encryption_writer
	// - Data flows: data → encryption_writer → compression_writer → pipe
	//
	// So read should be: pipe → decompression → decryption
	// That means we should apply compression.Reader first, then encryption.Reader
	for _, mw := range middlewares {
		wrapped = mw.Reader(wrapped)
	}
	return wrapped
}

// closeMiddlewareWriters closes all middleware writers in the chain properly
func (c *combinedStorage) closeMiddlewareWriters(wrappedWriter *writerWithClosers) {
	// Close all middleware writers in reverse order (outermost first)
	wrappedWriter.Close()
}

// StoreMessage stores both message data and metadata atomically
func (c *combinedStorage) StoreMessage(ctx context.Context, messageID string, headers map[string]string, data io.Reader, maxAttempts int, priority int, retryPolicyName string) error {
	// For storing data, we need to use a different approach since StoreData expects a Reader
	// but we need to apply middleware to the Writer. We'll use a pipe to handle this.
	var size int64
	var err error

	if len(c.dataMiddlewares) > 0 {
		// Use a pipe to apply middleware during writing
		pr, pw := io.Pipe()

		// Start a goroutine to write data through middleware
		go func() {
			defer func() {
				// Close the pipe writer last, after all middleware is closed
				pw.Close()
			}()

			// Wrap the pipe writer with middleware
			wrappedWriter := c.wrapWriterWithMiddlewares(pw, c.dataMiddlewares)

			// Copy data through the middleware chain
			_, copyErr := io.Copy(wrappedWriter, data)
			if copyErr != nil {
				pw.CloseWithError(copyErr)
				return
			}

			// Close all middleware writers BEFORE closing the pipe writer
			c.closeMiddlewareWriters(wrappedWriter)
		}()

		// Store the processed data
		size, err = c.dataStorage.StoreData(ctx, messageID, pr)
	} else {
		// No middleware, store directly
		size, err = c.dataStorage.StoreData(ctx, messageID, data)
	}

	if err != nil {
		return fmt.Errorf("failed to store message data: %w", err)
	}

	// Create metadata
	// Initialize headers if nil to prevent nil pointer issues
	if headers == nil {
		headers = make(map[string]string)
	}

	metadata := metastorage.MessageMetadata{
		ID:              messageID,
		State:           metastorage.StateIncoming,
		Attempts:        0,
		MaxAttempts:     maxAttempts,
		NextRetry:       time.Now(),
		Created:         time.Now(),
		Updated:         time.Now(),
		Size:            size,
		Priority:        priority,
		Headers:         headers,
		RetryPolicyName: retryPolicyName,
	}

	// Store metadata
	err = c.metaStorage.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		// Clean up data if metadata storage fails
		c.dataStorage.DeleteData(ctx, messageID)
		return fmt.Errorf("failed to store message metadata: %w", err)
	}

	return nil
}

// GetMessage retrieves both message data and metadata
func (c *combinedStorage) GetMessage(ctx context.Context, messageID string) (metastorage.MessageMetadata, io.ReadCloser, error) {
	// Get metadata first
	metadata, err := c.metaStorage.GetMeta(ctx, messageID)
	if err != nil {
		return metastorage.MessageMetadata{}, nil, fmt.Errorf("failed to get message metadata: %w", err)
	}

	// Get data reader
	reader, err := c.dataStorage.GetDataReader(ctx, messageID)
	if err != nil {
		return metastorage.MessageMetadata{}, nil, fmt.Errorf("failed to get message data: %w", err)
	}

	// Wrap reader with middleware for decoding if configured
	if len(c.dataMiddlewares) > 0 {
		wrappedReader := c.wrapReaderWithMiddlewares(reader, c.dataMiddlewares)
		// We need to return a ReadCloser, so we wrap it
		return metadata, &middlewareReadCloser{Reader: wrappedReader, Closer: reader}, nil
	}

	return metadata, reader, nil
}

// GetMessageReader retrieves only message data reader (for compatibility)
func (c *combinedStorage) GetMessageReader(ctx context.Context, messageID string) (io.ReadCloser, error) {
	reader, err := c.dataStorage.GetDataReader(ctx, messageID)
	if err != nil {
		return nil, err
	}

	// Wrap reader with middleware for decoding if configured
	if len(c.dataMiddlewares) > 0 {
		wrappedReader := c.wrapReaderWithMiddlewares(reader, c.dataMiddlewares)
		// We need to return a ReadCloser, so we wrap it
		return &middlewareReadCloser{Reader: wrappedReader, Closer: reader}, nil
	}

	return reader, nil
}

// GetMessageMetadata retrieves only message metadata
func (c *combinedStorage) GetMessageMetadata(ctx context.Context, messageID string) (metastorage.MessageMetadata, error) {
	return c.metaStorage.GetMeta(ctx, messageID)
}

// UpdateMessageMetadata updates message metadata
func (c *combinedStorage) UpdateMessageMetadata(ctx context.Context, messageID string, metadata metastorage.MessageMetadata) error {
	metadata.Updated = time.Now()
	return c.metaStorage.UpdateMeta(ctx, messageID, metadata)
}

// MoveToState atomically moves a message to a different queue state
func (c *combinedStorage) MoveToState(ctx context.Context, messageID string, fromState, toState metastorage.QueueState) error {
	return c.metaStorage.MoveToState(ctx, messageID, fromState, toState)
}

// DeleteMessage removes both message data and metadata atomically
func (c *combinedStorage) DeleteMessage(ctx context.Context, messageID string) error {
	// Delete metadata first
	err := c.metaStorage.DeleteMeta(ctx, messageID)
	if err != nil && err != metastorage.ErrMessageNotFound {
		return fmt.Errorf("failed to delete message metadata: %w", err)
	}

	// Delete data
	err = c.dataStorage.DeleteData(ctx, messageID)
	if err != nil {
		// Note: We've already deleted metadata, so this is best effort
		return fmt.Errorf("failed to delete message data: %w", err)
	}

	return nil
}

// ListMessages lists messages with pagination and filtering
func (c *combinedStorage) ListMessages(ctx context.Context, state metastorage.QueueState, options metastorage.MessageListOptions) (metastorage.MessageListResult, error) {
	return c.metaStorage.ListMessages(ctx, state, options)
}

// NewMessageIterator creates an iterator for messages in a specific state
func (c *combinedStorage) NewMessageIterator(ctx context.Context, state metastorage.QueueState, batchSize int) (metastorage.MessageIterator, error) {
	return c.metaStorage.NewMessageIterator(ctx, state, batchSize)
}

// GetStateCount returns the cached count for a specific state if supported
func (c *combinedStorage) GetStateCount(state metastorage.QueueState) (int64, bool) {
	if stateCounter, ok := c.metaStorage.(metastorage.StateCounterBackend); ok {
		return stateCounter.GetStateCount(state), true
	}
	return 0, false // Not supported
}

// Close closes both storage backends
func (c *combinedStorage) Close() error {
	var dataErr, metaErr error

	if c.dataStorage != nil {
		dataErr = c.dataStorage.Close()
	}

	if c.metaStorage != nil {
		metaErr = c.metaStorage.Close()
	}

	if dataErr != nil {
		return fmt.Errorf("failed to close data storage: %w", dataErr)
	}

	if metaErr != nil {
		return fmt.Errorf("failed to close meta storage: %w", metaErr)
	}

	return nil
}

// middlewareReadCloser wraps a Reader with a separate Closer
type middlewareReadCloser struct {
	io.Reader
	io.Closer
}
