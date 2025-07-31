package retryspool_test

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"schneider.vip/retryspool"
	datafs "schneider.vip/retryspool/storage/data/filesystem"
	metafs "schneider.vip/retryspool/storage/meta/filesystem"
)

// Example handler implementations for examples

type printHandler struct{}

func (h *printHandler) Handle(ctx context.Context, msg retryspool.Message, reader retryspool.MessageReader) error {
	content := make([]byte, 1024)
	n, _ := reader.Read(content)
	slog.Info("Processing message", "id", msg.ID, "content", string(content[:n]))
	return nil
}

func (h *printHandler) Name() string {
	return "print"
}

func (h *printHandler) CanHandle(headers map[string]string) bool {
	return true
}

type exampleHandler struct{}

func (h *exampleHandler) Handle(ctx context.Context, msg retryspool.Message, reader retryspool.MessageReader) error {
	return nil // Successfully process message
}

func (h *exampleHandler) Name() string {
	return "example"
}

func (h *exampleHandler) CanHandle(headers map[string]string) bool {
	return true
}

type processorHandler struct{}

func (h *processorHandler) Handle(ctx context.Context, msg retryspool.Message, reader retryspool.MessageReader) error {
	// Process the message
	slog.Info("Processing message", "id", msg.ID)
	return nil
}

func (h *processorHandler) Name() string {
	return "processor"
}

func (h *processorHandler) CanHandle(headers map[string]string) bool {
	return true
}

type bounceHandler struct{}

func (h *bounceHandler) Handle(ctx context.Context, msg retryspool.Message, reader retryspool.MessageReader) error {
	// Handle bounced messages
	slog.Warn("Message bounced", "id", msg.ID, "attempts", msg.Metadata.Attempts)
	return nil
}

func (h *bounceHandler) Name() string {
	return "bouncer"
}

func (h *bounceHandler) CanHandle(headers map[string]string) bool {
	return true
}

type customHandler struct{}

func (h *customHandler) Handle(ctx context.Context, msg retryspool.Message, reader retryspool.MessageReader) error {
	return nil
}

func (h *customHandler) Name() string {
	return "custom"
}

func (h *customHandler) CanHandle(headers map[string]string) bool {
	return true
}

// ExampleQueue demonstrates basic queue usage
func ExampleQueue() {
	// Create storage factories
	dataFactory := datafs.NewFactory("./data")
	metaFactory := metafs.NewFactory("./meta")

	// Simple handler that just prints messages
	handler := &printHandler{}

	// Create queue
	queue := retryspool.New(
		retryspool.WithDataStorage(dataFactory),
		retryspool.WithMetaStorage(metaFactory),
		retryspool.WithActiveHandler(handler),
		retryspool.WithMaxAttempts(3),
	)
	defer queue.Close()

	ctx := context.Background()

	// Enqueue message
	msgID, err := queue.Enqueue(ctx, map[string]string{
		"recipient": "user@example.com",
		"subject":   "Test Message",
	}, strings.NewReader("Hello, World!"))

	if err != nil {
		slog.Error("Failed to enqueue", "error", err)
		return
	}

	slog.Info("Message enqueued", "id", msgID)

	// Start processing
	err = queue.Start(ctx)
	if err != nil {
		slog.Error("Failed to start queue", "error", err)
	}
}

// ExampleQueue_withLogging demonstrates queue with structured logging
func ExampleQueue_withLogging() {
	// Create JSON logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create storage factories
	dataFactory := datafs.NewFactory("./data")
	metaFactory := metafs.NewFactory("./meta")

	// Simple handler
	handler := &exampleHandler{}

	// Create queue with logging
	queue := retryspool.New(
		retryspool.WithDataStorage(dataFactory),
		retryspool.WithMetaStorage(metaFactory),
		retryspool.WithActiveHandler(handler),
		retryspool.WithLogger(logger),
		retryspool.WithMaxAttempts(3),
		retryspool.WithProcessInterval(10*time.Second),
	)
	defer queue.Close()

	ctx := context.Background()

	// Enqueue message
	msgID, _ := queue.Enqueue(ctx, map[string]string{
		"type": "notification",
	}, strings.NewReader("Important notification"))

	logger.Info("Message enqueued", "message_id", msgID)

	// Start processing
	queue.Start(ctx)
}

// ExampleQueue_multipleHandlers demonstrates state-based handler registration
func ExampleQueue_multipleHandlers() {
	// Create storage
	dataFactory := datafs.NewFactory("./data")
	metaFactory := metafs.NewFactory("./meta")

	// Active handler for processing messages
	activeHandler := &processorHandler{}

	// Bounce handler for failed messages
	bounceHandler := &bounceHandler{}

	// Create queue with multiple handlers
	queue := retryspool.New(
		retryspool.WithDataStorage(dataFactory),
		retryspool.WithMetaStorage(metaFactory),
		retryspool.WithActiveHandler(activeHandler),
		retryspool.WithBounceHandler(bounceHandler),
		retryspool.WithMaxAttempts(3),
	)
	defer queue.Close()

	ctx := context.Background()

	// Enqueue message
	queue.Enqueue(ctx, map[string]string{
		"priority": "high",
	}, strings.NewReader("High priority message"))

	// Start processing
	queue.Start(ctx)
}

// ExampleQueue_customConfiguration demonstrates advanced configuration
func ExampleQueue_customConfiguration() {
	// Custom storage locations
	dataFactory := datafs.NewFactory("/var/spool/retryspool/data")
	metaFactory := metafs.NewFactory("/var/spool/retryspool/meta")

	// Handler
	handler := &customHandler{}

	// Create queue with custom configuration
	queue := retryspool.New(
		retryspool.WithDataStorage(dataFactory),
		retryspool.WithMetaStorage(metaFactory),
		retryspool.WithActiveHandler(handler),
		retryspool.WithMaxAttempts(5),                  // More retry attempts
		retryspool.WithProcessInterval(30*time.Second), // Less frequent processing
		retryspool.WithMaxConcurrency(20),              // Higher concurrency
		retryspool.WithDefaultPriority(10),             // Higher default priority
	)
	defer queue.Close()

	ctx := context.Background()

	// Enqueue with custom priority
	queue.Enqueue(ctx, map[string]string{
		"priority": "1", // Override default priority
		"category": "urgent",
	}, strings.NewReader("Urgent message with custom settings"))

	queue.Start(ctx)
}
