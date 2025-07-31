package retryspool

import (
	"context"
	"log/slog"
)

// Logger provides a structured logging interface for retryspool
type Logger interface {
	// Debug logs a debug message
	Debug(msg string, args ...any)
	// Info logs an info message
	Info(msg string, args ...any)
	// Warn logs a warning message
	Warn(msg string, args ...any)
	// Error logs an error message
	Error(msg string, args ...any)
}

// noOpLogger is a logger that does nothing - used when no logger is provided
type noOpLogger struct{}

func (n *noOpLogger) Debug(msg string, args ...any) {}
func (n *noOpLogger) Info(msg string, args ...any)  {}
func (n *noOpLogger) Warn(msg string, args ...any)  {}
func (n *noOpLogger) Error(msg string, args ...any) {}

// Note: *slog.Logger directly implements the Logger interface.
// You can pass *slog.Logger directly to WithLogger().

// ContextLogger wraps an slog.Logger with context support
type ContextLogger struct {
	logger *slog.Logger
	ctx    context.Context
}

// NewContextLogger creates a new ContextLogger from an slog.Logger and context
func NewContextLogger(logger *slog.Logger, ctx context.Context) *ContextLogger {
	return &ContextLogger{logger: logger, ctx: ctx}
}

func (c *ContextLogger) Debug(msg string, args ...any) {
	c.logger.DebugContext(c.ctx, msg, args...)
}

func (c *ContextLogger) Info(msg string, args ...any) {
	c.logger.InfoContext(c.ctx, msg, args...)
}

func (c *ContextLogger) Warn(msg string, args ...any) {
	c.logger.WarnContext(c.ctx, msg, args...)
}

func (c *ContextLogger) Error(msg string, args ...any) {
	c.logger.ErrorContext(c.ctx, msg, args...)
}
