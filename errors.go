package retryspool

import "errors"

var (
	// ErrMessageNotFound is returned when a message is not found
	ErrMessageNotFound = errors.New("message not found")

	// ErrInvalidState is returned when attempting an invalid state transition
	ErrInvalidState = errors.New("invalid state transition")

	// ErrQueueClosed is returned when operating on a closed queue
	ErrQueueClosed = errors.New("queue is closed")

	// ErrHandlerNotFound is returned when no handler is found for a message
	ErrHandlerNotFound = errors.New("no handler found for message")

	// ErrMaxAttemptsExceeded is returned when a message has exceeded retry attempts
	ErrMaxAttemptsExceeded = errors.New("maximum retry attempts exceeded")

	// ErrMessageLocked is returned when a message is locked for processing
	ErrMessageLocked = errors.New("message is locked for processing")
)

// RetryableError indicates a temporary failure that should be retried
type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string {
	return e.Err.Error()
}

func (e RetryableError) Unwrap() error {
	return e.Err
}

// IsRetryable checks if an error is retryable
func IsRetryable(err error) bool {
	var retryableErr RetryableError
	return errors.As(err, &retryableErr)
}

// NewRetryableError creates a retryable error
func NewRetryableError(err error) error {
	return RetryableError{Err: err}
}

// PermanentError indicates a permanent failure that should not be retried
type PermanentError struct {
	Err error
}

func (e PermanentError) Error() string {
	return e.Err.Error()
}

func (e PermanentError) Unwrap() error {
	return e.Err
}

// IsPermanent checks if an error is permanent
func IsPermanent(err error) bool {
	var permErr PermanentError
	return errors.As(err, &permErr)
}

// NewPermanentError creates a permanent error
func NewPermanentError(err error) error {
	return PermanentError{Err: err}
}
