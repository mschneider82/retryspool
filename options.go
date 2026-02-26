package retryspool

import (
	"fmt"
	"time"

	"schneider.vip/retryspool/middleware"
	datastorage "schneider.vip/retryspool/storage/data"
	metastorage "schneider.vip/retryspool/storage/meta"
)

// Option configures a Queue
type Option func(*Config) error

// Config holds queue configuration
type Config struct {
	DataFactory     datastorage.Factory      // Data storage factory
	MetaFactory     metastorage.Factory      // Meta storage factory
	DataMiddlewares []middleware.Middleware  // Middleware for data storage
	MetaMiddlewares []middleware.Middleware  // Middleware for metadata storage
	StateHandlers   map[QueueState][]Handler // Handlers per queue state
	RetryPolicy     RetryPolicy
	MaxAttempts     int
	ProcessInterval time.Duration
	MaxConcurrency  int
	DefaultPriority int

	// Postfix-like queue management
	MaxActiveMessages    int // Maximum messages in active state simultaneously
	MaxIncomingBatch     int // Maximum incoming messages to process per tick
	MaxDeferredBatch     int // Maximum deferred messages to process per tick
	MaxBounceBatch       int // Maximum bounce messages to process per tick
	MessageBatchSize     int // Storage pagination batch size
	ConcurrentProcessors int // Maximum concurrent message processors

	// Scheduling behavior
	DisableImmediateTrigger bool // Disable TriggerImmediate() after Enqueue, rely only on scan interval
	DisableRecovery         bool // Disable Recover() during Start()

	// Multi-policy support
	NamedPolicies map[string]RetryPolicy

	// Logging
	Logger Logger // Optional logger (defaults to NoOpLogger)

	// Archive behavior
	ArchiveSuccess bool // If true, successfully processed messages are moved to StateArchived instead of being deleted
	ArchiveBounce  bool // If true, bounced messages are moved to StateArchived instead of remaining in StateBounce
}

// defaultConfig returns a default configuration
func defaultConfig() *Config {
	return &Config{
		RetryPolicy:     DefaultExponentialRetryPolicy(), // Use exponential backoff by default
		MaxAttempts:     10,                              // Increased for better retry behavior
		ProcessInterval: 10 * time.Second,
		MaxConcurrency:  2,
		DefaultPriority: 5,
		StateHandlers:   make(map[QueueState][]Handler),

		// Postfix-like defaults
		MaxActiveMessages:    50, // Similar to postfix default_process_limit
		MaxIncomingBatch:     20, // Similar to postfix qmgr_message_active_limit
		MaxDeferredBatch:     20, // Process fewer deferred messages
		MaxBounceBatch:       10, // Process fewer bounce messages
		MessageBatchSize:     20, // Storage pagination size
		ConcurrentProcessors: 2,  // Maximum concurrent processors

		// Scheduling behavior defaults
		DisableImmediateTrigger: false, // Default: immediate trigger enabled
		DisableRecovery:         false, // Default: recovery enabled during start

		// Multi-policy support
		NamedPolicies: make(map[string]RetryPolicy),

		// Logging defaults to no-op (silent)
		Logger: &noOpLogger{},

		// Archive behavior defaults
		ArchiveSuccess: false,
		ArchiveBounce:  false,

		// Default storage will be set to filesystem if not specified
		DataFactory: nil, // Will default to filesystem
		MetaFactory: nil, // Will default to filesystem
	}
}

// WithDataStorage sets the data storage factory with optional middleware
func WithDataStorage(factory datastorage.Factory, middlewares ...middleware.Middleware) Option {
	return func(c *Config) error {
		if factory == nil {
			return fmt.Errorf("data storage factory cannot be nil")
		}
		c.DataFactory = factory
		// Add middleware to the data storage specific list
		c.DataMiddlewares = append(c.DataMiddlewares, middlewares...)
		return nil
	}
}

// WithMetaStorage sets the metadata storage factory with optional middleware
func WithMetaStorage(factory metastorage.Factory, middlewares ...middleware.Middleware) Option {
	return func(c *Config) error {
		if factory == nil {
			return fmt.Errorf("metadata storage factory cannot be nil")
		}
		c.MetaFactory = factory
		// Add middleware to the metadata storage specific list
		c.MetaMiddlewares = append(c.MetaMiddlewares, middlewares...)
		return nil
	}
}

// WithActiveHandler registers a handler for active message delivery
func WithActiveHandler(handler Handler) Option {
	return func(c *Config) error {
		if handler == nil {
			return fmt.Errorf("handler cannot be nil")
		}
		c.StateHandlers[StateActive] = append(c.StateHandlers[StateActive], handler)
		return nil
	}
}

// WithBounceHandler registers a handler for bounced messages
func WithBounceHandler(handler Handler) Option {
	return func(c *Config) error {
		if handler == nil {
			return fmt.Errorf("handler cannot be nil")
		}
		c.StateHandlers[StateBounce] = append(c.StateHandlers[StateBounce], handler)
		return nil
	}
}

// WithRetryPolicy sets the global default retry policy
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(c *Config) error {
		if policy == nil {
			return fmt.Errorf("retry policy cannot be nil")
		}
		c.RetryPolicy = policy
		return nil
	}
}

// WithNamedRetryPolicy registers a named retry policy
func WithNamedRetryPolicy(name string, policy RetryPolicy) Option {
	return func(c *Config) error {
		if name == "" {
			return fmt.Errorf("retry policy name cannot be empty")
		}
		if policy == nil {
			return fmt.Errorf("retry policy cannot be nil")
		}
		c.NamedPolicies[name] = policy
		return nil
	}
}

// WithMaxAttempts sets the maximum number of retry attempts
func WithMaxAttempts(attempts int) Option {
	return func(c *Config) error {
		if attempts < 1 {
			return fmt.Errorf("max attempts must be at least 1, got %d", attempts)
		}
		if attempts > 1000 {
			return fmt.Errorf("max attempts too high (max 1000), got %d", attempts)
		}
		c.MaxAttempts = attempts
		return nil
	}
}

// WithProcessInterval sets the interval for processing deferred messages
func WithProcessInterval(interval time.Duration) Option {
	return func(c *Config) error {
		if interval < time.Second {
			return fmt.Errorf("process interval too short (min 1s), got %v", interval)
		}
		if interval > 24*time.Hour {
			return fmt.Errorf("process interval too long (max 24h), got %v", interval)
		}
		c.ProcessInterval = interval
		return nil
	}
}

// WithMaxConcurrency sets the maximum number of concurrent message processors
func WithMaxConcurrency(concurrency int) Option {
	return func(c *Config) error {
		if concurrency < 1 {
			return fmt.Errorf("max concurrency must be at least 1, got %d", concurrency)
		}
		if concurrency > 10000 {
			return fmt.Errorf("max concurrency too high (max 10000), got %d", concurrency)
		}
		c.MaxConcurrency = concurrency
		return nil
	}
}

// WithDefaultPriority sets the default message priority
func WithDefaultPriority(priority int) Option {
	return func(c *Config) error {
		if priority < 0 {
			return fmt.Errorf("default priority cannot be negative, got %d", priority)
		}
		if priority > 100 {
			return fmt.Errorf("default priority too high (max 100), got %d", priority)
		}
		c.DefaultPriority = priority
		return nil
	}
}

// WithMaxActiveMessages sets the maximum number of messages in active state
func WithMaxActiveMessages(max int) Option {
	return func(c *Config) error {
		if max < 1 {
			return fmt.Errorf("max active messages must be at least 1, got %d", max)
		}
		if max > 100000 {
			return fmt.Errorf("max active messages too high (max 100000), got %d", max)
		}
		c.MaxActiveMessages = max
		return nil
	}
}

// WithMaxIncomingBatch sets the maximum incoming messages to process per tick
func WithMaxIncomingBatch(max int) Option {
	return func(c *Config) error {
		if max < 1 {
			return fmt.Errorf("max incoming batch must be at least 1, got %d", max)
		}
		if max > 10000 {
			return fmt.Errorf("max incoming batch too high (max 10000), got %d", max)
		}
		c.MaxIncomingBatch = max
		return nil
	}
}

// WithMaxDeferredBatch sets the maximum deferred messages to process per tick
func WithMaxDeferredBatch(max int) Option {
	return func(c *Config) error {
		if max < 1 {
			return fmt.Errorf("max deferred batch must be at least 1, got %d", max)
		}
		if max > 10000 {
			return fmt.Errorf("max deferred batch too high (max 10000), got %d", max)
		}
		c.MaxDeferredBatch = max
		return nil
	}
}

// WithMaxBounceBatch sets the maximum bounce messages to process per tick
func WithMaxBounceBatch(max int) Option {
	return func(c *Config) error {
		if max < 1 {
			return fmt.Errorf("max bounce batch must be at least 1, got %d", max)
		}
		if max > 10000 {
			return fmt.Errorf("max bounce batch too high (max 10000), got %d", max)
		}
		c.MaxBounceBatch = max
		return nil
	}
}

// WithMessageBatchSize sets the storage pagination batch size
func WithMessageBatchSize(size int) Option {
	return func(c *Config) error {
		if size < 1 {
			return fmt.Errorf("message batch size must be at least 1, got %d", size)
		}
		if size > 10000 {
			return fmt.Errorf("message batch size too high (max 10000), got %d", size)
		}
		c.MessageBatchSize = size
		return nil
	}
}

// WithConcurrentProcessors sets the maximum concurrent message processors
func WithConcurrentProcessors(max int) Option {
	return func(c *Config) error {
		if max < 1 {
			return fmt.Errorf("concurrent processors must be at least 1, got %d", max)
		}
		if max > 10000 {
			return fmt.Errorf("concurrent processors too high (max 10000), got %d", max)
		}
		c.ConcurrentProcessors = max
		return nil
	}
}

// WithLogger sets the logger for the queue
// Pass nil or don't use this option to disable logging (default behavior)
func WithLogger(logger Logger) Option {
	return func(c *Config) error {
		if logger == nil {
			c.Logger = &noOpLogger{}
		} else {
			c.Logger = logger
		}
		return nil
	}
}

// WithDisableImmediateTrigger disables automatic TriggerImmediate() calls after Enqueue
// When enabled, state transitions only happen through the scheduled scan interval
func WithDisableImmediateTrigger(disable bool) Option {
	return func(c *Config) error {
		c.DisableImmediateTrigger = disable
		return nil
	}
}

// WithDisableRecovery disables the recovery process during Start()
func WithDisableRecovery(disable bool) Option {
	return func(c *Config) error {
		c.DisableRecovery = disable
		return nil
	}
}

// WithArchiveSuccess enables archiving for successfully processed messages
func WithArchiveSuccess(archive bool) Option {
	return func(c *Config) error {
		c.ArchiveSuccess = archive
		return nil
	}
}

// WithArchiveBounce enables archiving for bounced messages
func WithArchiveBounce(archive bool) Option {
	return func(c *Config) error {
		c.ArchiveBounce = archive
		return nil
	}
}
