# RetrySpool

RetrySpool is a high-performance, pluggable message queue system with retry capabilities, inspired by Postfix's queue management architecture. It provides reliable message processing with configurable storage backends, middleware support, and comprehensive retry policies.

## Overview

RetrySpool implements a multi-state queue system where messages flow through different states based on processing outcomes. The system supports pluggable storage backends for both message data and metadata, allowing for flexible deployment scenarios from simple filesystem-based setups to distributed systems using NATS or PostgreSQL.

## Architecture

### Queue States

Messages in RetrySpool flow through six distinct states:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  INCOMING   │───▶│   ACTIVE    │───▶│  ARCHIVED   │
│   (new)     │    │(processing) │    │ (success)   │
└─────────────┘    └─────────────┘    └─────────────┘
                           │                 ▲
                           ▼                 │
                   ┌─────────────┐           │
                   │  DEFERRED   │───────────┤
                   │ (retry wait)│           │
                   └─────────────┘           │
                           │                 │
                           ▼                 │
                   ┌─────────────┐    ┌─────────────┐
                   │    HOLD     │    │   BOUNCE    │
                   │ (admin hold)│    │(perm. fail) │
                   └─────────────┘    └─────────────┘
```

**State Descriptions:**
- **INCOMING**: New messages awaiting processing
- **ACTIVE**: Messages currently being processed by handlers
- **DEFERRED**: Messages that failed temporarily and are waiting for retry
- **HOLD**: Messages manually paused by administrator intervention
- **BOUNCE**: Messages that failed permanently. Handlers can process these to generate DSNs or notifications.
- **ARCHIVED**: Final state for completed messages (both successful and failed) if archiving is enabled.

### Message Archiving

By default, RetrySpool deletes messages after successful processing and leaves permanent failures in the `BOUNCE` state. You can enable archiving to keep messages for audit or manual re-processing:

- **Archive Success**: Moves successfully processed messages to `StateArchived` instead of deleting them.
- **Archive Bounce**: Moves permanently failed messages to `StateArchived` after the bounce handlers have finished.

```go
q := retryspool.New(
    // ... storage and handlers ...
    retryspool.WithArchiveSuccess(true),
    retryspool.WithArchiveBounce(true),
)
```

Archived messages can be moved back to `INCOMING` via the API or CLI to trigger a new delivery attempt.

### Scheduler Flow

The message scheduler operates with the following priority system:

```
┌─────────────────────────────────────────────────────────────┐
│                    Message Scheduler                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐  Priority 1   ┌─────────────────────────┐  │
│  │  DEFERRED   │──────────────▶│     Ready for Retry     │  │
│  │ (NextRetry  │               │    (time.Now() >=      │  │
│  │  <= now)    │               │     NextRetry)          │  │
│  └─────────────┘               └─────────────────────────┘  │
│                                                             │
│  ┌─────────────┐  Priority 2   ┌─────────────────────────┐  │
│  │  INCOMING   │──────────────▶│    New Messages         │  │
│  │             │               │                         │  │
│  └─────────────┘               └─────────────────────────┘  │
│                                                             │
│  ┌─────────────┐  Priority 3   ┌─────────────────────────┐  │
│  │   BOUNCE    │──────────────▶│   Handler Processing    │  │
│  │             │               │                         │  │
│  └─────────────┘               └─────────────────────────┘  │
│                                                             │
│                                 ▼                           │
│                    ┌─────────────────────────┐              │
│                    │      Worker Pool        │              │
│                    │   (Configurable Size)   │              │
│                    └─────────────────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

### Retry Policy

RetrySpool implements exponential backoff retry intervals by default:

```
Attempt:  1     2     3    4    5    6     7     8+
Interval: 15m   30m   1h   2h   4h   8h    16h   24h
```

#### Per-Message Retry Policies

You can define multiple named retry policies and select them for specific messages. This is useful for handling different categories of messages (e.g., critical vs. bulk) within the same queue.

**1. Register Named Policies:**

```go
q := retryspool.New(
    retryspool.WithRetryPolicy(defaultPolicy), // Global default
    retryspool.WithNamedRetryPolicy("fast", fastPolicy),
    retryspool.WithNamedRetryPolicy("slow", slowPolicy),
)
```

**2. Select Policy during Enqueue:**

There are two ways to specify which policy should be used for a message:

**Via Context (Recommended):**

```go
ctx := retryspool.ContextWithRetryPolicy(context.Background(), "fast")
msgID, err := q.Enqueue(ctx, nil, data)
```

**Via Header:**

```go
headers := map[string]string{
    "x-retry-policy": "slow",
}
msgID, err := q.Enqueue(context.Background(), headers, data)
```

**3. Change Policy After Enqueue:**

You can change the retry policy of an existing message at any time:

```go
err := q.SetMessageRetryPolicy(ctx, msgID, "slow")
```

If no policy is specified, or the named policy is not found, the global default retry policy is used.

## Core Module

**Module Path:** `schneider.vip/retryspool`

### Key Interfaces

```go
// Queue represents the main message queue interface
type Queue interface {
    Enqueue(ctx context.Context, headers map[string]string, data io.Reader) (string, error)
    GetMessage(ctx context.Context, id string) (Message, MessageReader, error)
    ProcessMessage(ctx context.Context, id string, handler Handler) error
    MoveToState(ctx context.Context, id string, fromState, toState QueueState) error
    // ... additional methods
}

// Handler processes messages
type Handler interface {
    Handle(ctx context.Context, message Message, data MessageReader) error
    Name() string
    CanHandle(headers map[string]string) bool
}

// RetryPolicy defines retry behavior
type RetryPolicy interface {
    NextRetry(attempts int, lastError error) time.Time
    ShouldRetry(attempts int, maxAttempts int, lastError error) bool
}
```

## Storage Backends

RetrySpool separates message data and metadata storage, allowing for optimal backend selection based on requirements.

### Data Storage Backends

**Base Interface:** `schneider.vip/retryspool/storage/data`

#### Filesystem
**Module Path:** `schneider.vip/retryspool/storage/data/filesystem`
- Stores message content as individual files
- Suitable for single-node deployments
- High performance for large message payloads

#### NATS JetStream
**Module Path:** `schneider.vip/retryspool/storage/data/nats`
- Uses NATS JetStream Object Store
- Distributed storage with replication
- Suitable for multi-node deployments

#### S3 Compatible
**Module Path:** `schneider.vip/retryspool/storage/data/s3`
- Uses S3-compatible object storage (AWS S3, MinIO, etc.)
- Highly scalable and durable storage for large payloads
- Supports multi-part uploads for efficient streaming

#### PostgreSQL
**Module Path:** `schneider.vip/retryspool/storage/data/postgres`
- Stores message data in PostgreSQL BYTEA columns
- ACID compliance and transactional integrity
- Suitable for enterprise deployments

### Metadata Storage Backends

**Base Interface:** `schneider.vip/retryspool/storage/meta`

#### Filesystem
**Module Path:** `schneider.vip/retryspool/storage/meta/filesystem`
- Stores metadata as JSON files organized by state
- Simple deployment with no external dependencies
- Atomic state transitions via filesystem operations

#### SQLite
**Module Path:** `schneider.vip/retryspool/storage/meta/sqlite`
- Embedded SQL database for metadata
- ACID transactions and SQL query capabilities
- Suitable for single-node deployments

#### NATS JetStream
**Module Path:** `schneider.vip/retryspool/storage/meta/nats`
- Uses NATS JetStream Key-Value store
- Distributed metadata with atomic operations
- Suitable for multi-node deployments

#### PostgreSQL
**Module Path:** `schneider.vip/retryspool/storage/meta/postgres`
- Full SQL database for metadata
- Advanced querying and reporting capabilities
- Suitable for enterprise deployments

## Middleware

**Base Interface:** `schneider.vip/retryspool/middleware`

Middleware provides transparent data transformation during storage operations.

### Compression Middleware
**Module Path:** `schneider.vip/retryspool/middleware/compression`

Supported algorithms:
- **Gzip**: Standard gzip compression using klauspost/compress (faster than stdlib)
- **Zstd**: Excellent compression ratio and speed
- **S2**: Very fast compression/decompression
- **Snappy**: Very fast with moderate compression
- **Zlib**: Standard zlib compression
- **Flate**: Raw deflate compression

Compression levels:
- **Fastest**: Least CPU usage
- **Default**: Balanced compression
- **Better**: More CPU for better compression
- **Best**: Maximum compression with most CPU usage

Example usage:
```go
// Gzip with default level
gzipMiddleware := compression.New(compression.Gzip)

// Zstd with best compression
zstdMiddleware := compression.New(compression.Zstd, compression.WithLevel(compression.Best))

// S2 with fastest compression
s2Middleware := compression.New(compression.S2, compression.WithLevel(compression.Fastest))
```

### Encryption Middleware
**Module Path:** `schneider.vip/retryspool/middleware/encryption`

Supported ciphers:
- **AES256GCM**: AES-256-GCM authenticated encryption (default)
- **ChaCha20Poly1305**: ChaCha20-Poly1305 authenticated encryption

Example usage:
```go
// Generate a key
key, err := encryption.GenerateKey()
if err != nil {
    log.Fatal(err)
}

// AES-256-GCM encryption (default)
aesMiddleware := encryption.New(encryption.WithKey(key))

// ChaCha20-Poly1305 encryption
chachaMiddleware := encryption.New(
    encryption.WithKey(key),
    encryption.WithCipher(encryption.ChaCha20Poly1305),
)
```

## CLI Administration Tool

**Module Path:** `schneider.vip/retryspool/cli`

The RetrySpool CLI provides an interactive terminal interface for queue administration using Bubble Tea.

### Features
- Interactive message browsing across all queue states
- Bulk operations for message management
- Real-time queue statistics and monitoring
- Configuration management for storage backends
- Search and filtering capabilities

### Installation
```bash
cd retryspool-cli
go build -o retryspool-cli
```

### Usage
```bash
./retryspool-cli
```

The CLI supports:
- Message state transitions (move between incoming, active, deferred, hold, bounce)
- Bulk message operations (delete, move, hold)
- Real-time queue monitoring
- Configuration of storage backends and middleware
- Search and filtering of messages

## Configuration Example

```go
package main

import (
    "context"
    "fmt"
    "io"
    "log"
    "time"
    
    "schneider.vip/retryspool"
    datafs "schneider.vip/retryspool/storage/data/filesystem"
    datanats "schneider.vip/retryspool/storage/data/nats"
    datapostgres "schneider.vip/retryspool/storage/data/postgres"
    datas3 "schneider.vip/retryspool/storage/data/s3"
    metafs "schneider.vip/retryspool/storage/meta/filesystem"
    metanats "schneider.vip/retryspool/storage/meta/nats"
    metapostgres "schneider.vip/retryspool/storage/meta/postgres"
    metasqlite "schneider.vip/retryspool/storage/meta/sqlite"
    "schneider.vip/retryspool/middleware/compression"
    "schneider.vip/retryspool/middleware/encryption"
)

func main() {
    // Example 1: Simple filesystem setup
    queue1 := retryspool.New(
        retryspool.WithDataStorage(datafs.NewFactory("./data")),
        retryspool.WithMetaStorage(metafs.NewFactory("./meta")),
    )
    
    // Example 2: NATS with middleware
    dataFactory := datanats.NewFactory(
        datanats.WithURL("nats://localhost:4222"),
        datanats.WithBucket("retryspool-data"),
        datanats.WithReplicas(3),
    )
    
    metaFactory := metanats.NewFactory(
        metanats.WithURL("nats://localhost:4222"),
        metanats.WithBucket("retryspool-meta"),
        metanats.WithReplicas(3),
    )
    
    // Create middleware
    compMiddleware := compression.New(compression.Gzip, compression.WithLevel(compression.Default))
    encMiddleware := encryption.New(
        encryption.WithKey([]byte("your-32-byte-key-here-for-aes256")),
        encryption.WithCipher(encryption.AES256GCM),
    )
    
    // Create queue with configuration
    queue2 := retryspool.New(
        retryspool.WithDataStorage(dataFactory, compMiddleware, encMiddleware),
        retryspool.WithMetaStorage(metaFactory),
        retryspool.WithRetryPolicy(retryspool.DefaultExponentialRetryPolicy()),
        retryspool.WithMaxAttempts(10),
        retryspool.WithMaxConcurrency(50),
    )
    
    // Example 3: PostgreSQL setup
    dataPostgres := datapostgres.NewFactory("postgres://user:pass@localhost/retryspool").
        WithTableName("message_data").
        WithConnectionLimits(50, 10)
    
    metaPostgres := metapostgres.NewFactory("postgres://user:pass@localhost/retryspool").
        WithTableName("message_metadata").
        WithConnectionLimits(50, 10)
    
    queue3 := retryspool.New(
        retryspool.WithDataStorage(dataPostgres),
        retryspool.WithMetaStorage(metaPostgres),
    )
    
    // Example 4: SQLite metadata with filesystem data
    metaSQLite := metasqlite.NewFactory(
        metasqlite.WithDatabasePath("./retryspool.db"),
        metasqlite.WithWALMode(true),
        metasqlite.WithMaxConnections(10),
    )
    
    queue4 := retryspool.New(
        retryspool.WithDataStorage(datafs.NewFactory("./data")),
        retryspool.WithMetaStorage(metaSQLite),
    )
    
    // Example 5: S3 Data Storage with NATS Metadata
    s3Factory := datas3.NewFactory("my-bucket").
        WithBackendOptions(datas3.WithKeyPrefix("myapp/spool"))
    
    queue5 := retryspool.New(
        retryspool.WithDataStorage(s3Factory),
        retryspool.WithMetaStorage(metanats.NewFactory(metanats.WithBucket("meta"))),
    )
    
    // Example 6: Queue with active and bounce handlers
    emailHandler := &EmailHandler{}
    webhookBounceHandler := &WebhookBounceHandler{}
    
    queue6 := retryspool.New(
        retryspool.WithDataStorage(datafs.NewFactory("./data")),
        retryspool.WithMetaStorage(metafs.NewFactory("./meta")),
        retryspool.WithActiveHandler(emailHandler),
        retryspool.WithBounceHandler(webhookBounceHandler),
        retryspool.WithMaxAttempts(5),
        retryspool.WithMaxConcurrency(20),
    )
    
    // Start queue processing
    ctx := context.Background()
    if err := queue6.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer queue6.Close()
}

// EmailHandler processes active messages by sending emails
type EmailHandler struct{}

func (h *EmailHandler) Name() string {
    return "email-handler"
}

func (h *EmailHandler) CanHandle(headers map[string]string) bool {
    return true
}

func (h *EmailHandler) Handle(ctx context.Context, message retryspool.Message, data retryspool.MessageReader) error {
    defer data.Close()
    
    // Read email content
    content, err := io.ReadAll(data)
    if err != nil {
        return retryspool.NewRetryableError(fmt.Errorf("failed to read email content: %w", err))
    }
    
    // Send email
    if err := sendEmail(message.Metadata.Headers["to"], string(content)); err != nil {
        // Check if it's a permanent failure (e.g., invalid email address)
        if isInvalidEmail(err) {
            return retryspool.NewPermanentError(err)
        }
        // Temporary failure - will be retried
        return retryspool.NewRetryableError(err)
    }
    
    log.Printf("Email sent successfully to %s", message.Metadata.Headers["to"])
    return nil
}

### Context-based Header Backchannel

Handlers can set or overwrite any message headers during processing without changing the handler interface. This is especially useful for capturing delivery details (e.g., remote MTA responses, queue IDs) for archiving.

**Usage in Handler:**

```go
func (h *MyHandler) Handle(ctx context.Context, message retryspool.Message, data retryspool.MessageReader) error {
    // Set a single header
    retryspool.SetContextHeader(ctx, "x-remote-server", "mx1.example.com")

    // Set multiple headers at once
    retryspool.SetContextHeaders(ctx, map[string]string{
        "x-remote-response": "250 OK: queued as 12345",
        "x-remote-queue-id": "12345",
    })

    return nil
}
```

The set headers are automatically merged into the message metadata by the queue – both in case of success and errors (e.g., to log the last attempted server).

### Message ID in Context

RetrySpool automatically injects the message ID into the context when processing a message. You can retrieve it in your handlers for logging or tracing:

```go
func (h *MyHandler) Handle(ctx context.Context, message retryspool.Message, data retryspool.MessageReader) error {
    id := retryspool.MessageIDFromContext(ctx)
    log.Printf("[%s] Processing message...", id)
    // ...
    return nil
}
```

// WebhookBounceHandler processes bounced messages by sending webhook notifications
type WebhookBounceHandler struct{}

func (h *WebhookBounceHandler) Name() string {
    return "webhook-bounce-handler"
}

func (h *WebhookBounceHandler) CanHandle(headers map[string]string) bool {
    return true
}

func (h *WebhookBounceHandler) Handle(ctx context.Context, message retryspool.Message, data retryspool.MessageReader) error {
    defer data.Close()
    
    // Create bounce notification payload
    bounceData := map[string]interface{}{
        "message_id": message.ID,
        "recipient":  message.Metadata.Headers["to"],
        "attempts":   message.Metadata.Attempts,
        "error":      message.Metadata.LastError,
        "bounced_at": time.Now().Format(time.RFC3339),
    }
    
    // Send webhook notification
    webhookURL := message.Metadata.Headers["bounce_webhook"]
    if webhookURL == "" {
        webhookURL = "https://api.example.com/bounces" // Default webhook URL
    }
    
    if err := sendWebhook(webhookURL, bounceData); err != nil {
        log.Printf("Failed to send bounce webhook: %v", err)
        // Don't return error - we don't want to retry bounce processing
    }
    
    log.Printf("Bounce notification sent for message %s to %s", message.ID, webhookURL)
    return nil
}

// Helper functions (implementation details omitted for brevity)
func sendEmail(to, content string) error { /* implementation */ return nil }
func sendWebhook(url string, data map[string]interface{}) error { /* implementation */ return nil }
func isInvalidEmail(err error) bool { /* implementation */ return false }
}
```

## Storage Backend Factory APIs

### Data Storage Factories

```go
// Filesystem
datafs.NewFactory(basePath string)

// NATS JetStream Object Store
datanats.NewFactory(opts ...datanats.Option)
// Options: WithURL, WithBucket, WithConnection, WithMaxChunkSize, WithReplicas, etc.

// S3 Compatible
datas3.NewFactory(bucket string)
// Options: WithClient, WithClientOptions, WithBackendOptions (Prefix, StorageClass)

// PostgreSQL
datapostgres.NewFactory(dsn string).
    WithTableName(tableName string).
    WithConnectionLimits(maxOpen, maxIdle int)
```

### Metadata Storage Factories

```go
// Filesystem
metafs.NewFactory(basePath string).
    WithDisableSync(disable bool).
    WithBatchSync(enable bool)

// SQLite
metasqlite.NewFactory(opts ...metasqlite.Option)
// Options: WithDatabasePath, WithWALMode, WithBusyTimeout, WithCacheSize, etc.

// NATS JetStream Key-Value
metanats.NewFactory(opts ...metanats.Option)
// Options: WithURL, WithBucket, WithConnection, WithMaxValueSize, WithReplicas, etc.

// PostgreSQL
metapostgres.NewFactory(dsn string).
    WithTableName(tableName string).
    WithConnectionLimits(maxOpen, maxIdle int)
```

## Default Configuration

- **Retry Policy**: Exponential backoff (15min to 24h intervals)
- **Max Attempts**: 10
- **Max Concurrency**: 10 workers
- **Storage**: Filesystem (./retryspool/data and ./retryspool/meta)
- **Scheduler**: 50 workers, 10ms scan interval, 200 message batches

## Error Handling

RetrySpool provides structured error types for different failure scenarios:

- **RetryableError**: Temporary failures that should be retried
- **PermanentError**: Permanent failures that move messages to bounce state
- **ErrMessageNotFound**: Message does not exist
- **ErrInvalidState**: Invalid state transition attempted
- **ErrMaxAttemptsExceeded**: Message exceeded retry limit

## Performance Characteristics

- **High Throughput**: Optimized scheduler with configurable worker pools
- **Low Latency**: 10ms default scan intervals for immediate processing
- **Scalable**: Pluggable backends support horizontal scaling
- **Reliable**: Atomic state transitions and crash recovery
- **Efficient**: Middleware pipeline for data transformation

## Performance Benchmarks

Comprehensive performance tests measuring both enqueue rate and end-to-end throughput:

### Single Worker Performance (Concurrency = 1, 30s enqueing / 60s processing tests)

| Data Storage | Meta Storage | Enqueue Rate/s | End-to-End Throughput/s |
|--------------|--------------|----------------|--------------------------|
| Filesystem   | Filesystem   | 86.9           | 77.8                   |
| Filesystem   | Sqlite       | 99.3           | 99.0                   |
| Filesystem   | Nats         | 693.4          | 151.8                  |
| Filesystem   | Postgres     | 466.1          | 453.9                  |
| Postgres     | Postgres     | 239.2          | 238.4                  |
| Postgres     | Filesystem   | 27.7           | 27.6                   |
| Postgres     | Nats         | 118.7          | 78.7                   |
| Postgres     | Sqlite       | 44.6           | 44.5                   |
| Nats         | Filesystem   | 35.3           | 35.2                   |
| Nats         | Nats         | 250.3          | 86.4                   |
| Nats         | Postgres     | 376.1          | 216.3                  |
| Nats         | Sqlite       | 100.4          | 100.1                  |

### High Concurrency Performance (Concurrency = 5, 30s enqueing / 60s processing tests)

| Data Storage | Meta Storage | Enqueue Rate/s | End-to-End Throughput/s |
|--------------|--------------|----------------|--------------------------|
| Filesystem   | Filesystem   | 133.5          | 76.8                    |
| Filesystem   | Sqlite       | 187.1          | 73.2                    |
| Filesystem   | Nats         | 1166.2         | 199.5                   |
| Filesystem   | Postgres     | 2256.3         | 425.1                   |
| Postgres     | Postgres     | 1143.5         | 444.6                   |
| Postgres     | Filesystem   | 48.8           | 28.4                    |
| Postgres     | Nats         | 510.3          | 121.2                   |
| Postgres     | Sqlite       | 163.4          | 68.7                    |
| Nats         | Nats         | 340.0          | 89.3                    |
| Nats         | Filesystem   | 84.8           | 40.4                    |
| Nats         | Postgres     | 523.1          | 293.6                   |
| Nats         | Sqlite       | 189.8          | 45.9                    |


### Running Benchmarks

```bash
cd retryspool/benchmarks
go test -bench=. -benchmem -benchtime=5s
```

## License

This project is licensed under the terms specified in the main project license.