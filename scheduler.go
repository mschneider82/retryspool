package retryspool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	metastorage "schneider.vip/retryspool/storage/meta"
)

// workItem represents a scheduled message processing task
type workItem struct {
	MessageID   string
	FromState   QueueState
	ToState     QueueState
	Handler     Handler
	Priority    int
	ScheduledAt time.Time
	Attempt     int
}

// schedulerConfig contains configuration for the message scheduler
type schedulerConfig struct {
	WorkerCount       int           // Number of worker goroutines
	WorkQueueSize     int           // Size of work queue buffer
	ScanInterval      time.Duration // How often to scan for messages
	BatchSize         int           // Max messages to fetch per scan
	MaxActiveMessages int           // Max concurrent active messages
}

// defaultSchedulerConfig returns sensible default configuration
func defaultSchedulerConfig() schedulerConfig {
	return schedulerConfig{
		WorkerCount:       50,                    // More workers for high throughput
		WorkQueueSize:     5000,                  // Larger work queue
		ScanInterval:      10 * time.Millisecond, // Very frequent scans
		BatchSize:         200,                   // Larger batches for better throughput
		MaxActiveMessages: 2000,                  // More concurrent active messages
	}
}

// messageScheduler handles centralized message scheduling to eliminate race conditions
type messageScheduler struct {
	config   schedulerConfig
	storage  *combinedStorage
	queueRef *queueImpl // Reference back to queue for handler access
	logger   Logger     // Logger for debugging and monitoring

	// Channels
	workQueue chan workItem
	stopCh    chan struct{}
	wakeupCh  chan struct{}

	// Trigger state
	needsRescan int32 // atomic flag

	// Worker management
	workers sync.WaitGroup

	// State tracking
	running        bool
	activeCount    int64
	scheduledCount int64
	processedCount int64
	mu             sync.RWMutex

	// Scanning synchronization - prevents concurrent scanning
	scanMu sync.Mutex
}

// newMessageScheduler creates a new centralized message scheduler
func newMessageScheduler(config schedulerConfig, storage *combinedStorage, queue *queueImpl) *messageScheduler {
	return &messageScheduler{
		config:    config,
		storage:   storage,
		queueRef:  queue,
		logger:    queue.config.Logger,
		workQueue: make(chan workItem, config.WorkQueueSize),
		stopCh:    make(chan struct{}),
		wakeupCh:  make(chan struct{}, 1),
	}
}

// Start begins the scheduler and worker processes
func (s *messageScheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("scheduler is already running")
	}

	s.running = true
	s.logger.Info("Starting message scheduler",
		"workers", s.config.WorkerCount,
		"scan_interval", s.config.ScanInterval)

	// Start worker pool
	for i := 0; i < s.config.WorkerCount; i++ {
		s.workers.Add(1)
		go s.workerLoop(ctx, i)
	}

	// Start scheduling loop
	go s.schedulingLoop(ctx)

	return nil
}

func (s *messageScheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}

	s.logger.Info("Stopping message scheduler")
	s.running = false // Set this first to prevent new operations
	s.mu.Unlock()

	// Close stop channel to signal shutdown
	close(s.stopCh)

	// Wait for workers to finish
	s.workers.Wait()

	// Close work queue after all workers have finished
	close(s.workQueue)

	s.logger.Info("Message scheduler stopped",
		"processed_messages", atomic.LoadInt64(&s.processedCount))
	return nil
}

// TriggerImmediate triggers immediate scheduling (called when new messages arrive)
func (s *messageScheduler) TriggerImmediate() {
	s.mu.RLock()
	running := s.running
	s.mu.RUnlock()

	if !running {
		return
	}

	// Set atomic flag that a rescan is needed
	atomic.StoreInt32(&s.needsRescan, 1)

	// Try to wake up the scheduler immediately with multiple attempts for high-load scenarios
	for i := 0; i < 3; i++ {
		select {
		case s.wakeupCh <- struct{}{}:
			// Successfully triggered immediate scan
			return
		default:
			// Channel is full, try again briefly
			if i < 2 {
				continue
			}
			// Final attempt failed, but needsRescan flag ensures we'll rescan
		}
	}
}

// schedulingLoop is the main scheduling loop that scans for work
func (s *messageScheduler) schedulingLoop(ctx context.Context) {
	ticker := time.NewTicker(s.config.ScanInterval)
	defer ticker.Stop()

	// Initial scan
	s.scanAndSchedule(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.scanAndSchedule(ctx)
			// After scheduled scan, check if immediate rescan is needed
			s.checkAndRescan(ctx)
		case <-s.wakeupCh:
			// Immediate scan triggered by new message
			s.scanAndSchedule(ctx)
			// After triggered scan, check if another rescan is needed
			s.checkAndRescan(ctx)
		}
	}
}

// checkAndRescan performs additional scans if needed
func (s *messageScheduler) checkAndRescan(ctx context.Context) {
	// Moderate rescanning - only one additional scan to avoid CPU starvation
	if atomic.SwapInt32(&s.needsRescan, 0) == 1 {
		s.scanAndSchedule(ctx)
	}
}

// scanAndSchedule scans all message states and schedules work items
func (s *messageScheduler) scanAndSchedule(ctx context.Context) {
	// Try to acquire scan lock non-blocking - skip scan if already running
	if !s.scanMu.TryLock() {
		// Another scan is already running, skip this one to avoid blocking
		return
	}
	defer s.scanMu.Unlock()

	// Scan for messages to schedule

	// Continue scanning until no more messages can be scheduled
	totalScheduled := 0

	for {
		// Check available capacity
		currentActive := atomic.LoadInt64(&s.activeCount)
		if currentActive >= int64(s.config.MaxActiveMessages) {
			break // At capacity
		}

		availableSlots := int(int64(s.config.MaxActiveMessages) - currentActive)
		if availableSlots <= 0 {
			break
		}

		// Filesystem-optimized batch sizing - smaller batches, more frequent scans
		maxBatchSize := s.config.BatchSize
		if availableSlots > 100 {
			maxBatchSize = s.config.BatchSize * 2 // 2x only for very high load
		}
		if availableSlots > maxBatchSize {
			availableSlots = maxBatchSize
		}

		scheduledThisRound := 0

		// Priority 1: Ready deferred messages (highest priority)
		if scheduledThisRound < availableSlots {
			deferredScheduled := s.scheduleReadyDeferred(ctx, availableSlots-scheduledThisRound)
			scheduledThisRound += deferredScheduled
			if deferredScheduled > 0 {
				s.logger.Debug("Scheduled deferred messages", "count", deferredScheduled)
			}
		}

		// Priority 2: Incoming messages
		if scheduledThisRound < availableSlots {
			incomingScheduled := s.scheduleIncoming(ctx, availableSlots-scheduledThisRound)
			scheduledThisRound += incomingScheduled
			if incomingScheduled > 0 {
				s.logger.Debug("Scheduled incoming messages", "count", incomingScheduled)
			}
		}

		// Priority 3: Bounce messages (lowest priority)
		if scheduledThisRound < availableSlots {
			bounceScheduled := s.scheduleBounce(ctx, availableSlots-scheduledThisRound)
			scheduledThisRound += bounceScheduled
			if bounceScheduled > 0 {
				s.logger.Debug("Scheduled bounce messages", "count", bounceScheduled)
			}
		}

		totalScheduled += scheduledThisRound

		// If we didn't schedule anything this round, no more messages available
		if scheduledThisRound == 0 {
			break
		}

		// If we didn't fill all available slots, no more messages to process
		if scheduledThisRound < availableSlots {
			break
		}

		// Continue to next round if we scheduled a full batch and might have more
	}

	if totalScheduled > 0 {
		atomic.AddInt64(&s.scheduledCount, int64(totalScheduled))
	}
}

// scheduleReadyDeferred schedules deferred messages that are ready for retry
func (s *messageScheduler) scheduleReadyDeferred(ctx context.Context, maxCount int) int {
	if maxCount <= 0 {
		return 0
	}

	// Use iterator to stream deferred messages
	iterator, err := s.storage.NewMessageIterator(ctx, metastorage.StateDeferred, maxCount)
	if err != nil {
		s.logger.Error("Error creating deferred message iterator", "error", err)
		return 0
	}
	defer iterator.Close()

	now := time.Now()
	scheduled := 0

	for scheduled < maxCount {
		// Check context cancellation at the start of each loop iteration
		select {
		case <-ctx.Done():
			s.logger.Debug("Context cancelled during deferred message scheduling", "scheduled", scheduled)
			return scheduled
		default:
			// Continue with processing
		}

		metadata, _, err := iterator.Next(ctx)
		if err != nil {
			s.logger.Error("Error getting next deferred message", "error", err)
			continue
		}

		// Check if iterator is exhausted (empty message ID indicates end)
		if metadata.ID == "" {
			break
		}

		// Check if ready for retry
		if metadata.NextRetry.After(now) {
			continue // Not ready yet
		}

		// Find handler
		handler := s.findHandler(metadata.Headers, s.queueRef.config.StateHandlers[StateActive])
		if handler == nil {
			s.logger.Warn("No handler found for deferred message", "message_id", metadata.ID)
			continue
		}

		// Atomically move message to active state before scheduling
		err = s.queueRef.MoveToState(ctx, metadata.ID, StateDeferred, StateActive)
		if err != nil {
			if errors.Is(err, metastorage.ErrStateConflict) {
				// Expected: another worker already claimed the message
				s.logger.Debug("Deferred message already claimed by another worker", "message_id", metadata.ID)
			} else {
				s.logger.Error("Failed to move deferred message to active", "message_id", metadata.ID, "error", err)
			}
			continue // Skip this message
		}

		// Schedule work item (message is already in active state)
		wi := workItem{
			MessageID:   metadata.ID,
			FromState:   StateActive, // Already moved
			ToState:     StateActive, // No state change needed
			Handler:     handler,
			Priority:    1, // High priority
			ScheduledAt: time.Now(),
			Attempt:     metadata.Attempts + 1,
		}

		// Check if scheduler is still running before attempting to send
		s.mu.RLock()
		running := s.running
		s.mu.RUnlock()

		if !running {
			// Scheduler stopped, move message back and exit
			if moveErr := s.queueRef.MoveToState(ctx, metadata.ID, StateActive, StateDeferred); moveErr != nil {
				s.logger.Error("Failed to move deferred message back after scheduler stop", "message_id", metadata.ID, "error", moveErr)
			}
			return scheduled
		}

		select {
		case s.workQueue <- wi:
			scheduled++
			atomic.AddInt64(&s.activeCount, 1)
		case <-ctx.Done():
			// Context cancelled while trying to enqueue work item
			// Need to move message back to deferred state since we already moved it to active
			if moveErr := s.queueRef.MoveToState(ctx, metadata.ID, StateActive, StateDeferred); moveErr != nil {
				s.logger.Error("Failed to move message back to deferred after context cancellation", "message_id", metadata.ID, "error", moveErr)
			}
			s.logger.Debug("Context cancelled while enqueueing deferred message", "message_id", metadata.ID, "scheduled", scheduled)
			return scheduled
		default:
			// Work queue full, need to move message back to deferred state
			if moveErr := s.queueRef.MoveToState(ctx, metadata.ID, StateActive, StateDeferred); moveErr != nil {
				s.logger.Error("Failed to move deferred message back after queue full", "message_id", metadata.ID, "error", moveErr)
			}
			// Work queue full, stop scheduling
			return scheduled
		}
	}

	return scheduled
}

// scheduleIncoming schedules incoming messages for processing
func (s *messageScheduler) scheduleIncoming(ctx context.Context, maxCount int) int {
	if maxCount <= 0 {
		return 0
	}

	// Schedule incoming messages for processing

	// Use iterator to stream incoming messages
	iterator, err := s.storage.NewMessageIterator(ctx, metastorage.StateIncoming, maxCount)
	if err != nil {
		s.logger.Error("Error creating incoming message iterator", "error", err)
		return 0
	}
	defer iterator.Close()

	scheduled := 0

	for scheduled < maxCount {
		// Check context cancellation at the start of each loop iteration
		select {
		case <-ctx.Done():
			s.logger.Debug("Context cancelled during incoming message scheduling", "scheduled", scheduled)
			return scheduled
		default:
			// Continue with processing
		}

		metadata, _, err := iterator.Next(ctx)
		if err != nil {
			s.logger.Error("Error getting next incoming message", "error", err)
			continue
		}

		// Check if iterator is exhausted (empty message ID indicates end)
		if metadata.ID == "" {
			break
		}

		// Find handler
		handler := s.findHandler(metadata.Headers, s.queueRef.config.StateHandlers[StateActive])
		if handler == nil {
			s.logger.Warn("No handler found for incoming message", "message_id", metadata.ID)
			continue
		}

		// Atomically move message to active state before scheduling
		// This is a critical section - we need to ensure only one worker processes this message
		err = s.queueRef.MoveToState(ctx, metadata.ID, StateIncoming, StateActive)
		if err != nil {
			if errors.Is(err, metastorage.ErrStateConflict) {
				// Expected: another worker already claimed the message
				if s.logger != nil {
					s.logger.Debug("Incoming message already claimed by another worker", "message_id", metadata.ID)
				}
			} else {
				if s.logger != nil {
					s.logger.Error("Failed to move incoming message to active", "message_id", metadata.ID, "error", err)
				}
			}
			// Don't increment scheduled count for failed moves
			continue // Skip this message
		}

		// Schedule work item (message is already in active state)
		wi := workItem{
			MessageID:   metadata.ID,
			FromState:   StateActive, // Already moved
			ToState:     StateActive, // No state change needed
			Handler:     handler,
			Priority:    2, // Medium priority
			ScheduledAt: time.Now(),
			Attempt:     1, // First attempt
		}

		// Check if scheduler is still running before attempting to send
		s.mu.RLock()
		running := s.running
		s.mu.RUnlock()

		if !running {
			// Scheduler stopped, move message back and exit
			if moveErr := s.queueRef.MoveToState(ctx, metadata.ID, StateActive, StateIncoming); moveErr != nil {
				s.logger.Error("Failed to move incoming message back after scheduler stop", "message_id", metadata.ID, "error", moveErr)
			}
			return scheduled
		}

		select {
		case s.workQueue <- wi:
			scheduled++
			atomic.AddInt64(&s.activeCount, 1)
			if s.logger != nil {
				s.logger.Debug("Scheduled incoming message", "message_id", metadata.ID, "worker_queue_depth", len(s.workQueue))
			}
		case <-ctx.Done():
			// Context cancelled while trying to enqueue work item
			// Need to move message back to incoming state since we already moved it to active
			if moveErr := s.queueRef.MoveToState(ctx, metadata.ID, StateActive, StateIncoming); moveErr != nil {
				s.logger.Error("Failed to move message back to incoming after context cancellation", "message_id", metadata.ID, "error", moveErr)
			}
			s.logger.Debug("Context cancelled while enqueueing incoming message", "message_id", metadata.ID, "scheduled", scheduled)
			return scheduled
		default:
			// Work queue is full - we need to move the message back to incoming state
			// so it can be processed later
			if moveErr := s.queueRef.MoveToState(ctx, metadata.ID, StateActive, StateIncoming); moveErr != nil {
				s.logger.Error("Failed to move incoming message back after queue full", "message_id", metadata.ID, "error", moveErr)
			}
			// Work queue full, stop scheduling
			return scheduled
		}
	}

	return scheduled
}

// scheduleBounce schedules bounce messages for processing
func (s *messageScheduler) scheduleBounce(ctx context.Context, maxCount int) int {
	if maxCount <= 0 {
		return 0
	}

	// Use iterator to stream bounce messages
	iterator, err := s.storage.NewMessageIterator(ctx, metastorage.StateBounce, maxCount)
	if err != nil {
		s.logger.Error("Error creating bounce message iterator", "error", err)
		return 0
	}
	defer iterator.Close()

	scheduled := 0

	for scheduled < maxCount {
		// Check context cancellation at the start of each loop iteration
		select {
		case <-ctx.Done():
			s.logger.Debug("Context cancelled during bounce message scheduling", "scheduled", scheduled)
			return scheduled
		default:
			// Continue with processing
		}

		metadata, _, err := iterator.Next(ctx)
		if err != nil {
			s.logger.Error("Error getting next bounce message", "error", err)
			continue
		}

		// Check if iterator is exhausted (empty message ID indicates end)
		if metadata.ID == "" {
			break
		}

		// Find bounce handler
		handler := s.findHandler(metadata.Headers, s.queueRef.config.StateHandlers[StateBounce])
		if handler == nil {
			s.logger.Warn("No bounce handler found for message", "message_id", metadata.ID)
			continue
		}

		// Atomically move message to active state before scheduling
		err = s.queueRef.MoveToState(ctx, metadata.ID, StateBounce, StateActive)
		if err != nil {
			if errors.Is(err, metastorage.ErrStateConflict) {
				// Expected: another worker already claimed the message
				s.logger.Debug("Bounce message already claimed by another worker", "message_id", metadata.ID)
			} else {
				s.logger.Error("Failed to move bounce message to active", "message_id", metadata.ID, "error", err)
			}
			continue // Skip this message
		}

		// Schedule work item (message is already in active state)
		wi := workItem{
			MessageID:   metadata.ID,
			FromState:   StateActive, // Already moved
			ToState:     StateActive, // No state change needed
			Handler:     handler,
			Priority:    3, // Low priority
			ScheduledAt: time.Now(),
			Attempt:     metadata.Attempts,
		}

		// Check if scheduler is still running before attempting to send
		s.mu.RLock()
		running := s.running
		s.mu.RUnlock()

		if !running {
			// Scheduler stopped, move message back and exit
			if moveErr := s.queueRef.MoveToState(ctx, metadata.ID, StateActive, StateBounce); moveErr != nil {
				s.logger.Error("Failed to move bounce message back after scheduler stop", "message_id", metadata.ID, "error", moveErr)
			}
			return scheduled
		}

		select {
		case s.workQueue <- wi:
			scheduled++
			atomic.AddInt64(&s.activeCount, 1)
		case <-ctx.Done():
			// Context cancelled while trying to enqueue work item
			// Need to move message back to bounce state since we already moved it to active
			if moveErr := s.queueRef.MoveToState(ctx, metadata.ID, StateActive, StateBounce); moveErr != nil {
				s.logger.Error("Failed to move message back to bounce after context cancellation", "message_id", metadata.ID, "error", moveErr)
			}
			s.logger.Debug("Context cancelled while enqueueing bounce message", "message_id", metadata.ID, "scheduled", scheduled)
			return scheduled
		default:
			// Work queue full, need to move message back to bounce state
			if moveErr := s.queueRef.MoveToState(ctx, metadata.ID, StateActive, StateBounce); moveErr != nil {
				s.logger.Error("Failed to move bounce message back after queue full", "message_id", metadata.ID, "error", moveErr)
			}
			// Work queue full, stop scheduling
			return scheduled
		}
	}

	return scheduled
}

// workerLoop processes work items from the work queue
func (s *messageScheduler) workerLoop(ctx context.Context, workerID int) {
	defer s.workers.Done()

	s.logger.Debug("Worker started", "worker_id", workerID)
	defer s.logger.Debug("Worker stopped", "worker_id", workerID)

	for {
		select {
		case wi, ok := <-s.workQueue:
			if !ok {
				// Work queue closed, worker should exit
				return
			}

			// Process the work item
			s.processWorkItem(ctx, wi, workerID)

		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		}
	}
}

// processWorkItem processes a single work item
func (s *messageScheduler) processWorkItem(ctx context.Context, item workItem, workerID int) {
	defer atomic.AddInt64(&s.processedCount, 1)
	// Note: activeCount is decremented when message processing completes (success or failure)

	// State transition is already handled in the scheduling phase
	// Only do state transition if needed (fromState != toState)
	if item.FromState != item.ToState {
		err := s.queueRef.MoveToState(ctx, item.MessageID, item.FromState, item.ToState)
		if err != nil {
			s.logger.Error("Worker failed to move message",
				"worker_id", workerID,
				"message_id", item.MessageID,
				"from_state", item.FromState,
				"to_state", item.ToState,
				"error", err)
			return
		}
	}

	// Process the message with the handler
	err := s.queueRef.ProcessMessage(ctx, item.MessageID, item.Handler)

	// Always decrement active count after processing (success or failure)
	atomic.AddInt64(&s.activeCount, -1)

	if err != nil {
		s.logger.Error("Worker error processing message",
			"worker_id", workerID,
			"message_id", item.MessageID,
			"error", err)
	}
}

// findHandler finds the appropriate handler for a message
func (s *messageScheduler) findHandler(headers map[string]string, handlers []Handler) Handler {
	for _, handler := range handlers {
		if handler.CanHandle(headers) {
			return handler
		}
	}
	return nil
}

// GetMetrics returns current scheduler metrics
func (s *messageScheduler) GetMetrics() schedulerMetrics {
	return schedulerMetrics{
		ActiveMessages: atomic.LoadInt64(&s.activeCount),
		ScheduledTotal: atomic.LoadInt64(&s.scheduledCount),
		ProcessedTotal: atomic.LoadInt64(&s.processedCount),
		WorkQueueDepth: len(s.workQueue),
		WorkerCount:    s.config.WorkerCount,
		IsRunning:      s.running,
	}
}

// schedulerMetrics contains scheduler performance metrics
type schedulerMetrics struct {
	ActiveMessages int64
	ScheduledTotal int64
	ProcessedTotal int64
	WorkQueueDepth int
	WorkerCount    int
	IsRunning      bool
}
