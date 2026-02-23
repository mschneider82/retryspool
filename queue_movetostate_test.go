package retryspool

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	datafs "schneider.vip/retryspool/storage/data/filesystem"
	metafs "schneider.vip/retryspool/storage/meta/filesystem"
)

func TestQueueMoveToState_CAS(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "retryspool-integration-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dataFactory := datafs.NewFactory(filepath.Join(tempDir, "data"))
	metaFactory := metafs.NewFactory(filepath.Join(tempDir, "meta"))

	q := New(
		WithDataStorage(dataFactory),
		WithMetaStorage(metaFactory),
	)
	defer q.Close()

	ctx := context.Background()
	messageID, err := q.Enqueue(ctx, nil, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatal(err)
	}

	const workerCount = 50
	var wg sync.WaitGroup
	wg.Add(workerCount)

	successCount := 0
	conflictCount := 0
	var mu sync.Mutex

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			err := q.MoveToState(ctx, messageID, StateIncoming, StateActive)
			mu.Lock()
			if err == nil {
				successCount++
			} else {
				// We expect ErrStateConflict wrapped in fmt.Errorf
				conflictCount++
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if successCount != 1 {
		t.Errorf("Expected exactly 1 success, got %d", successCount)
	}
}
