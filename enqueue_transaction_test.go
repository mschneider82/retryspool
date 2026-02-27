package retryspool_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"schneider.vip/retryspool"
	datafs "schneider.vip/retryspool/storage/data/filesystem"
	metafs "schneider.vip/retryspool/storage/meta/filesystem"
)

func TestEnqueueTransaction_Success(t *testing.T) {
	tempDir := t.TempDir()

	dataFactory := datafs.NewFactory(tempDir + "/data")
	metaFactory := metafs.NewFactory(tempDir + "/meta")

	q := retryspool.New(
		retryspool.WithDataStorage(dataFactory),
		retryspool.WithMetaStorage(metaFactory),
	)
	defer q.Close()

	ctx := context.Background()
	tx, err := q.BeginEnqueue(ctx)
	if err != nil {
		t.Fatalf("Failed to begin enqueue: %v", err)
	}

	// Message ID should be available immediately
	if tx.MessageID() == "" {
		t.Error("Expected message ID to be set")
	}

	// Set some headers
	tx.SetHeader("X-Test", "value1")
	tx.SetHeaders(map[string]string{
		"X-Custom": "value2",
		"priority": "3",
	})

	// Stream some data
	_, err = tx.Write([]byte("part 1, "))
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Set more headers after writing data
	tx.SetHeader("X-Late", "value3")

	_, err = tx.Write([]byte("part 2"))
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Commit the transaction
	msgID, err := tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if msgID != tx.MessageID() {
		t.Errorf("Commit returned message ID %s, expected %s", msgID, tx.MessageID())
	}

	// Verify the message exists and has correct data/metadata
	msg, reader, err := q.GetMessage(ctx, msgID)
	if err != nil {
		t.Fatalf("Failed to get message: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read message data: %v", err)
	}

	if string(data) != "part 1, part 2" {
		t.Errorf("Expected 'part 1, part 2', got '%s'", string(data))
	}

	expectedHeaders := map[string]string{
		"X-Test":   "value1",
		"X-Custom": "value2",
		"priority": "3",
		"X-Late":   "value3",
	}

	for k, v := range expectedHeaders {
		if msg.Metadata.Headers[k] != v {
			t.Errorf("Expected header %s=%s, got %s", k, v, msg.Metadata.Headers[k])
		}
	}

	if msg.Metadata.Priority != 3 {
		t.Errorf("Expected priority 3, got %d", msg.Metadata.Priority)
	}

	if msg.Metadata.Size != int64(len("part 1, part 2")) {
		t.Errorf("Expected size %d, got %d", len("part 1, part 2"), msg.Metadata.Size)
	}
}

func TestEnqueueTransaction_Abort(t *testing.T) {
	tempDir := t.TempDir()

	dataFactory := datafs.NewFactory(tempDir + "/data")
	metaFactory := metafs.NewFactory(tempDir + "/meta")

	q := retryspool.New(
		retryspool.WithDataStorage(dataFactory),
		retryspool.WithMetaStorage(metaFactory),
	)
	defer q.Close()

	ctx := context.Background()
	tx, err := q.BeginEnqueue(ctx)
	if err != nil {
		t.Fatalf("Failed to begin enqueue: %v", err)
	}

	msgID := tx.MessageID()

	tx.Write([]byte("some data"))
	tx.SetHeader("X-Test", "value")

	// Abort the transaction
	err = tx.Abort()
	if err != nil {
		t.Fatalf("Failed to abort: %v", err)
	}

	// Verify message does not exist
	_, _, err = q.GetMessage(ctx, msgID)
	if err == nil {
		t.Error("Expected error when getting aborted message, got nil")
	}

	// Second abort should be no-op
	err = tx.Abort()
	if err != nil {
		t.Errorf("Second abort returned error: %v", err)
	}

	// Write after abort should fail
	_, err = tx.Write([]byte("more data"))
	if err == nil {
		t.Error("Expected error when writing after abort, got nil")
	}

	// Commit after abort should fail
	_, err = tx.Commit()
	if err == nil {
		t.Error("Expected error when committing after abort, got nil")
	}
}

func TestEnqueueTransaction_AbortNoData(t *testing.T) {
	tempDir := t.TempDir()

	dataFactory := datafs.NewFactory(tempDir + "/data")
	metaFactory := metafs.NewFactory(tempDir + "/meta")

	q := retryspool.New(
		retryspool.WithDataStorage(dataFactory),
		retryspool.WithMetaStorage(metaFactory),
	)
	defer q.Close()

	ctx := context.Background()
	tx, err := q.BeginEnqueue(ctx)
	if err != nil {
		t.Fatalf("Failed to begin enqueue: %v", err)
	}

	// Abort the transaction immediately without any write
	err = tx.Abort()
	if err != nil {
		t.Fatalf("Failed to abort transaction with no data: %v", err)
	}
}

func TestEnqueueTransaction_CommitTwice(t *testing.T) {
	tempDir := t.TempDir()

	dataFactory := datafs.NewFactory(tempDir + "/data")
	metaFactory := metafs.NewFactory(tempDir + "/meta")

	q := retryspool.New(
		retryspool.WithDataStorage(dataFactory),
		retryspool.WithMetaStorage(metaFactory),
	)
	defer q.Close()

	ctx := context.Background()
	tx, err := q.BeginEnqueue(ctx)
	if err != nil {
		t.Fatalf("Failed to begin enqueue: %v", err)
	}

	tx.Write([]byte("data"))

	msgID1, err := tx.Commit()
	if err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Second commit should return same ID and no error (idempotent for simplicity or just return same ID)
	msgID2, err := tx.Commit()
	if err != nil {
		t.Fatalf("Second commit failed: %v", err)
	}

	if msgID1 != msgID2 {
		t.Errorf("Expected same message ID, got %s and %s", msgID1, msgID2)
	}

	// Write after commit should fail
	_, err = tx.Write([]byte("more data"))
	if err == nil {
		t.Error("Expected error when writing after commit, got nil")
	}
}

func TestEnqueueTransaction_Middleware(t *testing.T) {
	tempDir := t.TempDir()

	dataFactory := datafs.NewFactory(tempDir + "/data")
	metaFactory := metafs.NewFactory(tempDir + "/meta")

	// Simple mock middleware that reverses the bytes
	mw := &mockMiddleware{}

	q := retryspool.New(
		retryspool.WithDataStorage(dataFactory, mw),
		retryspool.WithMetaStorage(metaFactory),
	)
	defer q.Close()

	ctx := context.Background()
	tx, err := q.BeginEnqueue(ctx)
	if err != nil {
		t.Fatalf("Failed to begin enqueue: %v", err)
	}

	tx.Write([]byte("hello"))
	msgID, err := tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify the message data was processed by middleware
	_, reader, err := q.GetMessage(ctx, msgID)
	if err != nil {
		t.Fatalf("Failed to get message: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read message data: %v", err)
	}

	if string(data) != "hello" {
		t.Errorf("Expected 'hello' (decrypted by middleware), got '%s'", string(data))
	}

	// Size should be the UNCOMPRESSED size (what was written to the transaction)
	// even if middleware transforms it.
	meta, err := q.GetMessageMetadata(ctx, msgID)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	if meta.Size != int64(len("hello")) {
		t.Errorf("Expected size %d, got %d", len("hello"), meta.Size)
	}
}

func TestEnqueueTransaction_Middleware_StorageInspection(t *testing.T) {
	tempDir := t.TempDir()

	dataDir := tempDir + "/data"
	dataFactory := datafs.NewFactory(dataDir)
	metaFactory := metafs.NewFactory(tempDir + "/meta")

	// Simple mock middleware that reverses the bytes
	mw := &mockMiddleware{}

	q := retryspool.New(
		retryspool.WithDataStorage(dataFactory, mw),
		retryspool.WithMetaStorage(metaFactory),
	)
	defer q.Close()

	ctx := context.Background()
	tx, err := q.BeginEnqueue(ctx)
	if err != nil {
		t.Fatalf("Failed to begin enqueue: %v", err)
	}

	msgID := tx.MessageID()
	payload := "hello"
	tx.Write([]byte(payload))
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Now inspect the storage directly to see if data is reversed
	// We need to find the file path used by the filesystem backend
	// Sharding is msgID[:2] if len >= 2, else "misc"
	var dataPath string
	if len(msgID) >= 2 {
		dataPath = filepath.Join(dataDir, msgID[:2], msgID+".data")
	} else {
		dataPath = filepath.Join(dataDir, "misc", msgID+".data")
	}

	rawContent, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("Failed to read raw storage file: %v", err)
	}

	expectedRaw := "olleh" // "hello" reversed
	if string(rawContent) != expectedRaw {
		t.Errorf("Expected raw storage content to be '%s', got '%s'", expectedRaw, string(rawContent))
	}
}

type mockMiddleware struct{}

func (m *mockMiddleware) Writer(w io.Writer) io.Writer {
	return &reverseWriter{w}
}

func (m *mockMiddleware) Reader(r io.Reader) io.Reader {
	return &reverseReader{r}
}

type reverseWriter struct {
	w io.Writer
}

func (rw *reverseWriter) Write(p []byte) (n int, err error) {
	reversed := make([]byte, len(p))
	for i := range p {
		reversed[i] = p[len(p)-1-i]
	}
	return rw.w.Write(reversed)
}

type reverseReader struct {
	r io.Reader
}

func (rr *reverseReader) Read(p []byte) (n int, err error) {
	n, err = rr.r.Read(p)
	if n > 0 {
		for i := 0; i < n/2; i++ {
			p[i], p[n-1-i] = p[n-1-i], p[i]
		}
	}
	return n, err
}
