package retryspool

import (
	"context"
	"testing"
)

func TestSetContextHeaders_Basic(t *testing.T) {
	ctx, collected := NewHeaderCollectorContext(context.Background())

	SetContextHeaders(ctx, map[string]string{
		"x-remote-response": "250 OK",
		"x-remote-server":   "mx1.example.com:25",
	})

	if collected["x-remote-response"] != "250 OK" {
		t.Errorf("expected '250 OK', got %q", collected["x-remote-response"])
	}
	if collected["x-remote-server"] != "mx1.example.com:25" {
		t.Errorf("expected 'mx1.example.com:25', got %q", collected["x-remote-server"])
	}
}

func TestSetContextHeaders_NoCollector(t *testing.T) {
	ctx := context.Background()
	// Must not panic
	SetContextHeaders(ctx, map[string]string{"key": "value"})
	SetContextHeader(ctx, "key", "value")
}

func TestSetContextHeaders_Overwrite(t *testing.T) {
	ctx, collected := NewHeaderCollectorContext(context.Background())

	SetContextHeader(ctx, "key", "first")
	SetContextHeader(ctx, "key", "second")

	if collected["key"] != "second" {
		t.Errorf("expected 'second', got %q", collected["key"])
	}
}

func TestSetContextHeaders_MultipleCallsMerge(t *testing.T) {
	ctx, collected := NewHeaderCollectorContext(context.Background())

	SetContextHeaders(ctx, map[string]string{"a": "1"})
	SetContextHeaders(ctx, map[string]string{"b": "2"})
	SetContextHeader(ctx, "c", "3")

	if len(collected) != 3 {
		t.Errorf("expected 3 headers, got %d", len(collected))
	}
}

func TestNewHeaderCollectorContext_Isolation(t *testing.T) {
	ctx1, collected1 := NewHeaderCollectorContext(context.Background())
	ctx2, collected2 := NewHeaderCollectorContext(context.Background())

	SetContextHeader(ctx1, "key", "from-ctx1")
	SetContextHeader(ctx2, "key", "from-ctx2")

	if collected1["key"] != "from-ctx1" {
		t.Errorf("ctx1: expected 'from-ctx1', got %q", collected1["key"])
	}
	if collected2["key"] != "from-ctx2" {
		t.Errorf("ctx2: expected 'from-ctx2', got %q", collected2["key"])
	}
}
