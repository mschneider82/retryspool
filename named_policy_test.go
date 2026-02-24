package retryspool

import (
	"context"
	"strings"
	"testing"
	"time"
)

type mockPolicy struct {
	nextRetry time.Time
	should    bool
}

func (p *mockPolicy) NextRetry(attempts int, lastError error) time.Time {
	return p.nextRetry
}

func (p *mockPolicy) ShouldRetry(attempts int, maxAttempts int, lastError error) bool {
	return p.should
}

func TestNamedRetryPolicy(t *testing.T) {
	// Setup policies
	fastNext := time.Now().Add(1 * time.Second)
	slowNext := time.Now().Add(1 * time.Hour)

	fastPolicy := &mockPolicy{nextRetry: fastNext, should: true}
	slowPolicy := &mockPolicy{nextRetry: slowNext, should: true}

	// Create queue with named policies
	q := New(
		WithNamedRetryPolicy("fast", fastPolicy),
		WithNamedRetryPolicy("slow", slowPolicy),
		WithRetryPolicy(&mockPolicy{should: false}), // Default: no retry
	)

	ctx := context.Background()

	// Test 1: Use "fast" policy via Context
	fastCtx := ContextWithRetryPolicy(ctx, "fast")
	id1, _ := q.Enqueue(fastCtx, nil, strings.NewReader("data1"))

	meta1, _ := q.GetMessageMetadata(ctx, id1)
	if meta1.RetryPolicyName != "fast" {
		t.Errorf("Expected policy 'fast', got '%s'", meta1.RetryPolicyName)
	}

	// Test 2: Use "slow" policy via Header
	id2, _ := q.Enqueue(ctx, map[string]string{"x-retry-policy": "slow"}, strings.NewReader("data2"))
	meta2, _ := q.GetMessageMetadata(ctx, id2)
	if meta2.RetryPolicyName != "slow" {
		t.Errorf("Expected policy 'slow', got '%s'", meta2.RetryPolicyName)
	}

	// Test 3: Change policy afterwards
	err := q.SetMessageRetryPolicy(ctx, id1, "slow")
	if err != nil {
		t.Fatalf("Failed to set retry policy: %v", err)
	}

	meta1Updated, _ := q.GetMessageMetadata(ctx, id1)
	if meta1Updated.RetryPolicyName != "slow" {
		t.Errorf("Expected updated policy 'slow', got '%s'", meta1Updated.RetryPolicyName)
	}
}
