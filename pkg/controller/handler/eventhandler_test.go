// SPDX-FileCopyrightText: 2024 The Crossplane Authors <https://crossplane.io>
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func newTestItem(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name}}
}

func newTestHandler() *EventHandler {
	eh := NewEventHandler(WithLogger(logging.NewNopLogger()))
	eh.queue = workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	return eh
}

func TestRequestReconcile_NoRateLimiter_UsesBackoff(t *testing.T) {
	eh := newTestHandler()

	if ok := eh.RequestReconcile(NoRateLimiter, "test-resource", nil); !ok {
		t.Fatal("expected RequestReconcile to return true on first call")
	}

	rl, exists := eh.rateLimiterMap[defaultRateLimiter]
	if !exists || rl == nil {
		t.Fatal("expected default rate limiter to be created in map")
	}

	item := newTestItem("test-resource")
	if got := rl.NumRequeues(item); got != 1 {
		t.Errorf("expected 1 requeue after first call, got %d", got)
	}

	if ok := eh.RequestReconcile(NoRateLimiter, "test-resource", nil); !ok {
		t.Fatal("expected RequestReconcile to return true on second call")
	}
	if got := rl.NumRequeues(item); got != 2 {
		t.Errorf("expected 2 requeues after second call, got %d", got)
	}
}

func TestRequestReconcile_NoRateLimiter_BackoffIncreases(t *testing.T) {
	eh := newTestHandler()

	rl := workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]()
	eh.rateLimiterMap[defaultRateLimiter] = rl

	item := newTestItem("test-resource")

	first := rl.When(item)
	second := rl.When(item)
	third := rl.When(item)

	if first <= 0 {
		t.Errorf("expected positive first delay, got %v", first)
	}
	if second <= first {
		t.Errorf("expected second delay (%v) > first delay (%v)", second, first)
	}
	if third <= second {
		t.Errorf("expected third delay (%v) > second delay (%v)", third, second)
	}
}

func TestRequestReconcile_NamedRateLimiter(t *testing.T) {
	eh := newTestHandler()

	if ok := eh.RequestReconcile("my-limiter", "test-resource", nil); !ok {
		t.Fatal("expected RequestReconcile to return true")
	}

	if _, exists := eh.rateLimiterMap["my-limiter"]; !exists {
		t.Fatal("expected named rate limiter to be created")
	}
	if _, exists := eh.rateLimiterMap[defaultRateLimiter]; exists {
		t.Fatal("expected default rate limiter NOT to be created for named call")
	}
}

func TestRequestReconcile_FailureLimit(t *testing.T) {
	eh := newTestHandler()

	limit := 2
	for i := 0; i < limit; i++ {
		if ok := eh.RequestReconcile(NoRateLimiter, "test-resource", &limit); !ok {
			t.Fatalf("expected RequestReconcile to return true on call %d", i+1)
		}
	}
	// NumRequeues is checked BEFORE When(), so after 2 When() calls NumRequeues=2.
	// 2 > 2 is false, so call 3 passes. Call 4 has NumRequeues=3, 3 > 2 → rejected.
	if ok := eh.RequestReconcile(NoRateLimiter, "test-resource", &limit); !ok {
		t.Fatal("expected call 3 to succeed (NumRequeues=2, not > limit=2)")
	}
	if ok := eh.RequestReconcile(NoRateLimiter, "test-resource", &limit); ok {
		t.Fatal("expected call 4 to fail (NumRequeues=3 > limit=2)")
	}
}

func TestRequestReconcile_NilQueue(t *testing.T) {
	eh := NewEventHandler(WithLogger(logging.NewNopLogger()))
	if ok := eh.RequestReconcile(NoRateLimiter, "test-resource", nil); ok {
		t.Fatal("expected RequestReconcile to return false when queue is nil")
	}
}

func TestForget_ResetsDefaultRateLimiter(t *testing.T) {
	eh := newTestHandler()

	for i := 0; i < 5; i++ {
		eh.RequestReconcile(NoRateLimiter, "test-resource", nil)
	}

	rl := eh.rateLimiterMap[defaultRateLimiter]
	item := newTestItem("test-resource")
	if got := rl.NumRequeues(item); got != 5 {
		t.Fatalf("expected 5 requeues, got %d", got)
	}

	eh.Forget("some-named-limiter", "test-resource")
	if got := rl.NumRequeues(item); got != 0 {
		t.Errorf("expected default rate limiter to be reset to 0 requeues, got %d", got)
	}
}

func TestForget_NamedRateLimiter(t *testing.T) {
	eh := newTestHandler()

	for i := 0; i < 3; i++ {
		eh.RequestReconcile("my-limiter", "test-resource", nil)
	}

	rl := eh.rateLimiterMap["my-limiter"]
	item := newTestItem("test-resource")
	if got := rl.NumRequeues(item); got != 3 {
		t.Fatalf("expected 3 requeues, got %d", got)
	}

	eh.Forget("my-limiter", "test-resource")
	if got := rl.NumRequeues(item); got != 0 {
		t.Errorf("expected rate limiter to be reset to 0 after Forget, got %d", got)
	}
}

func TestForget_DefaultRateLimiterDirectly(t *testing.T) {
	eh := newTestHandler()

	for i := 0; i < 3; i++ {
		eh.RequestReconcile(NoRateLimiter, "test-resource", nil)
	}

	rl := eh.rateLimiterMap[defaultRateLimiter]
	item := newTestItem("test-resource")
	if got := rl.NumRequeues(item); got != 3 {
		t.Fatalf("expected 3 requeues, got %d", got)
	}

	eh.Forget(defaultRateLimiter, "test-resource")
	if got := rl.NumRequeues(item); got != 0 {
		t.Errorf("expected 0 requeues after direct Forget, got %d", got)
	}
}

// TestNoRateLimiter_FixVerification validates the fix for upjet#592:
// NoRateLimiter must produce a positive delay via exponential backoff,
// not the previous when=0 (instant retry) behavior.
func TestNoRateLimiter_FixVerification(t *testing.T) {
	rl := workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]()
	item := newTestItem("test-resource")

	when := rl.When(item)
	if when <= 0 {
		t.Errorf("expected positive delay from DefaultControllerRateLimiter, got %v", when)
	}
	if when < 5*time.Millisecond {
		t.Errorf("expected delay >= 5ms (default base delay), got %v", when)
	}
}
