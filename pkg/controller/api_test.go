// SPDX-FileCopyrightText: 2023 The Crossplane Authors <https://crossplane.io>
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/pkg/logging"
	xpresource "github.com/crossplane/crossplane-runtime/pkg/resource"
	xpfake "github.com/crossplane/crossplane-runtime/pkg/resource/fake"
	"github.com/crossplane/crossplane-runtime/pkg/test"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	ctrl "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/crossplane/upjet/pkg/controller/handler"
	"github.com/crossplane/upjet/pkg/resource"
	"github.com/crossplane/upjet/pkg/resource/fake"
	tjerrors "github.com/crossplane/upjet/pkg/terraform/errors"
)

type recordingQueue struct {
	addAfter []time.Duration
}

var _ workqueue.TypedRateLimitingInterface[reconcile.Request] = &recordingQueue{}

func (q *recordingQueue) Add(item reconcile.Request) {}

func (q *recordingQueue) Len() int {
	return 0
}

func (q *recordingQueue) Get() (reconcile.Request, bool) {
	return reconcile.Request{}, true
}

func (q *recordingQueue) Done(item reconcile.Request) {}

func (q *recordingQueue) ShutDown() {}

func (q *recordingQueue) ShutDownWithDrain() {}

func (q *recordingQueue) ShuttingDown() bool {
	return false
}

func (q *recordingQueue) AddAfter(item reconcile.Request, duration time.Duration) {
	q.addAfter = append(q.addAfter, duration)
}

func (q *recordingQueue) AddRateLimited(item reconcile.Request) {}

func (q *recordingQueue) Forget(item reconcile.Request) {}

func (q *recordingQueue) NumRequeues(item reconcile.Request) int {
	return 0
}

func (q *recordingQueue) Reset() {
	q.addAfter = nil
}

func newTestEventHandler(t *testing.T, name string) (*handler.EventHandler, *recordingQueue) {
	t.Helper()

	eh := handler.NewEventHandler(handler.WithLogger(logging.NewNopLogger()))
	queue := &recordingQueue{}
	obj := fake.NewTerraformed()
	obj.SetName(name)
	eh.Generic(context.Background(), event.GenericEvent{Object: obj}, queue)

	return eh, queue
}

func TestAPICallbacksCreate(t *testing.T) {
	type args struct {
		mgr ctrl.Manager
		mg  xpresource.ManagedKind
		err error
	}
	type want struct {
		err error
	}
	cases := map[string]struct {
		reason string
		args
		want
	}{
		"CreateOperationFailed": {
			reason: "It should update the condition with error if async apply failed",
			args: args{
				mg: xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
				mgr: &xpfake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: func(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
							got := obj.(resource.Terraformed).GetCondition(resource.TypeLastAsyncOperation)
							if diff := cmp.Diff(resource.LastAsyncOperationCondition(tjerrors.NewApplyFailed(nil)), got); diff != "" {
								t.Errorf("\nCreate(...): -want error, +got error:\n%s", diff)
							}
							return nil
						},
					},
					Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
				},
				err: tjerrors.NewApplyFailed(nil),
			},
		},
		"CreateOperationSucceeded": {
			reason: "It should update the condition with success if the apply operation does not report error",
			args: args{
				mg: xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
				mgr: &xpfake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: func(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
							got := obj.(resource.Terraformed).GetCondition(resource.TypeLastAsyncOperation)
							if diff := cmp.Diff(resource.LastAsyncOperationCondition(nil), got); diff != "" {
								t.Errorf("\nCreate(...): -want error, +got error:\n%s", diff)
							}
							return nil
						},
					},
					Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
				},
			},
		},
		"CannotGet": {
			reason: "It should return error if it cannot get the resource to update",
			args: args{
				mg: xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
				mgr: &xpfake.Manager{
					Client: &test.MockClient{
						MockGet: func(_ context.Context, _ client.ObjectKey, _ client.Object) error {
							return errBoom
						},
					},
					Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
				},
			},
			want: want{
				err: errors.Wrapf(errBoom, errGetFmt, "", ", Kind=/name", "create"),
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := NewAPICallbacks(tc.args.mgr, tc.args.mg)
			err := e.Create("name")(tc.args.err, context.TODO())
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nCreate(...): -want error, +got error:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestAPICallbacksUpdate(t *testing.T) {
	type args struct {
		mgr ctrl.Manager
		mg  xpresource.ManagedKind
		err error
	}
	type want struct {
		err error
	}
	cases := map[string]struct {
		reason string
		args
		want
	}{
		"UpdateOperationFailed": {
			reason: "It should update the condition with error if async apply failed",
			args: args{
				mg: xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
				mgr: &xpfake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: func(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
							got := obj.(resource.Terraformed).GetCondition(resource.TypeLastAsyncOperation)
							if diff := cmp.Diff(resource.LastAsyncOperationCondition(tjerrors.NewApplyFailed(nil)), got); diff != "" {
								t.Errorf("\nUpdate(...): -want error, +got error:\n%s", diff)
							}
							return nil
						},
					},
					Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
				},
				err: tjerrors.NewApplyFailed(nil),
			},
		},
		"ApplyOperationSucceeded": {
			reason: "It should update the condition with success if the apply operation does not report error",
			args: args{
				mg: xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
				mgr: &xpfake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: func(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
							got := obj.(resource.Terraformed).GetCondition(resource.TypeLastAsyncOperation)
							if diff := cmp.Diff(resource.LastAsyncOperationCondition(nil), got); diff != "" {
								t.Errorf("\nUpdate(...): -want error, +got error:\n%s", diff)
							}
							return nil
						},
					},
					Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
				},
			},
		},
		"CannotGet": {
			reason: "It should return error if it cannot get the resource to update",
			args: args{
				mg: xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
				mgr: &xpfake.Manager{
					Client: &test.MockClient{
						MockGet: func(_ context.Context, _ client.ObjectKey, _ client.Object) error {
							return errBoom
						},
					},
					Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
				},
			},
			want: want{
				err: errors.Wrapf(errBoom, errGetFmt, "", ", Kind=/name", "update"),
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := NewAPICallbacks(tc.args.mgr, tc.args.mg)
			err := e.Update("name")(tc.args.err, context.TODO())
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nUpdate(...): -want error, +got error:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestAPICallbacks_Destroy(t *testing.T) {
	type args struct {
		mgr ctrl.Manager
		mg  xpresource.ManagedKind
		err error
	}
	type want struct {
		err error
	}
	cases := map[string]struct {
		reason string
		args
		want
	}{
		"DestroyOperationFailed": {
			reason: "It should update the condition with error if async destroy failed",
			args: args{
				mg: xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
				mgr: &xpfake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: func(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
							got := obj.(resource.Terraformed).GetCondition(resource.TypeLastAsyncOperation)
							if diff := cmp.Diff(resource.LastAsyncOperationCondition(tjerrors.NewDestroyFailed(nil)), got); diff != "" {
								t.Errorf("\nApply(...): -want error, +got error:\n%s", diff)
							}
							return nil
						},
					},
					Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
				},
				err: tjerrors.NewDestroyFailed(nil),
			},
		},
		"DestroyOperationSucceeded": {
			reason: "It should update the condition with success if the destroy operation does not report error",
			args: args{
				mg: xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
				mgr: &xpfake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: func(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
							got := obj.(resource.Terraformed).GetCondition(resource.TypeLastAsyncOperation)
							if diff := cmp.Diff(resource.LastAsyncOperationCondition(nil), got); diff != "" {
								t.Errorf("\nApply(...): -want error, +got error:\n%s", diff)
							}
							return nil
						},
					},
					Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
				},
			},
		},
		"CannotGet": {
			reason: "It should return error if it cannot get the resource to update",
			args: args{
				mg: xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
				mgr: &xpfake.Manager{
					Client: &test.MockClient{
						MockGet: func(_ context.Context, _ client.ObjectKey, _ client.Object) error {
							return errBoom
						},
					},
					Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
				},
			},
			want: want{
				err: errors.Wrapf(errBoom, errGetFmt, "", ", Kind=/name", "destroy"),
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := NewAPICallbacks(tc.args.mgr, tc.args.mg)
			err := e.Destroy("name")(tc.args.err, context.TODO())
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nDestroy(...): -want error, +got error:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestAPICallbacksCreate_SuccessRequeueUsesFreshLimiter(t *testing.T) {
	eh, queue := newTestEventHandler(t, "name")

	for i := 0; i < 5; i++ {
		if ok := eh.RequestReconcile(handler.NoRateLimiter, "name", nil); !ok {
			t.Fatalf("RequestReconcile() = false on seed call %d", i+1)
		}
	}
	queue.Reset()

	e := NewAPICallbacks(
		&xpfake.Manager{
			Client: &test.MockClient{
				MockGet: test.NewMockGetFn(nil),
				MockStatusUpdate: func(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					return nil
				},
			},
			Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
		},
		xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
		WithEventHandler(eh),
	)

	if err := e.Create("name")(nil, context.TODO()); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if len(queue.addAfter) != 1 {
		t.Fatalf("expected 1 delayed requeue, got %d", len(queue.addAfter))
	}
	if got := queue.addAfter[0]; got != 5*time.Millisecond {
		t.Fatalf("expected fresh callback follow-up delay %v, got %v", 5*time.Millisecond, got)
	}
}

// TestAPICallbacksCreate_SuccessResetsFollowUpBucket verifies that a
// successful callback resets the asyncCallbackFollow bucket itself.
// Even if prior successes have accumulated backoff in that bucket,
// the next success must still see a fresh 5ms delay because callbackFn
// calls Forget(rateLimiterCallbackFollow) before RequestReconcile.
func TestAPICallbacksCreate_SuccessResetsFollowUpBucket(t *testing.T) {
	eh, queue := newTestEventHandler(t, "name")

	// Pollute the asyncCallbackFollow bucket with 5 prior requeues
	// to simulate accumulated backoff from previous success follow-ups.
	for i := 0; i < 5; i++ {
		if ok := eh.RequestReconcile(rateLimiterCallbackFollow, "name", nil); !ok {
			t.Fatalf("RequestReconcile(asyncCallbackFollow) = false on seed call %d", i+1)
		}
	}
	queue.Reset()

	e := NewAPICallbacks(
		&xpfake.Manager{
			Client: &test.MockClient{
				MockGet: test.NewMockGetFn(nil),
				MockStatusUpdate: func(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					return nil
				},
			},
			Scheme: xpfake.SchemeWith(&fake.Terraformed{}),
		},
		xpresource.ManagedKind(xpfake.GVK(&fake.Terraformed{})),
		WithEventHandler(eh),
	)

	if err := e.Create("name")(nil, context.TODO()); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if len(queue.addAfter) != 1 {
		t.Fatalf("expected 1 delayed requeue, got %d", len(queue.addAfter))
	}
	if got := queue.addAfter[0]; got != 5*time.Millisecond {
		t.Fatalf("expected follow-up bucket to be reset to %v after Forget, got %v", 5*time.Millisecond, got)
	}
}
