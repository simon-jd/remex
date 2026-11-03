package scheduler

import (
	"context"
	"testing"
	"time"

	w "github.com/simon-jd/remex/api/worker/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A Lease that asks to wait is held while there is no work: it answers with
// nothing once Config.LeaseWait is over, is leased work as soon as some is
// queued, and returns when its caller gives up.
func TestLeaseWaitsForWork(t *testing.T) {
	f := setup(t, false)
	limit := f.s.cfg.LeaseWait
	registered, err := f.s.Register(f.ctx, &w.RegisterRequest{WorkerId: "cpu1", Properties: map[string]string{"pool": "cpu"}})
	if err != nil || registered.LeaseWaitMs != limit.Milliseconds() || limit != time.Second/3 {
		t.Fatalf("register = %v, %v; lease wait %v", registered, err, limit)
	}
	request := &w.LeaseRequest{WorkerId: "cpu1", Properties: map[string]string{"pool": "cpu"}, WaitMs: 60_000}

	started := time.Now()
	r, err := f.s.Lease(f.ctx, request)
	if waited := time.Since(started); err != nil || r.Available || waited < limit || waited > limit+time.Second {
		t.Fatalf("idle lease = %v, %v after %v; want no work after %v", r, err, waited, limit)
	}

	leased := make(chan *w.LeaseResponse, 1)
	started = time.Now()
	go func() {
		r, err := f.s.Lease(f.ctx, request)
		if err != nil {
			t.Error(err)
		}
		leased <- r
	}()
	time.Sleep(limit / 4)
	select {
	case r := <-leased:
		t.Fatalf("lease answered %v before any work was queued", r)
	default:
	}
	id := f.start(t, true)
	r = <-leased
	if waited := time.Since(started); !r.Available || r.ExecutionId != id || waited >= limit {
		t.Fatalf("lease = %v after %v; want %s before the wait of %v ran out", r, waited, id, limit)
	}

	// cpu1 now holds a live attempt, so a further Lease waits as well.
	ctx, cancel := context.WithCancel(f.ctx)
	time.AfterFunc(limit/4, cancel)
	if _, err = f.s.Lease(ctx, request); status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled lease error = %v, want Canceled", err)
	}
}
