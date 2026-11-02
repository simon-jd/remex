package faults_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"github.com/simon-jd/remex/scheduler"
	"github.com/simon-jd/remex/tests/protocol/testkit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEvictedOutputCannotCommitAndRepairIsRetryable(t *testing.T) {
	h := testkit.New(t, nil)
	lease := h.Start()
	d := h.Cache.Put([]byte("evict me"))
	req := h.Request(lease, &re.ActionResult{OutputFiles: []*re.OutputFile{{Path: "output", Digest: d}}})
	h.Cache.Evict(d)
	if r := h.Complete(req); r.Decision != w.CompleteResponse_REJECTED {
		t.Fatal(r)
	}
	if h.Cache.Writes() != 0 || h.Scheduler.Export().Executions[0].WinnerAttemptID != "" {
		t.Fatal("missing output reached commit or AC")
	}
	h.Cache.Put([]byte("evict me"))
	if r := h.Complete(req); r.Decision != w.CompleteResponse_ACCEPTED {
		t.Fatal(r)
	}
	if h.Cache.Writes() != 1 {
		t.Fatalf("writes = %d", h.Cache.Writes())
	}
}

// Faults are injected at the public persistence boundary, not by changing the
// scheduler implementation or its private fields.
type commitFailure struct {
	scheduler.Store
	once sync.Once
}

func (s *commitFailure) Save(ctx context.Context, change *scheduler.Change) error {
	fail := false
	for _, e := range change.Events {
		if e.Kind == "attempt_committed" {
			s.once.Do(func() { fail = true })
			break
		}
	}
	if fail {
		return errors.New("independent injected durable commit failure")
	}
	return s.Store.Save(ctx, change)
}

// A failed durable commit stops the scheduler: its memory may be ahead of the
// store, so it neither publishes nor answers from it. The client's retry
// succeeds against a restarted scheduler, which serves what is durable.
func TestFailedDurableCommitNeverPublishesAndClientCanRetry(t *testing.T) {
	store := &commitFailure{Store: scheduler.NewMemoryStore()}
	h := testkit.New(t, store)
	lease := h.Start()
	req := h.Request(lease, &re.ActionResult{})
	_, err := h.Worker.Complete(h.Ctx, req)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("commit error = %v", err)
	}
	durable, err := store.Load(h.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.Cache.Writes() != 0 || durable.Executions[0].WinnerAttemptID != "" {
		t.Fatal("failed durable commit published")
	}
	if _, err = h.Worker.Complete(h.Ctx, req); status.Code(err) != codes.Unavailable {
		t.Fatalf("stopped scheduler answered the retry: %v", err)
	}
	restarted := testkit.New(t, store)
	if r := restarted.Complete(req); r.Decision != w.CompleteResponse_ACCEPTED {
		t.Fatal(r)
	}
	if restarted.Cache.Writes() != 1 || h.Cache.Writes() != 0 {
		t.Fatalf("retry writes = %d", restarted.Cache.Writes())
	}
}

func TestACOutageRetainsWinnerAndRecoveryPublishesOnce(t *testing.T) {
	h := testkit.New(t, nil)
	lease := h.Start()
	h.Cache.FailWrites(true)
	if r := h.Complete(h.Request(lease, &re.ActionResult{})); r.Decision != w.CompleteResponse_ACCEPTED {
		t.Fatal(r)
	}
	e := h.Scheduler.Export().Executions[0]
	if e.State != "Committed" || e.WinnerAttemptID != lease.AttemptId || h.Cache.Writes() != 0 {
		t.Fatalf("lost committed result: %+v", e)
	}
	h.Cache.FailWrites(false)
	if err := h.Scheduler.PublishPending(h.Ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Scheduler.PublishPending(h.Ctx); err != nil {
		t.Fatal(err)
	}
	if h.Cache.Writes() != 1 || len(h.Scheduler.Export().Attempts) != 1 {
		t.Fatal("publication recovery reran or republished action")
	}
}

func TestCancellationAndExpiryDuringRemoteCASVerificationFenceCompletion(t *testing.T) {
	for _, fault := range []string{"cancel", "expire"} {
		t.Run(fault, func(t *testing.T) {
			h := testkit.New(t, nil)
			lease := h.Start()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			h.Cache.OnMissing(func(ctx context.Context) error {
				once.Do(func() { close(entered) })
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			type completion struct {
				response *w.CompleteResponse
				err      error
			}
			done := make(chan completion, 1)
			go func() {
				r, err := h.Worker.Complete(h.Ctx, h.Request(lease, &re.ActionResult{}))
				done <- completion{r, err}
			}()
			select {
			case <-entered:
			case <-h.Ctx.Done():
				t.Fatal("CAS verification never started")
			}
			if fault == "cancel" {
				recorder := httptest.NewRecorder()
				h.Scheduler.AdminHandler().ServeHTTP(
					recorder,
					httptest.NewRequest("POST", "/cancel?id="+lease.ExecutionId, nil),
				)
				if recorder.Code != 204 {
					t.Fatalf("cancel HTTP status = %d", recorder.Code)
				}
			} else {
				h.Advance(2 * time.Minute)
			}
			close(release)
			result := <-done
			if result.err != nil || result.response.Decision != w.CompleteResponse_FENCED {
				t.Fatalf("completion = %v, %v", result.response, result.err)
			}
			if h.Cache.Writes() != 0 || h.Scheduler.Export().Executions[0].WinnerAttemptID != "" {
				t.Fatal("fenced completion reached AC")
			}
			if fault == "expire" {
				next := h.Lease()
				if next.Epoch != lease.Epoch+1 {
					t.Fatal("retry did not increase epoch")
				}
			}
		})
	}
}

func TestOOMEvidenceCannotPublishSuccessfulLookingResult(t *testing.T) {
	h := testkit.New(t, nil)
	lease := h.Start()
	req := h.Request(lease, &re.ActionResult{ExitCode: 0})
	req.OomKill = 1
	_ = h.Complete(req)
	if h.Cache.Writes() != 0 {
		t.Fatal("OOM evidence was ignored: exit=0/status=OK result reached AC")
	}
}

func TestConcurrentDuplicateCompletionPublishesOneWinner(t *testing.T) {
	h := testkit.New(t, nil)
	lease := h.Start()
	req := h.Request(lease, &re.ActionResult{})
	const duplicates = 8
	entered, release := make(chan struct{}), make(chan struct{})
	var arrivals atomic.Int32
	h.Cache.OnMissing(func(ctx context.Context) error {
		if arrivals.Add(1) == duplicates {
			close(entered)
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	type outcome struct {
		result *w.CompleteResponse
		err    error
	}
	results := make(chan outcome, duplicates)
	for range duplicates {
		go func() { result, err := h.Worker.Complete(h.Ctx, req); results <- outcome{result, err} }()
	}
	select {
	case <-entered:
	case <-h.Ctx.Done():
		t.Fatal("concurrent Complete requests did not reach the verification barrier")
	}
	close(release)
	for range duplicates {
		r := <-results
		if r.err != nil ||
			(r.result.Decision != w.CompleteResponse_ACCEPTED && r.result.Decision != w.CompleteResponse_FENCED) {
			t.Fatalf("duplicate response = %v, %v", r.result, r.err)
		}
	}
	if h.Cache.Writes() != 1 {
		t.Fatalf("concurrent duplicate AC writes = %d", h.Cache.Writes())
	}
	commits := 0
	for _, event := range h.Scheduler.Export().Events {
		if event.Kind == "attempt_committed" {
			commits++
		}
	}
	if commits != 1 {
		t.Fatalf("concurrent duplicate committed attempts = %d", commits)
	}
}
