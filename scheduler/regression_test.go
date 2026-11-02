package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The fixtures are unmodified official REAPI wire messages. In proto3 the
// empty path is omitted on the wire; the decoded default must remain valid.
func TestREAPIWorkingDirectoryOutputFixture(t *testing.T) {
	f := setup(t, false)
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	read := func(suffix string) []byte {
		t.Helper()
		raw, err := os.ReadFile("testdata/working_directory." + suffix)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	f.c.put(read("payload"))
	tree := f.c.put(read("tree.binpb"))
	raw := read("result.binpb")
	var result re.ActionResult
	if err := proto.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.OutputDirectories) != 1 || result.OutputDirectories[0].Path != "" || !proto.Equal(
		result.OutputDirectories[0].TreeDigest,
		tree,
	) {
		t.Fatal("invalid raw REAPI fixture")
	}
	req := completion(lease, "cpu1", &result)
	req.ActionResult = raw
	reply, err := f.s.Complete(f.ctx, req)
	if err != nil || reply.Decision != w.CompleteResponse_ACCEPTED {
		t.Fatalf("working-directory output rejected: %v %v", reply, err)
	}
	if e := f.s.Export().execution(id); e.State != "Published" || f.c.writes != 1 {
		t.Fatalf("working-directory result not published: %+v", e)
	}
}
func TestEmptyPathAllowedOnlyForOutputDirectory(t *testing.T) {
	c := newCache()
	file := c.put([]byte("x"))
	tree := c.putProto(&re.Tree{Root: &re.Directory{}})
	cases := map[string]*re.ActionResult{
		"file":              {OutputFiles: []*re.OutputFile{{Path: "", Digest: file}}},
		"symlink":           {OutputSymlinks: []*re.OutputSymlink{{Path: "", Target: "x"}}},
		"file_symlink":      {OutputFileSymlinks: []*re.OutputSymlink{{Path: "", Target: "x"}}},
		"directory_symlink": {OutputDirectorySymlinks: []*re.OutputSymlink{{Path: "", Target: "x"}}},
		"repeated_root": {
			OutputDirectories: []*re.OutputDirectory{{Path: "", TreeDigest: tree}, {Path: "", TreeDigest: tree}},
		},
	}
	for name, result := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ResultDigests(context.Background(), c, "", result); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected invalid path, got %v", err)
			}
		})
	}
}

type publicationObserverCache struct {
	*fakeCache
	observe func(context.Context) error
}

func (c *publicationObserverCache) UpdateActionResult(
	ctx context.Context,
	instance string,
	d *re.Digest,
	result *re.ActionResult,
) error {
	if err := c.observe(ctx); err != nil {
		return err
	}
	return c.fakeCache.UpdateActionResult(ctx, instance, d, result)
}

// Run the admin handler as a synchronous external publication observer would:
// UpdateActionResult cannot respond until it has read the already committed
// durable prefix. This catches acquiring s.mu in the durable export endpoint.
func verifyDurablePublicationObserver(t *testing.T, store Store) {
	t.Helper()
	f := setup(t, false)
	if store != nil {
		var err error
		f.s, err = New(f.ctx, store, f.c, Config{Now: func() time.Time { return f.now }})
		if err != nil {
			t.Fatal(err)
		}
		f.register(t, "cpu1", map[string]string{"pool": "cpu"})
	}
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	handler := f.s.AdminHandler()
	observed := make(chan error, 1)
	f.s.cache = &publicationObserverCache{fakeCache: f.c, observe: func(ctx context.Context) error {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/audit/durable-export", nil).WithContext(ctx))
		if rec.Code != http.StatusOK {
			err := fmt.Errorf("durable export status %d: %s", rec.Code, rec.Body.String())
			observed <- err
			return err
		}
		var ledger Ledger
		if err := json.Unmarshal(rec.Body.Bytes(), &ledger); err != nil {
			observed <- err
			return err
		}
		e := ledger.execution(id)
		a := ledger.attempt(lease.AttemptId)
		if e == nil || e.State != "Committed" || e.WinnerAttemptID != lease.AttemptId || a == nil || a.Published {
			err := fmt.Errorf("observer did not see committed/unpublished winner: %+v / %+v", e, a)
			observed <- err
			return err
		}
		md, _ := metadata.FromOutgoingContext(ctx)
		sequence, err := strconv.Atoi(md.Get("x-remex-ledger-sequence")[0])
		if err != nil || sequence != len(ledger.Events) || ledger.Events[sequence-1].Kind != "attempt_committed" {
			err = fmt.Errorf(
				"correlation does not identify durable commit prefix: metadata=%v events=%d",
				md,
				len(ledger.Events),
			)
			observed <- err
			return err
		}
		observed <- nil
		return nil
	}}
	type outcome struct {
		reply *w.CompleteResponse
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		reply, err := f.s.Complete(f.ctx, completion(lease, "cpu1", &re.ActionResult{}))
		done <- outcome{reply, err}
	}()
	select {
	case result := <-done:
		if result.err != nil || result.reply.Decision != w.CompleteResponse_ACCEPTED {
			t.Fatalf("complete: %v %v", result.reply, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("durable-export deadlocked synchronous publication observer")
	}
	select {
	case err := <-observed:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("publication observer was not called")
	}
	if f.c.writes != 1 || f.s.Export().execution(id).State != "Published" {
		t.Fatal("observer interrupted publication")
	}
}
func TestDurableExportDuringSynchronousPublication(t *testing.T) {
	verifyDurablePublicationObserver(t, nil)
}

func TestAuditContextReplacesStaleCorrelationMetadata(t *testing.T) {
	f := setup(t, false)
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	original := metadata.Pairs(
		"x-remex-execution-id",
		"stale-one",
		"x-remex-execution-id",
		"stale-two",
		"x-remex-attempt-id",
		"old-attempt",
		"x-remex-epoch",
		"999",
		"x-remex-ledger-sequence",
		"-1",
		"authorization",
		"opaque-test-token",
		"x-custom",
		"preserved",
	)
	ctx := metadata.NewOutgoingContext(f.ctx, original)
	f.s.mu.Lock()
	e := f.s.ledger.execution(id)
	a := f.s.ledger.attempt(lease.AttemptId)
	updated := f.s.auditContext(ctx, e, a)
	f.s.ledger.event(f.now, "test_boundary", e, a, nil)
	updated = f.s.auditContext(updated, e, a)
	wantSequence := len(f.s.ledger.Events)
	f.s.mu.Unlock()
	md, _ := metadata.FromOutgoingContext(updated)
	for key, want := range map[string]string{
		"x-remex-execution-id":    id,
		"x-remex-attempt-id":      lease.AttemptId,
		"x-remex-epoch":           "1",
		"x-remex-ledger-sequence": strconv.Itoa(wantSequence),
		"authorization":           "opaque-test-token",
		"x-custom":                "preserved",
	} {
		values := md.Get(key)
		if len(values) != 1 || values[0] != want {
			t.Fatalf("%s metadata = %q, want one %q", key, values, want)
		}
	}
	before, _ := metadata.FromOutgoingContext(ctx)
	if got := before.Get("x-remex-execution-id"); len(got) != 2 ||
		got[0] != "stale-one" ||
		before.Get("x-remex-ledger-sequence")[0] != "-1" {
		t.Fatal("audit context modified caller metadata")
	}
}

func TestEightConcurrentCompletionsAcknowledgeOneWinner(t *testing.T) {
	f := setup(t, false)
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	var arrivals atomic.Int32
	allVerifying := make(chan struct{})
	release := make(chan struct{})
	f.c.beforeMissing = func() {
		n := arrivals.Add(1)
		if n == 8 {
			close(allVerifying)
		}
		if n <= 8 {
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}
	type outcome struct {
		reply *w.CompleteResponse
		err   error
	}
	done := make(chan outcome, 8)
	req := completion(lease, "cpu1", &re.ActionResult{})
	for i := 0; i < 8; i++ {
		go func() { reply, err := f.s.Complete(ctx, req); done <- outcome{reply, err} }()
	}
	select {
	case <-allVerifying:
		close(release)
	case <-ctx.Done():
		t.Fatal("eight Complete calls did not reach the precommit CAS barrier")
	}
	for i := 0; i < 8; i++ {
		select {
		case got := <-done:
			if got.err != nil || got.reply.Decision != w.CompleteResponse_ACCEPTED {
				t.Fatalf("completion %d = %v %v", i, got.reply, got.err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent completion did not return")
		}
	}
	ledger := f.s.Export()
	commits, publishes := 0, 0
	for _, ev := range ledger.Events {
		if ev.Kind == "attempt_committed" {
			commits++
		}
		if ev.Kind == "ac_published" {
			publishes++
		}
		if ev.Kind == "stale_complete_rejected" {
			t.Fatal("same committed winner was fenced")
		}
	}
	if f.c.writes != 1 || commits != 1 || publishes != 1 || ledger.execution(id).WinnerAttemptID != lease.AttemptId {
		t.Fatalf("writes=%d commits=%d publishes=%d", f.c.writes, commits, publishes)
	}
}

func TestLedgerCloneDoesNotAliasMutableFields(t *testing.T) {
	now := time.Now().UTC()
	original := &Ledger{
		SchemaVersion: 1,
		Executions: []*Execution{
			{
				ID:            "e",
				InvocationIDs: []string{"inv"},
				Platform:      map[string]string{"pool": "cpu"},
				Action:        []byte{1},
				Command:       []byte{2},
				Result:        []byte{3},
			},
		},
		Attempts: []*Attempt{
			{
				ID:               "a",
				WorkerPlatform:   map[string]string{"cpu": "8"},
				CommandStartedAt: &now,
				ResultDigests:    []Digest{{Hash: "digest", SizeBytes: 1}},
				Result:           []byte{4},
			},
		},
		Events: []Event{
			{
				Details: map[string]any{
					"platform":    map[string]string{"pool": "cpu"},
					"digests":     []Digest{{Hash: "event-digest"}},
					"invocations": []string{"original"},
					"decoded":     []any{map[string]any{"values": []any{"original"}}},
					"bytes":       []byte{5},
				},
			},
		},
	}
	before, _ := json.Marshal(original)
	clone := original.clone()
	same, _ := json.Marshal(clone)
	if !bytes.Equal(before, same) {
		t.Fatal("clone changed exported ledger representation")
	}
	e := clone.Executions[0]
	e.InvocationIDs[0] = "changed"
	e.Platform["pool"] = "gpu"
	e.Action[0] = 9
	e.Command[0] = 9
	e.Result[0] = 9
	a := clone.Attempts[0]
	a.WorkerPlatform["cpu"] = "1"
	*a.CommandStartedAt = now.Add(time.Hour)
	a.ResultDigests[0].Hash = "changed"
	a.Result[0] = 9
	details := clone.Events[0].Details
	details["platform"].(map[string]string)["pool"] = "gpu"
	details["digests"].([]Digest)[0].Hash = "changed"
	details["invocations"].([]string)[0] = "changed"
	details["decoded"].([]any)[0].(map[string]any)["values"].([]any)[0] = "changed"
	details["bytes"].([]byte)[0] = 9
	after, _ := json.Marshal(original)
	if !bytes.Equal(before, after) {
		t.Fatal("clone mutation leaked into original ledger")
	}
	for _, ledger := range []*Ledger{{SchemaVersion: 1}, NewLedger()} {
		before, _ := json.Marshal(ledger)
		after, _ := json.Marshal(ledger.clone())
		if !bytes.Equal(before, after) {
			t.Fatal("clone changed nil/empty collection semantics")
		}
	}
}
