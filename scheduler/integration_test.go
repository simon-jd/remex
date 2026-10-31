package scheduler

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// These tests never silently substitute an in-memory service. CI can opt into
// real PostgreSQL and bazel-remote by providing the two endpoint variables.
func TestPostgresDurabilityIntegration(t *testing.T) {
	dsn := os.Getenv("REMEX_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("REMEX_TEST_POSTGRES is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	schema := "remex_test_" + newID("")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	store, err := OpenPostgres(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenPostgres(ctx, u.String())
	if err == nil {
		second.Close()
		t.Fatal("second scheduler acquired ownership")
	}
	l := NewLedger()
	e := &Execution{ID: "executions/integration", CurrentEpoch: 1, State: "Queued"}
	l.Executions = append(l.Executions, e)
	l.event(time.Now(), "execution_created", e, nil, nil)
	if err = store.Save(ctx, &Change{Executions: []*Execution{e}, Events: l.Events}); err != nil {
		t.Fatal(err)
	}
	// A canceled client cannot cancel the ownership connection or durable commit.
	var originalXmin string
	if err = store.conn.QueryRow(ctx, "SELECT xmin::text FROM executions WHERE id=$1", e.ID).Scan(&originalXmin); err != nil {
		t.Fatal(err)
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	extra := &Execution{ID: "executions/cancelled-client", CurrentEpoch: 1, State: "Queued"}
	l.Executions = append(l.Executions, extra)
	l.event(time.Now(), "execution_created", extra, nil, nil)
	if err = store.Save(cancelled, &Change{Executions: []*Execution{extra}, Events: l.Events[1:]}); err != nil {
		t.Fatalf("cancelled client destroyed commit: %v", err)
	}
	var unchangedXmin string
	if err = store.conn.QueryRow(ctx, "SELECT xmin::text FROM executions WHERE id=$1", e.ID).Scan(&unchangedXmin); err != nil || unchangedXmin != originalXmin {
		t.Fatalf("unchanged row rewritten: %s -> %s, %v", originalXmin, unchangedXmin, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPostgres(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, err := reopened.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Executions) != 2 || len(loaded.Events) != 2 || loaded.execution(e.ID) == nil {
		t.Fatalf("bad persisted ledger: %+v", loaded)
	}
	if err = reopened.Save(ctx, &Change{}); err != nil {
		t.Fatal(err)
	}
	stale := &Change{Events: loaded.Events[1:]}
	if err = reopened.Save(ctx, stale); err == nil {
		t.Fatal("replayed event sequence accepted")
	}
	var count int
	if err = reopened.conn.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&count); err != nil || count != 2 {
		t.Fatalf("event append idempotence count=%d err=%v", count, err)
	}
	// Losing the session also loses the leader lock: never keep serving memory.
	if _, err = admin.Exec(ctx, "SELECT pg_terminate_backend($1)", reopened.conn.PgConn().PID()); err != nil {
		t.Fatal(err)
	}
	loaded.execution(e.ID).State = "Cancelled"
	if err = reopened.Save(ctx, &Change{Executions: []*Execution{loaded.execution(e.ID)}}); !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("lost leader connection not fatal: %v", err)
	}
}
func TestBazelRemoteCASIntegration(t *testing.T) {
	endpoint := os.Getenv("REMEX_TEST_CAS")
	if endpoint == "" {
		t.Skip("REMEX_TEST_CAS is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := re.NewContentAddressableStorageClient(conn)
	cache := NewGRPCCache(conn)
	fake := newCache()
	content := []byte("remex scheduler CAS integration " + newID(""))
	digest := fake.put(content)
	write, err := client.BatchUpdateBlobs(ctx, &re.BatchUpdateBlobsRequest{Requests: []*re.BatchUpdateBlobsRequest_Request{{Digest: digest, Data: content}}, DigestFunction: re.DigestFunction_SHA256})
	if err != nil || len(write.GetResponses()) != 1 || write.Responses[0].Status.Code != 0 {
		t.Fatalf("CAS write %v %v", write, err)
	}
	got, err := cache.ReadBlob(ctx, "", digest)
	if err != nil || string(got) != string(content) {
		t.Fatalf("CAS read %q %v", got, err)
	}
	missing, err := cache.Missing(ctx, "", []*re.Digest{digest})
	if err != nil || len(missing) != 0 {
		t.Fatalf("CAS missing %v %v", missing, err)
	}
	actionDigest := fake.putProto(&re.Action{Salt: []byte(newID(""))})
	result := &re.ActionResult{OutputFiles: []*re.OutputFile{{Path: "result", Digest: digest}}}
	if err = cache.UpdateActionResult(ctx, "", actionDigest, result); err != nil {
		t.Fatal(err)
	}
	read, err := cache.GetActionResult(ctx, "", actionDigest)
	if err != nil || read.ExitCode != result.ExitCode || len(read.OutputFiles) != 1 || read.OutputFiles[0].Digest.Hash != digest.Hash {
		t.Fatalf("AC read %v %v", read, err)
	}
}

func TestPostgresDurablePublicationAuditIntegration(t *testing.T) {
	dsn := os.Getenv("REMEX_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("REMEX_TEST_POSTGRES is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	schema := "remex_test_" + newID("")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	endpoint, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := endpoint.Query()
	query.Set("search_path", schema)
	endpoint.RawQuery = query.Encode()
	store, err := OpenPostgres(ctx, endpoint.String())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	verifyDurablePublicationObserver(t, store)
	// The observer's read must not desynchronize incremental Save bookkeeping.
	ledger, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Executions) != 1 || ledger.Executions[0].State != "Published" || !ledger.Attempts[0].Published || ledger.Events[len(ledger.Events)-1].Kind != "ac_published" {
		t.Fatalf("publication after durable observation was not persisted: %+v", ledger)
	}
}

// An audit snapshot runs while the leader commits. Everything it returns must
// come from one commit: rows and events read by separate statements could
// each see a different one.
func TestPostgresSnapshotSeesOneCommit(t *testing.T) {
	dsn := os.Getenv("REMEX_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("REMEX_TEST_POSTGRES is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	schema := "remex_test_" + newID("")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	endpoint, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := endpoint.Query()
	query.Set("search_path", schema)
	endpoint.RawQuery = query.Encode()
	store, err := OpenPostgres(ctx, endpoint.String())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Commit i sets both rows to "i" and appends event i+1.
	l := NewLedger()
	e := &Execution{ID: "executions/snapshot", CurrentEpoch: 1, State: "0"}
	a := &Attempt{ID: "attempt-snapshot", ExecutionID: e.ID, Epoch: 1, Phase: "0"}
	l.event(time.Now(), "execution_created", e, a, nil)
	if err = store.Save(ctx, &Change{Executions: []*Execution{e}, Attempts: []*Attempt{a}, Events: l.Events}); err != nil {
		t.Fatal(err)
	}
	const commits = 300
	written := make(chan error, 1)
	go func() {
		for i := 1; i <= commits; i++ {
			execution, attempt := *e, *a
			execution.State, attempt.Phase = strconv.Itoa(i), strconv.Itoa(i)
			l.event(time.Now(), "advanced", &execution, &attempt, nil)
			if err := store.Save(ctx, &Change{Executions: []*Execution{&execution}, Attempts: []*Attempt{&attempt}, Events: l.Events[i:]}); err != nil {
				written <- err
				return
			}
		}
		written <- nil
	}()
	for snapshots := 0; ; snapshots++ {
		select {
		case err := <-written:
			if err != nil {
				t.Fatal(err)
			}
			if snapshots < 10 {
				t.Fatalf("only %d snapshots overlapped the commits", snapshots)
			}
			return
		default:
		}
		s, err := store.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if want := strconv.Itoa(len(s.Events) - 1); s.Executions[0].State != want || s.Attempts[0].Phase != want {
			t.Fatalf("snapshot mixes commits: execution at %s, attempt at %s, %d events", s.Executions[0].State, s.Attempts[0].Phase, len(s.Events))
		}
	}
}
