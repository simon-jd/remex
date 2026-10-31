package scheduler

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// creationOrderLedger holds three executions created 100 ms, 120 ms and 1 s
// after noon. Their created_at text sorts .12Z before .1Z, and their IDs
// sort in reverse, so neither can stand in for creation order.
func creationOrderLedger() (*Ledger, []string) {
	noon := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	l := NewLedger()
	ids := []string{"executions/c", "executions/b", "executions/a"}
	for i, offset := range []time.Duration{100 * time.Millisecond, 120 * time.Millisecond, time.Second} {
		e := &Execution{ID: ids[i], CurrentEpoch: 1, State: "Queued", CreatedAt: noon.Add(offset)}
		l.Executions = append(l.Executions, e)
		l.event(noon.Add(offset), "execution_created", e, nil, nil)
	}
	return l, ids
}

func executionIDs(l *Ledger) []string {
	var ids []string
	for _, e := range l.Executions {
		ids = append(ids, e.ID)
	}
	return ids
}

func sameOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSortByCreationUsesEventOrder(t *testing.T) {
	l, want := creationOrderLedger()
	l.Executions = []*Execution{l.Executions[2], l.Executions[0], l.Executions[1]}
	// One execution without an event, as in a hand-built ledger, goes last.
	l.Executions = append([]*Execution{{ID: "executions/unnamed", CreatedAt: time.Unix(0, 0)}}, l.Executions...)
	sortByCreation(l)
	if got := executionIDs(l); !sameOrder(got, append(want, "executions/unnamed")) {
		t.Fatalf("order = %v, want %v then executions/unnamed", got, want)
	}
}

func TestPostgresLoadKeepsCreationOrder(t *testing.T) {
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
	defer store.Close()
	l, want := creationOrderLedger()
	if err = store.Save(ctx, &Change{Executions: l.Executions, Events: l.Events}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := executionIDs(loaded); !sameOrder(got, want) {
		t.Fatalf("loaded order = %v, want creation order %v", got, want)
	}
}
