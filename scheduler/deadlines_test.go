package scheduler

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"
)

// The heap must agree with a plain map after any sequence of sets, renewals
// and removals: the same due set, the same deadlines and the same attempts
// per worker.
func TestLeaseDeadlinesMatchMapModel(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	base := time.Unix(0, 0)
	d := newLeaseDeadlines()
	model := map[string]time.Time{}
	owner := map[string]string{}
	for step := range 20000 {
		id := fmt.Sprintf("a%d", rng.Intn(300))
		switch rng.Intn(3) {
		case 0, 1:
			at := base.Add(time.Duration(rng.Intn(1000)) * time.Millisecond)
			if _, ok := owner[id]; !ok {
				owner[id] = fmt.Sprintf("w%d", rng.Intn(20))
			}
			d.set(id, owner[id], at)
			model[id] = at
		case 2:
			d.remove(id)
			delete(model, id)
			delete(owner, id)
		}
		if d.len() != len(model) {
			t.Fatalf("step %d: %d deadlines, model has %d", step, d.len(), len(model))
		}
		now := base.Add(time.Duration(rng.Intn(1000)) * time.Millisecond)
		want := []string{}
		for id, at := range model {
			if !now.Before(at) {
				want = append(want, id)
			}
		}
		got := d.due(now)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("step %d: due %v, want %v", step, got, want)
		}
	}
	for id, at := range model {
		if got, ok := d.get(id); !ok || !got.Equal(at) {
			t.Fatalf("%s: deadline %v, want %v", id, got, at)
		}
		if e := d.worker(owner[id])[id]; e == nil || !e.at.Equal(at) {
			t.Fatalf("%s missing from worker %s", id, owner[id])
		}
	}
	for id := range model {
		d.remove(id)
	}
	if d.len() != 0 || len(d.byWorker) != 0 || len(d.byID) != 0 {
		t.Fatal("removing every attempt left entries behind")
	}
}

// A renewal keeps the attempt's worker and moves it in the heap.
func TestLeaseDeadlineRenewal(t *testing.T) {
	base := time.Unix(0, 0)
	d := newLeaseDeadlines()
	d.set("a", "w1", base.Add(time.Second))
	d.set("b", "w2", base.Add(2*time.Second))
	d.set("a", "other", base.Add(3*time.Second))
	if got := d.due(base.Add(2 * time.Second)); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("due after renewal = %v", got)
	}
	if d.worker("w1")["a"] == nil || d.worker("other") != nil {
		t.Fatal("renewal changed the attempt's worker")
	}
}
