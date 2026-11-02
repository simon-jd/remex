package scheduler

import (
	"container/heap"
	"time"
)

// leaseDeadlines holds the lease deadline of every live attempt, indexed two
// ways: a min-heap by deadline, so expiry visits only the attempts that are
// due, and a set per worker, so Lease finds the caller's own attempt without
// looking at anyone else's. Setting, renewing and removing a deadline cost
// O(log n) in the number of live attempts; finding the k due ones costs O(k).
type leaseDeadlines struct {
	heap     deadlineHeap
	byID     map[string]*deadlineEntry
	byWorker map[string]map[string]*deadlineEntry
}

type deadlineEntry struct {
	id, worker string
	at         time.Time
	index      int // position in the heap
}

func newLeaseDeadlines() *leaseDeadlines {
	return &leaseDeadlines{byID: map[string]*deadlineEntry{}, byWorker: map[string]map[string]*deadlineEntry{}}
}

// set records or renews the deadline of attempt id, run by worker. A renewal
// keeps the worker the attempt was first recorded with.
func (d *leaseDeadlines) set(id, worker string, at time.Time) {
	if e, ok := d.byID[id]; ok {
		e.at = at
		heap.Fix(&d.heap, e.index)
		return
	}
	e := &deadlineEntry{id: id, worker: worker, at: at}
	heap.Push(&d.heap, e)
	d.byID[id] = e
	if d.byWorker[worker] == nil {
		d.byWorker[worker] = map[string]*deadlineEntry{}
	}
	d.byWorker[worker][id] = e
}

// get returns the deadline of attempt id; the zero time if it has none.
func (d *leaseDeadlines) get(id string) (time.Time, bool) {
	if e, ok := d.byID[id]; ok {
		return e.at, true
	}
	return time.Time{}, false
}

func (d *leaseDeadlines) remove(id string) {
	e, ok := d.byID[id]
	if !ok {
		return
	}
	heap.Remove(&d.heap, e.index)
	delete(d.byID, id)
	if delete(d.byWorker[e.worker], id); len(d.byWorker[e.worker]) == 0 {
		delete(d.byWorker, e.worker)
	}
}

func (d *leaseDeadlines) len() int { return len(d.heap) }

// due returns the attempts whose deadline is at or before now. It descends
// only into heap nodes that are due, so it visits at most 2k+1 nodes.
func (d *leaseDeadlines) due(now time.Time) []string {
	var ids []string
	var visit func(int)
	visit = func(i int) {
		if i >= len(d.heap) || now.Before(d.heap[i].at) {
			return
		}
		ids = append(ids, d.heap[i].id)
		visit(2*i + 1)
		visit(2*i + 2)
	}
	visit(0)
	return ids
}

// worker returns the attempts recorded for worker and their deadlines.
func (d *leaseDeadlines) worker(worker string) map[string]*deadlineEntry { return d.byWorker[worker] }

type deadlineHeap []*deadlineEntry

func (h deadlineHeap) Len() int           { return len(h) }
func (h deadlineHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h deadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *deadlineHeap) Push(x any) {
	e := x.(*deadlineEntry)
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *deadlineHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return e
}
