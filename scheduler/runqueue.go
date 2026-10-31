package scheduler

import (
	"container/heap"
	"sort"
	"strings"
)

// runQueues holds the runnable executions grouped by platform, each group
// oldest first, so Lease looks only at the head of every platform a worker
// can serve instead of sorting all runnable work under the lock. Entries are
// ledger positions: creation order, which never changes, so a retried
// execution keeps its place ahead of newer work.
//
// Entries are removed lazily. An execution is pushed each time it becomes
// runnable, and an entry whose execution is no longer runnable is dropped
// when it reaches the head of its queue.
type runQueues map[string]*runQueue

type runQueue struct {
	platform  map[string]string
	positions []int // a min-heap of ledger positions
}

func (q *runQueue) Len() int           { return len(q.positions) }
func (q *runQueue) Less(i, j int) bool { return q.positions[i] < q.positions[j] }
func (q *runQueue) Swap(i, j int)      { q.positions[i], q.positions[j] = q.positions[j], q.positions[i] }
func (q *runQueue) Push(x any)         { q.positions = append(q.positions, x.(int)) }
func (q *runQueue) Pop() any {
	last := q.positions[len(q.positions)-1]
	q.positions = q.positions[:len(q.positions)-1]
	return last
}

// platformKey is a canonical form of a platform, equal for equal maps.
func platformKey(platform map[string]string) string {
	keys := make([]string, 0, len(platform))
	for k := range platform {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(platform[k])
		b.WriteByte(0)
	}
	return b.String()
}

// push queues the execution at ledger position.
func (qs runQueues) push(e *Execution, position int) {
	key := platformKey(e.Platform)
	q, ok := qs[key]
	if !ok {
		q = &runQueue{platform: copyProps(e.Platform)}
		qs[key] = q
	}
	heap.Push(q, position)
}

// oldest returns the ledger position of the oldest runnable execution whose
// platform the worker properties satisfy, or -1. runnable reports whether
// the execution at a position is still runnable; stale heads are dropped,
// and so are queues left empty.
func (qs runQueues) oldest(properties map[string]string, runnable func(position int) bool) int {
	best := -1
	for key, q := range qs {
		for q.Len() > 0 && !runnable(q.positions[0]) {
			heap.Pop(q)
		}
		if q.Len() == 0 {
			delete(qs, key)
			continue
		}
		if head := q.positions[0]; (best < 0 || head < best) && Match(q.platform, properties) == nil {
			best = head
		}
	}
	return best
}
