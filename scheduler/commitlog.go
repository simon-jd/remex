package scheduler

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// noWait marks a call whose answer depends on no ledger change that could
// still be lost; settle returns at once for it.
const noWait = ^uint64(0)

// commitLog makes ledger changes durable in groups. A mutation applies its
// change in memory and appends it here under s.mu, then releases the lock;
// one writer saves every change appended while its previous write was in
// flight as a single store transaction. A call answers only once the log
// is durable up to the position it observed, so a client never sees a
// change that a crash could lose. The first failed write stops the log:
// memory may then be ahead of the store, so the scheduler fails stop and a
// restart reloads the durable prefix.
type commitLog struct {
	store  Store
	onFail func(error)

	mu       sync.Mutex
	pending  []*Change
	tail     uint64        // position of the last appended change
	durable  uint64        // position of the last durable change
	flushed  chan struct{} // closed, then replaced, after every write
	err      error         // the failed write that stopped the log
	closing  bool
	writes   int // store transactions, for tests and metrics
	appended int // changes appended

	wake chan struct{}
	done chan struct{}
}

func newCommitLog(store Store, onFail func(error)) *commitLog {
	l := &commitLog{store: store, onFail: onFail, flushed: make(chan struct{}),
		wake: make(chan struct{}, 1), done: make(chan struct{})}
	go l.run()
	return l
}

// append queues a change and returns its log position.
func (l *commitLog) append(c *Change) uint64 {
	l.mu.Lock()
	l.pending = append(l.pending, c)
	l.tail++
	l.appended++
	position := l.tail
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return position
}

// position is the log position of the last appended change.
func (l *commitLog) position() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tail
}

// wait blocks until the change at position is durable.
func (l *commitLog) wait(ctx context.Context, position uint64) error {
	for {
		l.mu.Lock()
		durable, err, flushed := l.durable, l.err, l.flushed
		l.mu.Unlock()
		if position <= durable {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Unavailable, "ledger commit failed: %v", err)
		}
		select {
		case <-flushed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// stats reports how many changes were appended and how many store
// transactions made them durable.
func (l *commitLog) stats() (changes, writes int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appended, l.writes
}

// close writes what is pending, then stops the writer.
func (l *commitLog) close() {
	l.mu.Lock()
	l.closing = true
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
	<-l.done
}

func (l *commitLog) run() {
	defer close(l.done)
	for range l.wake {
		for {
			l.mu.Lock()
			batch, end, closing := l.pending, l.tail, l.closing
			l.pending = nil
			l.mu.Unlock()
			if len(batch) == 0 {
				if closing {
					return
				}
				break
			}
			// The store bounds its own transaction; client cancellation must
			// not abandon a write that other calls are waiting on.
			err := l.store.Save(context.Background(), merge(batch))
			if err != nil {
				// Mark the scheduler failed before the failed write becomes
				// visible here: a call that sees it must also find every later
				// call refused, not answered from memory ahead of the store.
				l.onFail(err)
			}
			l.mu.Lock()
			l.writes++
			if err != nil {
				l.err = err
			} else {
				l.durable = end
			}
			close(l.flushed)
			l.flushed = make(chan struct{})
			l.mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// merge combines consecutive changes into one: the last version of each row
// and every event in order. Rows are copied on first write and never
// modified after commit, so the merged change can share them.
func merge(changes []*Change) *Change {
	if len(changes) == 1 {
		return changes[0]
	}
	merged := &Change{}
	executions, attempts := map[string]int{}, map[string]int{}
	for _, c := range changes {
		for _, e := range c.Executions {
			if i, ok := executions[e.ID]; ok {
				merged.Executions[i] = e
			} else {
				executions[e.ID] = len(merged.Executions)
				merged.Executions = append(merged.Executions, e)
			}
		}
		for _, a := range c.Attempts {
			if i, ok := attempts[a.ID]; ok {
				merged.Attempts[i] = a
			} else {
				attempts[a.ID] = len(merged.Attempts)
				merged.Attempts = append(merged.Attempts, a)
			}
		}
		merged.Events = append(merged.Events, c.Events...)
	}
	return merged
}
