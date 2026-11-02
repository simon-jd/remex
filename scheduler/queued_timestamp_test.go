package scheduler

import (
	"testing"
	"time"
)

func TestLeasePreservesOriginalQueuedTimestamp(t *testing.T) {
	f := setup(t, false)
	id := f.start(t, true)
	queued := f.s.Export().execution(id).CreatedAt
	f.now = f.now.Add(250 * time.Millisecond)
	first := f.lease(t, "cpu1")
	if first.QueuedTimestamp == nil || !first.QueuedTimestamp.AsTime().Equal(queued) {
		t.Fatalf("first lease lost durable enqueue time: %v", first.QueuedTimestamp)
	}

	// Recovery must not turn the enqueue time into the restart time. After the
	// recovered lease expires, its successor retains the execution's original
	// timestamp rather than pretending to describe only its latest queue wait.
	recovered, err := New(
		f.ctx,
		f.store,
		f.c,
		Config{RecoveryGrace: time.Second, LeaseDuration: time.Second, Now: func() time.Time { return f.now }},
	)
	if err != nil {
		t.Fatal(err)
	}
	f.s = recovered
	f.now = f.now.Add(2 * time.Second)
	f.register(t, "cpu2", map[string]string{"pool": "cpu"})
	second := f.lease(t, "cpu2")
	if second.Epoch != first.Epoch+1 ||
		second.QueuedTimestamp == nil ||
		!second.QueuedTimestamp.AsTime().Equal(queued) {
		t.Fatalf("recovered lease changed original enqueue time: %v", second)
	}
}
