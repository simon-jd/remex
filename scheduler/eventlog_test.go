package scheduler

import (
	"testing"
	"time"
)

// A ledger loaded with events keeps them in Events; commits append after
// them in chunks, never move an event already appended, and clone returns
// the whole sequence in order with its own copy of every event's details.
func TestEventLogAppendsWithoutMovingEvents(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	l := NewLedger()
	for range 3 {
		l.event(now, "loaded", nil, nil, nil)
	}
	var first *Event
	for _, n := range []int{1, eventChunkLen - 2, 2, 2*eventChunkLen + 5} {
		t1 := l.begin()
		for range n {
			t1.event(now, "committed", nil, nil, map[string]any{"n": n})
		}
		l.apply(&t1.change)
		if first == nil {
			first = &l.history.chunks[0][0]
		}
	}
	l.event(now, "after_commits", nil, nil, nil)
	want := 3 + 1 + eventChunkLen - 2 + 2 + 2*eventChunkLen + 5 + 1
	if l.eventCount() != want || len(l.Events) != 3 {
		t.Fatalf("events = %d (%d loaded), want %d (3 loaded)", l.eventCount(), len(l.Events), want)
	}
	if &l.history.chunks[0][0] != first {
		t.Fatal("an append moved events that were already in the log")
	}
	for i, chunk := range l.history.chunks[:len(l.history.chunks)-1] {
		if len(chunk) != eventChunkLen {
			t.Fatalf("chunk %d holds %d events, want %d", i, len(chunk), eventChunkLen)
		}
	}
	c := l.clone()
	if len(c.Events) != want || c.history.len() != 0 {
		t.Fatalf("clone has %d events and %d in history, want %d and 0", len(c.Events), c.history.len(), want)
	}
	for i, ev := range c.Events {
		if ev.Sequence != int64(i+1) {
			t.Fatalf("clone event %d has sequence %d", i, ev.Sequence)
		}
	}
	if c.Events[2].Kind != "loaded" || c.Events[3].Kind != "committed" || c.Events[want-1].Kind != "after_commits" {
		t.Fatalf("clone kinds %q %q %q", c.Events[2].Kind, c.Events[3].Kind, c.Events[want-1].Kind)
	}
	c.Events[3].Details["n"] = -1
	if first.Details["n"] != 1 {
		t.Fatal("clone shares event details with the ledger")
	}
}
