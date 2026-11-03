package scheduler

// eventChunkLen is the number of events in one chunk of an eventLog:
// 4,096 events of 96 bytes, 384 KiB.
const eventChunkLen = 4096

// eventLog holds events in fixed-size chunks. An append never moves an event
// that is already in the log, so it costs only the events it adds however
// long the log is. A single slice would instead copy every event each time
// it outgrew its capacity: at 4 million events that is about 300 MB, copied
// while the scheduler lock is held.
type eventLog struct {
	chunks [][]Event // every chunk but the last is full
	n      int
}

func (l *eventLog) len() int { return l.n }

func (l *eventLog) append(events []Event) {
	for len(events) > 0 {
		last := len(l.chunks) - 1
		if last < 0 || len(l.chunks[last]) == eventChunkLen {
			l.chunks = append(l.chunks, make([]Event, 0, eventChunkLen))
			last++
		}
		n := min(len(events), eventChunkLen-len(l.chunks[last]))
		l.chunks[last] = append(l.chunks[last], events[:n]...)
		events = events[n:]
		l.n += n
	}
}
