package main

import (
	"runtime/metrics"
	"sort"
)

// slowHeartbeat is one probe heartbeat that took over 10 ms. EventGrowth
// reports that a commit reallocated the scheduler's event log while it
// waited; GCCycles counts the GC cycles that ended in the same interval.
// Neither read takes the scheduler lock.
type slowHeartbeat struct {
	MS          float64 `json:"ms"`
	AtS         float64 `json:"at_s"`
	Completed   int64   `json:"completed"`
	Events      int64   `json:"ledger_events"`
	EventGrowth bool    `json:"event_log_grew"`
	GCCycles    uint64  `json:"gc_cycles"`
}

func summarizeSlow(slow []slowHeartbeat) map[string]any {
	grew, gc := 0, 0
	for _, h := range slow {
		if h.EventGrowth {
			grew++
		}
		if h.GCCycles > 0 {
			gc++
		}
	}
	sort.Slice(slow, func(i, j int) bool { return slow[i].MS > slow[j].MS })
	return map[string]any{
		"over_10ms":           len(slow),
		"during_event_growth": grew,
		"during_gc":           gc,
		"slowest":             slow[:min(len(slow), 5)],
	}
}

func gcCycles() uint64 {
	sample := []metrics.Sample{{Name: "/gc/cycles/total:gc-cycles"}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

// gcState is a snapshot of the runtime's GC counters.
type gcState struct {
	cycles uint64
	pauses *metrics.Float64Histogram
	gogc   uint64
	heap   uint64
}

func readGC() gcState {
	samples := []metrics.Sample{
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/sched/pauses/total/gc:seconds"},
		{Name: "/gc/gogc:percent"},
		{Name: "/memory/classes/heap/objects:bytes"},
	}
	metrics.Read(samples)
	return gcState{
		cycles: samples[0].Value.Uint64(),
		pauses: samples[1].Value.Float64Histogram(),
		gogc:   samples[2].Value.Uint64(),
		heap:   samples[3].Value.Uint64(),
	}
}

// since reports the GC cycles and stop-the-world GC pauses between start
// and s. The histogram gives each pause's bucket, so the longest pause is
// reported as its bucket's upper bound and the total from bucket midpoints.
func (s gcState) since(start gcState) map[string]any {
	pauses, total, longest := uint64(0), 0.0, 0.0
	for i, count := range s.pauses.Counts {
		n := count - start.pauses.Counts[i]
		if n == 0 {
			continue
		}
		lo, hi := s.pauses.Buckets[i], s.pauses.Buckets[i+1]
		if lo < 0 || lo != lo {
			lo = 0
		}
		if hi > 1e9 {
			hi = lo
		}
		pauses += n
		total += float64(n) * (lo + hi) / 2
		longest = hi
	}
	return map[string]any{
		"cycles":           s.cycles - start.cycles,
		"pauses":           pauses,
		"pause_total_ms":   total * 1000,
		"pause_max_ms_le":  longest * 1000,
		"gogc_percent":     gogc(s.gogc),
		"heap_objects_mib": float64(s.heap) / (1 << 20),
	}
}

// gogc reports GOGC=off as -1 rather than the runtime's MaxUint64.
func gogc(percent uint64) int64 {
	if percent > 1<<31 {
		return -1
	}
	return int64(percent)
}
