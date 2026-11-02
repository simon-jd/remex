package scheduler

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

var benchmarkClonedLedger *Ledger

func benchmarkLedger() *Ledger {
	l := NewLedger()
	now := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 600; i++ {
		e := &Execution{
			ID:            fmt.Sprintf("e-%d", i),
			InvocationIDs: []string{"build-1"},
			Platform:      map[string]string{"pool": "rtl", "image": "sha256:image"},
			Action:        make([]byte, 256),
			Command:       make([]byte, 2048),
			Result:        make([]byte, 16<<10),
			State:         "Published",
			CurrentEpoch:  2,
		}
		l.Executions = append(l.Executions, e)
		for epoch := int64(1); epoch <= 2; epoch++ {
			a := &Attempt{
				ID:               fmt.Sprintf("a-%d-%d", i, epoch),
				ExecutionID:      e.ID,
				Epoch:            epoch,
				WorkerPlatform:   map[string]string{"pool": "rtl", "cpu": "8"},
				CommandStartedAt: &now,
				Result:           make([]byte, 16<<10),
				ResultDigests: []Digest{
					{Hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", SizeBytes: 4096},
				},
			}
			l.Attempts = append(l.Attempts, a)
			for _, kind := range []string{
				"lease_granted",
				"command_started",
				"uploading",
				"cas_verified",
				"attempt_committed",
				"ac_published",
			} {
				l.event(
					now,
					kind,
					e,
					a,
					map[string]any{"result_digests": a.ResultDigests, "worker_platform": a.WorkerPlatform},
				)
			}
		}
	}
	return l
}
func BenchmarkLedgerClone600Executions(b *testing.B) {
	ledger := benchmarkLedger()
	b.Run("json_roundtrip", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			raw, _ := json.Marshal(ledger)
			var cloned Ledger
			_ = json.Unmarshal(raw, &cloned)
			benchmarkClonedLedger = &cloned
		}
	})
	b.Run("explicit_copy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkClonedLedger = ledger.clone()
		}
	})
}
