package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	w "github.com/simon-jd/remex/api/worker/v1"
)

// These synthetic histories measure control-plane work only. They are not
// build throughput or remote-execution correctness evidence.
func BenchmarkIdleHistory(b *testing.B) {
	for _, count := range []int{100, 5000} {
		for _, operation := range []string{"lease", "tick"} {
			b.Run(fmt.Sprintf("%s/%d", operation, count), func(b *testing.B) {
				ctx := context.Background()
				s, err := New(ctx, NewMemoryStore(), newCache(), Config{})
				if err != nil {
					b.Fatal(err)
				}
				for i := 0; i < count; i++ {
					id, aid := fmt.Sprintf("execution-%d", i), fmt.Sprintf("attempt-%d", i)
					s.ledger.Executions = append(s.ledger.Executions, &Execution{ID: id, State: "Committed", CurrentEpoch: 1, WinnerAttemptID: aid, DoNotCache: true, CreatedAt: time.Now()})
					s.ledger.Attempts = append(s.ledger.Attempts, &Attempt{ID: aid, ExecutionID: id, Epoch: 1, WorkerID: "cpu1", Phase: "Committed", CgroupEmpty: true})
				}
				properties := map[string]string{"pool": "cpu"}
				if _, err := s.Register(ctx, &w.RegisterRequest{WorkerId: "cpu1", Properties: properties}); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if operation == "lease" {
						response, err := s.Lease(ctx, &w.LeaseRequest{WorkerId: "cpu1", Properties: properties})
						if err != nil || response.Available {
							b.Fatalf("idle Lease: %v %v", response, err)
						}
					} else if err := s.Tick(ctx); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
