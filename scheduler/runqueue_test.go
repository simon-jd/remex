package scheduler

import (
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
)

func (f *fixture) leaseAs(t *testing.T, worker, pool string) *w.LeaseResponse {
	t.Helper()
	r, err := f.s.Lease(f.ctx, &w.LeaseRequest{WorkerId: worker, Properties: map[string]string{"pool": pool}})
	if err != nil || !r.Available {
		t.Fatalf("lease for %s = %v, %v", worker, r, err)
	}
	return r
}

func TestRunQueuesOldestMatchingAndLazyRemoval(t *testing.T) {
	qs := runQueues{}
	cpu, gpu := map[string]string{"pool": "cpu"}, map[string]string{"pool": "gpu"}
	for _, position := range []int{5, 1, 3} {
		qs.push(&Execution{Platform: cpu}, position)
	}
	qs.push(&Execution{Platform: gpu}, 2)
	qs.push(&Execution{Platform: cpu}, 3) // runnable again after a retry
	done := map[int]bool{}
	runnable := func(position int) bool { return !done[position] }
	if got := qs.oldest(cpu, runnable); got != 1 {
		t.Fatalf("oldest cpu = %d, want 1", got)
	}
	if got := qs.oldest(map[string]string{"pool": "rtl"}, runnable); got != -1 {
		t.Fatalf("oldest rtl = %d, want none", got)
	}
	done[1] = true
	if got := qs.oldest(cpu, runnable); got != 3 {
		t.Fatalf("oldest cpu after leasing 1 = %d, want 3", got)
	}
	if got := qs.oldest(gpu, runnable); got != 2 {
		t.Fatalf("oldest gpu = %d, want 2", got)
	}
	done[2], done[3], done[5] = true, true, true
	if got := qs.oldest(cpu, runnable); got != -1 || len(qs) != 0 {
		t.Fatalf("oldest = %d with %d queues left, want none and no queues", got, len(qs))
	}
}

// Each worker gets the oldest execution it can run, whatever the other
// platforms hold, and a retry keeps its place ahead of newer work.
func TestLeaseTakesOldestMatchingAcrossPlatforms(t *testing.T) {
	f := setup(t, true)
	f.register(t, "rtl1", map[string]string{"pool": "rtl"})
	rtlAction := f.c.putProto(&re.Action{
		CommandDigest:   f.c.putProto(&re.Command{Arguments: []string{"/bin/true"}}),
		InputRootDigest: f.c.putProto(&re.Directory{}),
		Platform:        &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "rtl"}}},
		DoNotCache:      true,
	})
	var cpu, rtl []string
	for range 3 {
		id, err := f.s.Start(f.ctx, &re.ExecuteRequest{ActionDigest: rtlAction, SkipCacheLookup: true})
		if err != nil {
			t.Fatal(err)
		}
		rtl = append(rtl, id)
		cpu = append(cpu, f.start(t, true))
	}
	if r := f.lease(t, "cpu1"); r.ExecutionId != cpu[0] {
		t.Fatalf("cpu1 leased %s, want %s", r.ExecutionId, cpu[0])
	}
	if r := f.leaseAs(t, "rtl1", "rtl"); r.ExecutionId != rtl[0] {
		t.Fatalf("rtl1 leased %s, want %s", r.ExecutionId, rtl[0])
	}
	f.now = f.now.Add(time.Second) // both leases expire
	f.register(t, "cpu2", map[string]string{"pool": "cpu"})
	if r := f.lease(t, "cpu2"); r.ExecutionId != cpu[0] || r.Epoch != 2 {
		t.Fatalf("cpu2 leased %s at epoch %d, want the retried %s at 2", r.ExecutionId, r.Epoch, cpu[0])
	}
	if r := f.lease(t, "cpu1"); r.ExecutionId != cpu[1] {
		t.Fatalf("cpu1 leased %s, want %s", r.ExecutionId, cpu[1])
	}
	if r := f.leaseAs(t, "rtl1", "rtl"); r.ExecutionId != rtl[0] || r.Epoch != 2 {
		t.Fatalf("rtl1 leased %s at epoch %d, want the retried %s at 2", r.ExecutionId, r.Epoch, rtl[0])
	}
}
