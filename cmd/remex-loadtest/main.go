// Command remex-loadtest drives the scheduler's control plane in process: N
// synthetic workers loop Lease → Heartbeat(RUNNING) → Heartbeat(UPLOADING) →
// Complete against a ledger that already holds H finished executions, while
// a probe worker heartbeats a long-running action every 100 ms.
//
// It reports completed actions per second, the probe's heartbeat latency
// (and how many heartbeats took over 1 s), attempts lost: fenced by the
// scheduler because their lease ran out while they waited, and the number
// of ledger changes per store transaction. -stall makes every action-cache
// write take that long, to measure what a slow cache does to everyone else.
// -queue keeps that many runnable executions queued ahead of the workers:
// each worker submits one action and leases the oldest, so the queue stays
// that deep and every Lease has to find its work behind it.
//
// Each record also carries the host, Go and PostgreSQL environment and the
// -commit it was given. It uses only the scheduler's exported API.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"github.com/simon-jd/remex/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// cache is an in-memory CAS and action cache. Writes to the action cache
// take stall each.
type cache struct {
	mu    sync.RWMutex
	blobs map[string][]byte
	stall time.Duration
}

func (c *cache) put(m proto.Message) *re.Digest {
	b, _ := proto.Marshal(m)
	sum := sha256.Sum256(b)
	d := &re.Digest{Hash: fmt.Sprintf("%x", sum), SizeBytes: int64(len(b))}
	c.mu.Lock()
	c.blobs[d.Hash] = b
	c.mu.Unlock()
	return d
}
func (c *cache) ReadBlob(_ context.Context, _ string, d *re.Digest) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if b, ok := c.blobs[d.Hash]; ok {
		return b, nil
	}
	return nil, status.Error(codes.NotFound, "missing")
}
func (c *cache) Missing(context.Context, string, []*re.Digest) ([]*re.Digest, error) { return nil, nil }
func (c *cache) GetActionResult(context.Context, string, *re.Digest) (*re.ActionResult, error) {
	return nil, status.Error(codes.NotFound, "missing")
}
func (c *cache) UpdateActionResult(context.Context, string, *re.Digest, *re.ActionResult) error {
	time.Sleep(c.stall)
	return nil
}

var props = map[string]string{"pool": "cpu"}

type harness struct {
	s     *scheduler.Scheduler
	c     *cache
	ctx   context.Context
	seq   atomic.Int64
	empty []byte
	lost  atomic.Int64
}

var errLost = fmt.Errorf("attempt fenced")

func (h *harness) submit() error {
	n := h.seq.Add(1)
	action := &re.Action{
		CommandDigest:   h.c.put(&re.Command{Arguments: []string{"/bin/true", fmt.Sprint(n)}}),
		InputRootDigest: h.c.put(&re.Directory{}),
		Platform:        &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "cpu"}}},
	}
	_, err := h.s.Start(h.ctx, &re.ExecuteRequest{ActionDigest: h.c.put(action), SkipCacheLookup: true})
	return err
}

// run executes one leased action end to end. It reports false if no work was available.
func (h *harness) run(worker string) (bool, error) {
	lease, err := h.s.Lease(h.ctx, &w.LeaseRequest{WorkerId: worker, Properties: props})
	if err != nil || !lease.Available {
		return false, err
	}
	for _, phase := range []w.HeartbeatRequest_Phase{w.HeartbeatRequest_RUNNING, w.HeartbeatRequest_UPLOADING} {
		r, err := h.s.Heartbeat(h.ctx, &w.HeartbeatRequest{WorkerId: worker, AttemptId: lease.AttemptId, Epoch: lease.Epoch, Phase: phase})
		if err != nil {
			return true, fmt.Errorf("heartbeat: %v", err)
		}
		if r.Decision != w.HeartbeatResponse_CONTINUE {
			h.lost.Add(1)
			return true, errLost
		}
	}
	resp, err := h.s.Complete(h.ctx, &w.CompleteRequest{WorkerId: worker, AttemptId: lease.AttemptId, Epoch: lease.Epoch,
		ActionResult: h.empty, CgroupEmpty: true, AttemptDir: "/attempts/" + lease.AttemptId})
	if err != nil {
		return true, fmt.Errorf("complete: %v", err)
	}
	if resp.Decision != w.CompleteResponse_ACCEPTED {
		h.lost.Add(1)
		return true, errLost
	}
	return true, nil
}

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, int(q*float64(len(sorted))))]
}

func main() {
	workers := flag.Int("workers", 64, "synthetic workers")
	history := flag.Int("history", 0, "finished executions in the ledger before measuring")
	duration := flag.Duration("duration", 20*time.Second, "measurement window")
	stall := flag.Duration("stall", 0, "latency of every action-cache write")
	dsn := flag.String("postgres", "", "PostgreSQL DSN; empty uses the in-memory store")
	label := flag.String("label", "", "free-form label copied into the output")
	queue := flag.Int("queue", 0, "runnable executions kept queued ahead of the workers")
	run := flag.Int("run", 0, "repetition number copied into the output")
	commit := flag.String("commit", "", "source commit of this build, copied into the output")
	extra := settings{}
	flag.Var(extra, "env", "key=value added to the recorded environment (repeatable)")
	flag.Parse()
	ctx := context.Background()
	var store scheduler.Store = scheduler.NewMemoryStore()
	if *dsn != "" {
		pg, err := scheduler.OpenPostgres(ctx, *dsn)
		if err != nil {
			log.Fatal(err)
		}
		store = pg
	}
	c := &cache{blobs: map[string][]byte{}}
	s, err := scheduler.New(ctx, store, c, scheduler.Config{LeaseDuration: 30 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	empty, _ := proto.Marshal(&re.ActionResult{})
	if len(empty) == 0 {
		empty = []byte{0x20, 0} // exit_code 0, explicitly
	}
	h := &harness{s: s, c: c, ctx: ctx, empty: empty}
	register := func(id string) {
		if _, err := s.Register(ctx, &w.RegisterRequest{WorkerId: id, Properties: props}); err != nil {
			log.Fatal(err)
		}
	}
	for i := range *workers {
		register(fmt.Sprintf("worker-%d", i))
	}
	register("probe")
	tick, stopTick := context.WithCancel(ctx)
	go s.Run(tick, nil)

	// Fill the ledger with finished history through the same API.
	for done := 0; done < *history; {
		if err := h.submit(); err != nil {
			log.Fatal(err)
		}
		if ok, err := h.run("worker-0"); err != nil {
			log.Fatal(err)
		} else if ok {
			done++
		}
	}
	c.stall = *stall
	for range *queue {
		if err := h.submit(); err != nil {
			log.Fatal(err)
		}
	}

	// The probe holds one long-running action and heartbeats it every 100 ms.
	if err := h.submit(); err != nil {
		log.Fatal(err)
	}
	probeLease, err := s.Lease(ctx, &w.LeaseRequest{WorkerId: "probe", Properties: props})
	if err != nil || !probeLease.Available {
		log.Fatalf("probe lease: %v %v", probeLease, err)
	}
	stop := make(chan struct{})
	var probe []time.Duration
	probeLost := false
	var probeDone sync.WaitGroup
	probeDone.Add(1)
	go func() {
		defer probeDone.Done()
		phase := w.HeartbeatRequest_RUNNING
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
			started := time.Now()
			r, err := s.Heartbeat(ctx, &w.HeartbeatRequest{WorkerId: "probe", AttemptId: probeLease.AttemptId, Epoch: probeLease.Epoch, Phase: phase})
			probe = append(probe, time.Since(started))
			if err != nil {
				log.Fatalf("probe heartbeat: %v", err)
			}
			if r.Decision != w.HeartbeatResponse_CONTINUE {
				probeLost = true
				return
			}
		}
	}()

	var completed atomic.Int64
	var group sync.WaitGroup
	changesBefore, writesBefore := s.CommitStats()
	deadline := time.Now().Add(*duration)
	for i := range *workers {
		group.Add(1)
		go func(worker string) {
			defer group.Done()
			for time.Now().Before(deadline) {
				if err := h.submit(); err != nil {
					log.Fatal(err)
				}
				ok, err := h.run(worker)
				if err == errLost {
					continue
				}
				if err != nil {
					log.Fatal(err)
				}
				if ok {
					completed.Add(1)
				} else {
					time.Sleep(time.Millisecond)
				}
			}
		}(fmt.Sprintf("worker-%d", i))
	}
	started := time.Now()
	group.Wait()
	elapsed := time.Since(started)
	changesAfter, writesAfter := s.CommitStats()
	close(stop)
	probeDone.Wait()
	stopTick()

	sort.Slice(probe, func(i, j int) bool { return probe[i] < probe[j] })
	over := 0
	for _, d := range probe {
		if d > time.Second {
			over++
		}
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	out := map[string]any{
		"label":       *label,
		"run":         *run,
		"commit":      *commit,
		"environment": environment(ctx, *dsn, extra),
		"store":       map[bool]string{true: "postgres", false: "memory"}[*dsn != ""],
		"workers":     *workers,
		"history":     *history,
		"queue":       *queue,
		"stall_ms":    ms(*stall),
		"seconds":     elapsed.Seconds(),

		"actions_per_second": float64(completed.Load()) / elapsed.Seconds(),
		"heartbeat_ms": map[string]float64{
			"p50": ms(percentile(probe, 0.5)),
			"p99": ms(percentile(probe, 0.99)),
			"max": ms(probe[len(probe)-1]),
		},
		"heartbeats":         len(probe),
		"heartbeats_over_1s": over,
		"attempts_lost":      h.lost.Load(),
		"probe_lost":         probeLost,
		// Ledger changes per store transaction: the group-commit batch size.
		"changes_per_write": float64(changesAfter-changesBefore) / float64(max(writesAfter-writesBefore, 1)),
	}
	_ = json.NewEncoder(os.Stdout).Encode(out)
}
