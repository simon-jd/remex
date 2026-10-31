package faults_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"github.com/simon-jd/remex/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// These opt-in tests launch the actual C++ worker against real bazel-remote and
// a fresh scheduler on an ephemeral loopback port. PostgreSQL is not shared.
type realWorker struct {
	ctx               context.Context
	client            re.ExecutionClient
	cas               re.ContentAddressableStorageClient
	scheduler         *scheduler.Scheduler
	process           *exec.Cmd
	root, cgroup, log string
	exited            chan error
}

func newRealWorker(t *testing.T, memoryLimit bool) *realWorker {
	t.Helper()
	binary, root, endpoint := os.Getenv("REMEX_TEST_WORKER_BINARY"), os.Getenv("REMEX_TEST_CGROUP_ROOT"), os.Getenv("REMEX_TEST_CAS")
	if binary == "" || root == "" || endpoint == "" {
		t.Skip("set REMEX_TEST_WORKER_BINARY, REMEX_TEST_CGROUP_ROOT and REMEX_TEST_CAS for real C++/cgroup integration")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	cgroup := filepath.Join(root, fmt.Sprintf("independent-worker-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if err := os.Mkdir(cgroup, 0755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Cleanup only this test's dedicated subtree, after the assertions.
		_ = os.WriteFile(filepath.Join(cgroup, "cgroup.kill"), []byte("1"), 0600)
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			b, _ := os.ReadFile(filepath.Join(cgroup, "cgroup.events"))
			if strings.Contains(string(b), "populated 0") {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		entries, _ := os.ReadDir(cgroup)
		for _, entry := range entries {
			if entry.IsDir() {
				_ = os.Remove(filepath.Join(cgroup, entry.Name()))
			}
		}
		if err := os.Remove(cgroup); err != nil {
			t.Errorf("test cgroup cleanup: %v", err)
		}
	})
	if err := os.WriteFile(filepath.Join(cgroup, "cgroup.subtree_control"), []byte("+cpu +memory +pids"), 0600); err != nil {
		t.Fatal(err)
	}
	if memoryLimit {
		if err := os.WriteFile(filepath.Join(cgroup, "memory.max"), []byte("67108864"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cgroup, "memory.swap.max"), []byte("0"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	casConn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = casConn.Close() })
	s, err := scheduler.New(ctx, scheduler.NewMemoryStore(), scheduler.NewGRPCCache(casConn), scheduler.Config{CASEndpoint: endpoint, LeaseDuration: 2 * time.Second, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	re.RegisterExecutionServer(server, s)
	re.RegisterCapabilitiesServer(server, s)
	w.RegisterWorkerServiceServer(server, s)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	go s.Run(ctx, func(err error) { t.Logf("scheduler tick: %v", err) })
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	h := &realWorker{ctx: ctx, client: re.NewExecutionClient(conn), cas: re.NewContentAddressableStorageClient(casConn), scheduler: s, root: filepath.Join(dir, "worker"), cgroup: cgroup, log: filepath.Join(dir, "worker.log"), exited: make(chan error, 1)}
	log, err := os.Create(h.log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	h.process = exec.Command(binary, "--scheduler", listener.Addr().String(), "--cas", endpoint, "--cgroup-root", cgroup, "--root", h.root, "--id", "independent-cpp", "--property", "pool=cpu", "--once")
	h.process.Stdout, h.process.Stderr = log, log
	if err := h.process.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { h.exited <- h.process.Wait() }()
	t.Cleanup(func() {
		_ = h.process.Process.Kill()
		select {
		case <-h.exited:
		case <-time.After(2 * time.Second):
		}
		b, _ := os.ReadFile(h.log)
		t.Logf("C++ worker log:\n%s", b)
	})
	for {
		b, _ := os.ReadFile(h.log)
		if strings.Contains(string(b), "registered worker=") {
			break
		}
		select {
		case err := <-h.exited:
			t.Fatalf("worker exited before registration: %v\n%s", err, b)
		case <-ctx.Done():
			t.Fatalf("worker registration timeout: %s", b)
		case <-time.After(10 * time.Millisecond):
		}
	}
	return h
}

func (h *realWorker) put(t *testing.T, message proto.Message) *re.Digest {
	t.Helper()
	b, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	d := &re.Digest{Hash: fmt.Sprintf("%x", sum), SizeBytes: int64(len(b))}
	r, err := h.cas.BatchUpdateBlobs(h.ctx, &re.BatchUpdateBlobsRequest{InstanceName: "independent-worker-tests", DigestFunction: re.DigestFunction_SHA256, Requests: []*re.BatchUpdateBlobsRequest_Request{{Digest: d, Data: b}}})
	if err != nil || len(r.GetResponses()) != 1 || r.Responses[0].GetStatus().GetCode() != 0 {
		t.Fatalf("upload = %v, %v", r, err)
	}
	return d
}
func (h *realWorker) execute(t *testing.T, argv []string, timeout time.Duration) re.Execution_ExecuteClient {
	t.Helper()
	d := h.put(t, &re.Action{CommandDigest: h.put(t, &re.Command{Arguments: argv}), InputRootDigest: h.put(t, &re.Directory{}), Timeout: durationpb.New(timeout), DoNotCache: true, Platform: &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "cpu"}}}})
	stream, err := h.client.Execute(h.ctx, &re.ExecuteRequest{InstanceName: "independent-worker-tests", ActionDigest: d, SkipCacheLookup: true})
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func TestRealWorkerResultClassification(t *testing.T) {
	for _, tc := range []struct {
		name    string
		argv    []string
		timeout time.Duration
		code    codes.Code
		oom     bool
		exit    int32
	}{
		{"success", []string{"/bin/sh", "-c", "printf independent-success"}, 5 * time.Second, codes.OK, false, 0},
		{"timeout", []string{"/bin/bash", "-c", "set -m; sleep 60 & wait"}, 100 * time.Millisecond, codes.DeadlineExceeded, false, 0},
		{"oom", []string{"/usr/bin/python3", "-c", "x=bytearray(256*1024*1024)"}, 5 * time.Second, codes.ResourceExhausted, true, 0},
		{"missing_executable", []string{"/remex-independent-missing-executable"}, 5 * time.Second, codes.Internal, false, 0},
		{"real_exit_127", []string{"/bin/sh", "-c", "exit 127"}, 5 * time.Second, codes.OK, false, 127},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRealWorker(t, tc.oom)
			stream := h.execute(t, tc.argv, tc.timeout)
			for {
				op, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if !op.Done {
					continue
				}
				var response re.ExecuteResponse
				if err := op.GetResponse().UnmarshalTo(&response); err != nil {
					t.Fatal(err)
				}
				if codes.Code(response.GetStatus().GetCode()) != tc.code {
					t.Fatalf("status = %s (%s); expected %s", codes.Code(response.GetStatus().GetCode()), response.GetStatus().GetMessage(), tc.code)
				}
				if tc.code == codes.OK && response.GetResult().GetExitCode() != tc.exit {
					t.Fatalf("command exit = %d, expected %d", response.GetResult().GetExitCode(), tc.exit)
				}
				break
			}
			ledger := h.scheduler.Export()
			if len(ledger.Attempts) != 1 || !ledger.Attempts[0].CgroupEmpty {
				t.Fatalf("missing empty cgroup evidence: %+v", ledger.Attempts)
			}
			if tc.oom && ledger.Attempts[0].OOMKill < 1 {
				t.Fatal("OOM classification lacks kernel oom_kill evidence")
			}
			t.Logf("status=%s oom_kill=%d resources=%s", ledger.Attempts[0].Status, ledger.Attempts[0].OOMKill, ledger.Attempts[0].ResourceUsageJSON)
		})
	}
}

func TestRealWorkerSIGKILLDoesNotLeaveActionProcess(t *testing.T) {
	h := newRealWorker(t, false)
	stream := h.execute(t, []string{"/bin/sh", "-c", "echo $$ > running.pid; exec /bin/sleep 60"}, 10*time.Second)
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	for {
		files, _ := filepath.Glob(filepath.Join(h.root, "attempts", "*", "root", "running.pid"))
		if len(files) == 1 {
			break
		}
		select {
		case <-h.ctx.Done():
			t.Fatal("action never started")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := h.process.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		b, err := os.ReadFile(filepath.Join(h.cgroup, "cgroup.events"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "populated 0") {
			t.Log("SIGKILL worker: dedicated action subtree reached populated 0 without test intervention")
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("SIGKILL left action process alive after 3s: %s", b)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRealWorkerSIGSTOPThenCONTAbandonsExpiredLease(t *testing.T) {
	h := newRealWorker(t, false)
	stream := h.execute(t, []string{"/bin/sh", "-c", "echo $$ > running.pid; exec /bin/sleep 60"}, 10*time.Second)
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	for {
		files, _ := filepath.Glob(filepath.Join(h.root, "attempts", "*", "root", "running.pid"))
		if len(files) == 1 {
			break
		}
		select {
		case <-h.ctx.Done():
			t.Fatal("action never started")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := h.process.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	// Wall-time wait intentionally exceeds the real scheduler's two-second lease.
	time.Sleep(2600 * time.Millisecond)
	if err := h.process.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	for {
		op, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if !op.Done {
			continue
		}
		var response re.ExecuteResponse
		if err := op.GetResponse().UnmarshalTo(&response); err != nil {
			t.Fatal(err)
		}
		if codes.Code(response.GetStatus().GetCode()) != codes.Unavailable {
			t.Fatalf("expired paused-worker status = %v", response.GetStatus())
		}
		break
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		b, err := os.ReadFile(filepath.Join(h.cgroup, "cgroup.events"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "populated 0") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("resumed worker retained action after lease expiry: %s", b)
		}
		time.Sleep(10 * time.Millisecond)
	}
	ledger := h.scheduler.Export()
	if len(ledger.Executions) != 1 || ledger.Executions[0].WinnerAttemptID != "" || ledger.Attempts[0].Published {
		t.Fatal("expired paused worker committed or published a result")
	}
	t.Log("SIGSTOP/SIGCONT after lease expiry: no winner, infrastructure status, populated 0")
}
