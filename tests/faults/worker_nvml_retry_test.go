package faults_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	w "github.com/simon-jd/remex/api/worker/v1"
	"google.golang.org/grpc"
)

type nvmlObserver struct {
	w.UnimplementedWorkerServiceServer
	mu            sync.Mutex
	registrations []string
	leases        []string
}

func (s *nvmlObserver) Register(_ context.Context, r *w.RegisterRequest) (*w.RegisterResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registrations = append(s.registrations, r.DeviceSnapshotJson)
	return &w.RegisterResponse{LeaseDurationMs: 2000, HeartbeatIntervalMs: 500}, nil
}
func (s *nvmlObserver) Lease(_ context.Context, r *w.LeaseRequest) (*w.LeaseResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leases = append(s.leases, r.DeviceSnapshotJson)
	return &w.LeaseResponse{}, nil
}
func (s *nvmlObserver) observations() ([]string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.registrations...), append([]string(nil), s.leases...)
}

func TestRealWorkerTransientNVMLFailureGatesRPCsAndRecoversFreshIdentity(t *testing.T) {
	binary, parent := os.Getenv("REMEX_TEST_WORKER_BINARY"), os.Getenv("REMEX_TEST_CGROUP_ROOT")
	if binary == "" || parent == "" {
		t.Skip("set REMEX_TEST_WORKER_BINARY and REMEX_TEST_CGROUP_ROOT")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	gate := filepath.Join(dir, "nvml-ready")
	library := filepath.Join(dir, "libnvidia-ml.so.1")
	compile := exec.Command("cc", "-shared", "-fPIC", "-o", library, "fixtures/nvml_transient.c")
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile independent NVML injector: %v\n%s", err, output)
	}
	cgroup := filepath.Join(parent, fmt.Sprintf("independent-nvml-retry-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if err := os.Mkdir(cgroup, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroup, "cgroup.subtree_control"), []byte("+cpu +memory +pids"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(cgroup, "cgroup.kill"), []byte("1"), 0600)
		if err := os.Remove(cgroup); err != nil {
			t.Errorf("NVML test cgroup cleanup: %v", err)
		}
	})
	observer := &nvmlObserver{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	w.RegisterWorkerServiceServer(server, observer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	logPath := filepath.Join(dir, "worker.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	args := []string{"--scheduler", listener.Addr().String(), "--cgroup-root", cgroup, "--root", filepath.Join(dir, "worker"), "--id", "independent-nvml", "--property", "pool=gpu"}
	command := exec.Command(binary, args...)
	command.Env = append(os.Environ(), "LD_LIBRARY_PATH="+dir, "REMEX_TEST_NVML_GATE="+gate)
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		select {
		case <-exited:
		case <-time.After(2 * time.Second):
		}
		data, _ := os.ReadFile(logPath)
		t.Logf("worker synthetic NVML log:\n%s", data)
	})
	wait := func(description string, predicate func() bool) {
		t.Helper()
		end := time.Now().Add(8 * time.Second)
		for time.Now().Before(end) {
			if predicate() {
				return
			}
			select {
			case err := <-exited:
				t.Fatalf("worker exited during %s: %v", description, err)
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("timeout: %s", description)
	}
	wait("initial NVML transient error", func() bool {
		data, _ := os.ReadFile(logPath)
		return strings.Contains(string(data), "NVML initialization failed")
	})
	registrations, leases := observer.observations()
	if len(registrations) != 0 || len(leases) != 0 {
		t.Fatal("worker contacted scheduler before valid NVML identity")
	}
	if err := os.WriteFile(gate, []byte("GPU-independent-first\n"), 0600); err != nil {
		t.Fatal(err)
	}
	wait("first recovered lease", func() bool { _, leases := observer.observations(); return len(leases) > 0 })
	registrations, leases = observer.observations()
	if len(registrations) != 1 || !strings.Contains(registrations[0], "GPU-independent-first") || !strings.Contains(leases[0], "GPU-independent-first") {
		t.Fatalf("first observed identity: %v %v", registrations, leases)
	}
	beforeLog, _ := os.ReadFile(logPath)
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	wait("second NVML outage", func() bool {
		data, _ := os.ReadFile(logPath)
		return strings.Count(string(data), "NVML initialization failed") > strings.Count(string(beforeLog), "NVML initialization failed")
	})
	_, leases = observer.observations()
	pausedCount := len(leases)
	time.Sleep(700 * time.Millisecond)
	_, leases = observer.observations()
	if len(leases) != pausedCount {
		t.Fatalf("NVML outage reused stale identity: leases %d -> %d", pausedCount, len(leases))
	}
	if err := os.WriteFile(gate, []byte("GPU-independent-second\n"), 0600); err != nil {
		t.Fatal(err)
	}
	wait("fresh identity after second recovery", func() bool {
		_, leases := observer.observations()
		return len(leases) > pausedCount && strings.Contains(leases[len(leases)-1], "GPU-independent-second")
	})
	registrations, leases = observer.observations()
	t.Logf("actual production worker, synthetic NVML: registrations=%v leases=%v", registrations, leases)
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Fatal(err)
		}
		exited <- nil
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop promptly during polling")
	}
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	once := exec.Command(binary, append(args, "--once")...)
	once.Env = command.Env
	output, err := once.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "NVML initialization failed") {
		t.Fatalf("--once did not fail clearly: %v %s", err, output)
	}
}
