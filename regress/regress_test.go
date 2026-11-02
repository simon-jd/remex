package regress

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestFailureRateLowerBound(t *testing.T) {
	if got := FailureRateLowerBound(1, 1, .05); math.Abs(got-.05) > 1e-10 {
		t.Fatalf("exact 1/1 lower bound=%v", got)
	}
	if got := FailureRateLowerBound(10, 10, .05); math.Abs(got-math.Pow(.05, .1)) > 1e-10 {
		t.Fatalf("exact 10/10 lower bound=%v", got)
	}
	if got := FailureRateLowerBound(0, 20, .05); got != 0 {
		t.Fatal(got)
	}
	if got := FailureRateLowerBound(5, 20, .05); got <= 0 || got >= .25 {
		t.Fatal(got)
	}
}
func TestCalibrationStopsAtTheTargetFailureCount(t *testing.T) {
	r, err := Bisect(context.Background(), BisectConfig{Revisions: []string{"a", "b"}, Budget: 500}, RunnerFunc(func(_ context.Context, rev string, _ int) (Observation, error) {
		if rev == "b" {
			return Observation{Category: TestFailure, Signature: "bug"}, nil
		}
		return Observation{Category: Pass}, nil
	}))
	if err != nil || r.Conclusion != ConclusionCulprit {
		t.Fatalf("%+v %v", r, err)
	}
	if n := len(r.Probes[0].Observations); n != 10 {
		t.Fatalf("a deterministic failure calibrated with %d runs, want 10", n)
	}
	if want := math.Pow(.05, .1); math.Abs(r.FailureRateLowerBound-want) > 1e-9 {
		t.Fatalf("bound %v, want %v (10 failures in 10 runs)", r.FailureRateLowerBound, want)
	}
}
func TestRareFlakeBisectsWithinBudget(t *testing.T) {
	revisions := make([]string, 32)
	for i := range revisions {
		revisions[i] = fmt.Sprintf("r%02d", i)
	}
	rng := rand.New(rand.NewPCG(7, 7))
	r, err := Bisect(context.Background(), BisectConfig{Revisions: revisions, Budget: 5000}, RunnerFunc(func(_ context.Context, rev string, _ int) (Observation, error) {
		if rev >= "r19" && rng.Float64() < 0.1 {
			return Observation{Category: TestFailure, Signature: "bug"}, nil
		}
		return Observation{Category: Pass}, nil
	}))
	if err != nil || r.Conclusion != ConclusionCulprit || r.Culprit != "r19" {
		t.Fatalf("10%% flake: %s %s after %d runs (%s)", r.Conclusion, r.Culprit, r.Runs, r.Reason)
	}
}
func TestBisectConfirmsAdjacentBoundary(t *testing.T) {
	revisions := []string{"a", "b", "c", "d", "e"}
	runs := map[string]int{}
	runner := RunnerFunc(func(_ context.Context, rev string, seed int) (Observation, error) {
		runs[rev]++
		if rev >= "c" {
			return Observation{Category: TestFailure, Signature: "bug"}, nil
		}
		return Observation{Category: Pass}, nil
	})
	r, err := Bisect(context.Background(), BisectConfig{Revisions: revisions, Budget: 200}, runner)
	if err != nil {
		t.Fatal(err)
	}
	if r.Conclusion != ConclusionCulprit || r.Culprit != "c" {
		t.Fatalf("%+v", r)
	}
	if r.Probes[len(r.Probes)-1].Purpose != PurposeConfirmCulprit || len(r.Probes[len(r.Probes)-1].Observations) != 2*r.K || len(r.Probes[len(r.Probes)-2].Observations) != 2*r.K {
		t.Fatal("missing full confirmation")
	}
}
func TestBisectBudgetInfraAndSignature(t *testing.T) {
	t.Run("infra-not-bad", func(t *testing.T) {
		r, err := Bisect(context.Background(), BisectConfig{Revisions: []string{"a", "b"}, Budget: 7}, RunnerFunc(func(context.Context, string, int) (Observation, error) {
			return Observation{Category: Infrastructure}, nil
		}))
		if err != nil || r.Conclusion != ConclusionInconclusive || r.Runs != 7 {
			t.Fatalf("%+v %v", r, err)
		}
	})
	t.Run("unreproducible", func(t *testing.T) {
		r, _ := Bisect(context.Background(), BisectConfig{Revisions: []string{"a", "b"}}, RunnerFunc(func(context.Context, string, int) (Observation, error) { return Observation{Category: Pass}, nil }))
		if r.Conclusion != ConclusionInconclusive {
			t.Fatal(r)
		}
	})
	t.Run("different-signature-range", func(t *testing.T) {
		r, _ := Bisect(context.Background(), BisectConfig{Revisions: []string{"a", "b", "c"}}, RunnerFunc(func(_ context.Context, rev string, _ int) (Observation, error) {
			switch rev {
			case "a":
				return Observation{Category: Pass}, nil
			case "b":
				return Observation{Category: TestFailure, Signature: "other"}, nil
			default:
				return Observation{Category: TestFailure, Signature: "target"}, nil
			}
		}))
		if r.Conclusion != ConclusionRange || len(r.Range) != 2 {
			t.Fatal(r)
		}
	})
	t.Run("confirmation-fails", func(t *testing.T) {
		n := 0
		// b fails only while it is being calibrated, so its confirmation fails.
		r, _ := Bisect(context.Background(), BisectConfig{Revisions: []string{"a", "b"}, CalibrationFailures: 10}, RunnerFunc(func(_ context.Context, rev string, _ int) (Observation, error) {
			if rev == "b" {
				n++
				if n <= 10 {
					return Observation{Category: TestFailure, Signature: "target"}, nil
				}
			}
			return Observation{Category: Pass}, nil
		}))
		if r.Conclusion != ConclusionInconclusive {
			t.Fatal(r)
		}
	})
}
func TestBEPClassificationUsesEvidence(t *testing.T) {
	bep := `{"id":{"testResult":{"label":"//:test","run":1,"attempt":1}},"testResult":{"status":"FAILED"}}
{"id":{"testSummary":{"label":"//:test"}},"testSummary":{"overallStatus":"FLAKY"}}
`
	results, err := ReadBEP(strings.NewReader(bep))
	if err != nil || len(results) != 1 {
		t.Fatal(results, err)
	}
	if got := Classify(results, nil, ""); got.Category != TestFailure || got.Signature != "" {
		t.Fatal(got)
	}
	if got := Classify(results, nil, "explicit-failure"); got.Signature != "explicit-failure" {
		t.Fatal(got)
	}
	results[0].FailureText = "assertion expected 2 got 1"
	first := Classify(results, nil, "")
	results[0].FailureText = "different assertion"
	if second := Classify(results, nil, ""); first.Signature == second.Signature || first.Signature == "" {
		t.Fatal("distinct test failures merged")
	}
	if got := Classify(results, []AttemptEvidence{{ID: "oom", Status: "OK", OOMKill: 1}}, ""); got.Category != Infrastructure {
		t.Fatal(got)
	}
	if got := Classify(nil, []AttemptEvidence{{ExitCode: 1, Status: "OK"}}, ""); got.Category != Unknown {
		t.Fatal(got)
	}
	if got := Classify([]BEPResult{{Label: "//:test", Status: "PASSED", Cached: true}}, nil, ""); got.Category != Unknown {
		t.Fatal(got)
	}
}
func TestHardwareSelectionConservativeFallback(t *testing.T) {
	r := SelectionRequest{Targets: []HardwareTarget{{"//:cpu", nil}, {"//:gpu", []string{"remex-hw", "no-remote-cache"}}, {"//:rtl", []string{"remex-hw", "no-remote-cache"}}}, Affected: []string{"//:gpu"}}
	if s := SelectHardware(r); s.Full || len(s.Targets) != 1 || s.Targets[0] != "//:gpu" {
		t.Fatal(s)
	}
	for _, file := range []string{"MODULE.bazel", "config/.bazelrc", "platforms/gpu.bzl", "toolchains/cc.bzl", "environment.lock"} {
		r.ChangedFiles = []string{file}
		if s := SelectHardware(r); !s.Full || len(s.Targets) != 2 {
			t.Fatal(s)
		}
	}
	r.ChangedFiles = nil
	r.SelectorError = "command failed"
	if s := SelectHardware(r); !s.Full || len(s.Targets) != 2 {
		t.Fatal(s)
	}
}
func TestBundleReplaysWithoutOriginalInputs(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd unavailable")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 unavailable")
	}
	root := t.TempDir()
	inputs := filepath.Join(root, "input")
	if err := os.Mkdir(inputs, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputs, "data"), []byte("payload"), 0644); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(root, "bundle")
	if err := ExportBundle(inputs, bundle, Bundle{Command: []string{"sh", "-c", "cat data; printf '%s' \"$REMEX_REPLAY_SEED\""}, Seed: 42}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(inputs); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(filepath.Join(bundle, "run.sh")).CombinedOutput()
	if err != nil || string(out) != "payload42" {
		t.Fatalf("%s %v", out, err)
	}
}
func TestBundleRejectsSymlinksAndTampering(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd unavailable")
	}
	root := t.TempDir()
	inputs := filepath.Join(root, "input")
	os.Mkdir(inputs, 0755)
	os.Symlink("/etc/passwd", filepath.Join(inputs, "link"))
	if err := ExportBundle(inputs, filepath.Join(root, "bundle"), Bundle{Command: []string{"true"}}); err == nil {
		t.Fatal("accepted symlink")
	}
	os.Remove(filepath.Join(inputs, "link"))
	os.WriteFile(filepath.Join(inputs, "data"), []byte("payload"), 0644)
	bundle := filepath.Join(root, "bundle")
	if err := ExportBundle(inputs, bundle, Bundle{Command: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	var b Bundle
	data, _ := os.ReadFile(filepath.Join(bundle, "bundle.json"))
	json.Unmarshal(data, &b)
	b.Inputs[0].SHA256 = strings.Repeat("0", 64)
	data, _ = json.Marshal(b)
	os.WriteFile(filepath.Join(bundle, "bundle.json"), data, 0644)
	if out, err := exec.Command(filepath.Join(bundle, "run.sh")).CombinedOutput(); err == nil || !bytes.Contains(out, []byte("hash mismatch")) {
		t.Fatalf("tampering accepted: %s %v", out, err)
	}
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	tw.WriteHeader(&tar.Header{Name: "../escaped", Mode: 0644, Size: 1})
	tw.Write([]byte("x"))
	tw.Close()
	cmd := exec.Command("zstd", "-q", "-c")
	cmd.Stdin = &archive
	compressed, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(bundle, "inputs.tar.zst"), compressed, 0644)
	if out, err := exec.Command(filepath.Join(bundle, "run.sh")).CombinedOutput(); err == nil || !bytes.Contains(out, []byte("unsafe")) {
		t.Fatalf("traversal accepted: %s %v", out, err)
	}
}
func TestGitHistoryRealRepository(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git: %v %s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("config", "user.email", "test@example.invalid")
	git("config", "user.name", "Test")
	var revisions []string
	for i := 0; i < 4; i++ {
		os.WriteFile(filepath.Join(root, "file"), []byte{byte('a' + i)}, 0644)
		git("add", "file")
		git("commit", "-qm", "revision")
		revisions = append(revisions, git("rev-parse", "HEAD"))
	}
	got, err := GitHistory(context.Background(), root, revisions[0], revisions[3])
	if err != nil || len(got) != 4 || got[2] != revisions[2] {
		t.Fatal(got, err)
	}
	if _, err := GitHistory(context.Background(), root, "--help", "HEAD"); err == nil {
		t.Fatal("accepted option injection")
	}
}

func TestLedgerTimeoutPreservesFreshLogSignature(t *testing.T) {
	results := []BEPResult{{Label: "//:timeout", Status: "TIMEOUT", FailureText: "seed=12\nREMEX_SIGNATURE: deadlock-v1\n"}}
	attempts := []AttemptEvidence{{ID: "deadline", Status: "DeadlineExceeded"}}
	got := Classify(results, attempts, "")
	if got.Category != Timeout || got.Signature != "deadlock-v1" {
		t.Fatalf("timeout signature lost: %+v", got)
	}
	attempts = append(attempts, AttemptEvidence{ID: "oom", Status: "ResourceExhausted", OOMKill: 1})
	if got := Classify(results, attempts, ""); got.Category != Infrastructure {
		t.Fatalf("timeout hid OOM: %+v", got)
	}
}

func TestExecutionCancellationPreservesLedgerEvidence(t *testing.T) {
	for _, attempts := range []string{`[]`, `[{"id":"a","execution_id":"executions/cancelled","status":"UNKNOWN","phase":"Running"}]`} {
		raw := []byte(`{"executions":[{"id":"executions/cancelled","invocation_ids":["ours"],"state":"Cancelled","status_code":1}],"attempts":` + attempts + `}`)
		evidence, err := ParseLedgerEvidence(raw, "ours")
		if err != nil {
			t.Fatal(err)
		}
		if got := Classify([]BEPResult{{Label: "//:test", Status: "REMOTE_FAILURE"}}, evidence, ""); got.Category != Cancelled || len(got.Evidence) != 1 || got.Evidence[0] != "execution:executions/cancelled" {
			t.Fatalf("cancelled execution lost: %+v", got)
		}
		if _, err = ParseLedgerEvidence(raw, "unrelated"); err == nil {
			t.Fatal("accepted unmatched invocation")
		}
		evidence = append(evidence, AttemptEvidence{ID: "oom", Status: "OK", OOMKill: 1})
		if got := Classify(nil, evidence, ""); got.Category != Infrastructure {
			t.Fatalf("cancellation hid OOM: %+v", got)
		}
	}
}
