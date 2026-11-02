package regress

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// GitHistory returns the first-parent linear ancestry ending at bad. It refuses
// ranges where good is merely on a merged side branch.
func GitHistory(ctx context.Context, repository, good, bad string) ([]string, error) {
	resolve := func(rev string) (string, error) {
		if strings.HasPrefix(rev, "-") {
			return "", fmt.Errorf("invalid revision")
		}
		out, err := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", "--verify", rev+"^{commit}").Output()
		return strings.TrimSpace(string(out)), err
	}
	g, err := resolve(good)
	if err != nil {
		return nil, err
	}
	b, err := resolve(bad)
	if err != nil {
		return nil, err
	}
	out, err := exec.CommandContext(ctx, "git", "-C", repository, "rev-list", "--first-parent", b).Output()
	if err != nil {
		return nil, err
	}
	var reversed []string
	found := false
	for _, rev := range strings.Fields(string(out)) {
		reversed = append(reversed, rev)
		if rev == g {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("good revision is not a first-parent ancestor of bad")
	}
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	return reversed, nil
}

type BazelRunner struct {
	Repository            string   `json:"repository"`
	Bazel                 string   `json:"bazel"`
	Targets               []string `json:"targets"`
	Flags                 []string `json:"flags"`
	EvidenceDirectory     string   `json:"evidence_directory"`
	SchedulerAdmin        string   `json:"scheduler_admin,omitempty"`
	WorkspaceSubdirectory string   `json:"workspace_subdirectory,omitempty"`
	// Optional explicit command, invoked with revision, seed and evidence dir.
	// It must emit one Observation JSON. Useful for instrumented fault/corpus runs.
	ProbeCommand []string `json:"probe_command,omitempty"`
}

func (r BazelRunner) Run(ctx context.Context, revision string, seed int) (Observation, error) {
	if r.EvidenceDirectory == "" {
		return Observation{}, fmt.Errorf("evidence_directory is required")
	}
	if err := os.MkdirAll(r.EvidenceDirectory, 0755); err != nil {
		return Observation{}, err
	}
	evidence, err := os.MkdirTemp(r.EvidenceDirectory, "probe-")
	if err != nil {
		return Observation{}, err
	}
	evidence, err = filepath.Abs(evidence)
	if err != nil {
		return Observation{}, err
	}
	meta, _ := json.Marshal(map[string]any{"revision": revision, "seed": seed})
	if err = os.WriteFile(filepath.Join(evidence, "probe.json"), meta, 0644); err != nil {
		return Observation{}, err
	}
	if len(r.ProbeCommand) > 0 {
		args := append(append([]string{}, r.ProbeCommand[1:]...), revision, fmt.Sprint(seed), evidence)
		cmd := exec.CommandContext(ctx, r.ProbeCommand[0], args...)
		cmd.Dir = r.Repository
		stderr, err := os.Create(filepath.Join(evidence, "stderr.log"))
		if err != nil {
			return Observation{}, err
		}
		defer stderr.Close()
		cmd.Stderr = stderr
		out, err := cmd.Output()
		if writeErr := os.WriteFile(filepath.Join(evidence, "observation.json"), out, 0644); writeErr != nil {
			return Observation{}, writeErr
		}
		if err != nil {
			return Observation{}, err
		}
		var observation Observation
		if err = json.Unmarshal(out, &observation); err != nil {
			return Observation{}, err
		}
		observation.Evidence = append(observation.Evidence, evidence)
		return observation, nil
	}
	if len(r.Targets) == 0 {
		return Observation{}, fmt.Errorf("Bazel runner requires test targets")
	}
	for _, target := range r.Targets {
		if strings.HasPrefix(target, "-") {
			return Observation{}, fmt.Errorf("target must not be a Bazel option")
		}
	}
	if strings.HasPrefix(revision, "-") {
		return Observation{}, fmt.Errorf("invalid revision")
	}
	if r.Bazel == "" {
		r.Bazel = "bazel"
	}
	workRoot, err := os.MkdirTemp("", "remex-bisect-")
	if err != nil {
		return Observation{}, err
	}
	defer os.RemoveAll(workRoot)
	work := filepath.Join(workRoot, "worktree")
	if out, err := exec.CommandContext(
		ctx,
		"git",
		"-C",
		r.Repository,
		"worktree",
		"add",
		"--detach",
		work,
		revision,
	).CombinedOutput(); err != nil {
		return Observation{}, fmt.Errorf("worktree: %w: %s", err, out)
	}
	defer exec.Command("git", "-C", r.Repository, "worktree", "remove", "--force", work).Run()
	bep := filepath.Join(evidence, "bep.json")
	var invocationBytes [16]byte
	if _, err := rand.Read(invocationBytes[:]); err != nil {
		return Observation{}, err
	}
	invocationBytes[6] = (invocationBytes[6] & 15) | 64
	invocationBytes[8] = (invocationBytes[8] & 63) | 128
	hexID := hex.EncodeToString(invocationBytes[:])
	invocation := hexID[:8] + "-" + hexID[8:12] + "-" + hexID[12:16] + "-" + hexID[16:20] + "-" + hexID[20:]
	args := append([]string{"test"}, r.Flags...)
	args = append(args, "--invocation_id="+invocation)
	args = append(
		args,
		"--nocache_test_results",
		"--remote_accept_cached=false",
		"--flaky_test_attempts=1",
		"--runs_per_test=1",
		"--build_event_json_file="+bep,
		"--remote_grpc_log="+filepath.Join(evidence, "grpc.log"),
		"--execution_log_compact_file="+filepath.Join(evidence, "execution.log"),
		fmt.Sprintf("--test_env=REMEX_SEED=%d", seed),
	)
	args = append(args, r.Targets...)
	cmd := exec.CommandContext(ctx, r.Bazel, args...)
	cmd.Dir = work
	if r.WorkspaceSubdirectory != "" {
		clean := filepath.Clean(r.WorkspaceSubdirectory)
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return Observation{}, fmt.Errorf("workspace_subdirectory must stay inside the repository")
		}
		cmd.Dir = filepath.Join(work, clean)
	}
	log, err := os.Create(filepath.Join(evidence, "bazel.log"))
	if err != nil {
		return Observation{}, err
	}
	cmd.Stdout = log
	cmd.Stderr = log
	runErr := cmd.Run()
	log.Close()
	f, err := os.Open(bep)
	if err != nil {
		return Observation{
			Category: Infrastructure,
			Detail:   fmt.Sprintf("Bazel did not write BEP: %v; %v", runErr, err),
			Evidence: []string{evidence},
		}, nil
	}
	defer f.Close()
	results, err := ReadBEP(f)
	if err != nil {
		return Observation{}, err
	}
	for i := range results {
		if results[i].Status != "FAILED" && results[i].Status != "TIMEOUT" {
			continue
		}
		uri, parseErr := url.Parse(results[i].LogURI)
		if parseErr == nil {
			var data []byte
			var readErr error
			switch {
			case uri.Scheme == "file" && (uri.Host == "" || uri.Host == "localhost"):
				data, readErr = os.ReadFile(uri.Path)
			case uri.Scheme == "bytestream":
				data, readErr = readRemoteTestLog(ctx, uri)
			default:
				readErr = fmt.Errorf("unsupported test log URI")
			}
			if readErr == nil {
				results[i].FailureText = string(data)
				_ = os.WriteFile(filepath.Join(evidence, fmt.Sprintf("test-%d.log", i)), data, 0644)
			}
		}
	}
	var attempts []AttemptEvidence
	if r.SchedulerAdmin != "" {
		attempts, err = invocationAttempts(ctx, r.SchedulerAdmin, invocation, evidence)
		if err != nil {
			return Observation{
				Category: Infrastructure,
				Detail:   "ledger evidence unavailable: " + err.Error(),
				Evidence: []string{evidence},
			}, nil
		}
	}
	observation := Classify(results, attempts, "")
	if observation.Category == Pass && runErr != nil {
		observation = Observation{
			Category: Infrastructure,
			Detail:   "Bazel invocation failed despite passing test results",
		}
	}
	observation.Evidence = append(observation.Evidence, evidence)
	encoded, _ := json.MarshalIndent(observation, "", "  ")
	if err = os.WriteFile(filepath.Join(evidence, "observation.json"), encoded, 0644); err != nil {
		return Observation{}, err
	}
	return observation, nil
}

// Correlate by Bazel's explicit invocation UUID, never by wall-clock intervals:
// unrelated concurrent invocations must not contaminate a probe's evidence.
func invocationAttempts(ctx context.Context, admin, invocation, evidence string) ([]AttemptEvidence, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(admin, "/")+"/export", nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ledger HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	if err = os.WriteFile(filepath.Join(evidence, "ledger.json"), raw, 0644); err != nil {
		return nil, err
	}
	return ParseLedgerEvidence(raw, invocation)
}
