// Command remex-bisectsim measures how often a bisection names the right
// culprit, and how many test runs it spends, on simulated histories.
//
// A history has N commits with the culprit placed uniformly at random. At
// and after the culprit the test fails with probability p (one signature);
// before it, it passes. Independently, each run hits an infrastructure
// failure with probability infra. Strategies:
//
//	remex        regress.Bisect with its default budget: calibrate on the
//	             known-bad revision, size each probe from a Clopper-Pearson
//	             bound, confirm the boundary afresh
//	one-shot     git bisect run, one run per probe; any failure counts as bad
//	rerun-5      five runs per probe, bad if any of them did not pass
//	git-skip     git bisect run, one run per probe; the script exits 125 on an
//	             infrastructure failure, so git skips that revision
//	git-rerun    git bisect run with a script that runs the test until it has
//	             N results (reruns), bad at the first test failure; it retries
//	             infrastructure failures and exits 125 after five in a row
//
// One-shot and rerun-5 count infrastructure failures as test failures, as a
// plain `git bisect run` script that only sees an exit code does. git-rerun
// is swept over N, so its correctness can be read against the runs it
// spends; with N chosen for the true failure rate it is the cost-matched
// comparison for remex, which has to estimate that rate itself.
//
// Every row reports wrong_upper95, the one-sided 95% Clopper-Pearson upper
// bound on the rate of wrong answers given the trials run.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"

	"github.com/simon-jd/remex/regress"
)

type world struct {
	culprit   int
	p, infra  float64
	rng       *rand.Rand
	revisions []string
}

func (w *world) run(index int) regress.Observation {
	if w.rng.Float64() < w.infra {
		return regress.Observation{Category: regress.Infrastructure, Detail: "worker lost"}
	}
	if index >= w.culprit && w.rng.Float64() < w.p {
		return regress.Observation{Category: regress.TestFailure, Signature: "assertion at fifo_test.cc:42"}
	}
	return regress.Observation{Category: regress.Pass}
}

// naive bisects with n runs per probe; a probe is bad if any run did not pass.
func (w *world) naive(n int) (int, int) {
	good, bad, runs := 0, len(w.revisions)-1, 0
	for bad-good > 1 {
		mid := (good + bad) / 2
		failed := false
		for range n {
			runs++
			if w.run(mid).Category != regress.Pass {
				failed = true
				break
			}
		}
		if failed {
			bad = mid
		} else {
			good = mid
		}
	}
	return bad, runs
}

type stats struct {
	Strategy     string  `json:"strategy"`
	Reruns       int     `json:"reruns,omitempty"`
	Budget       int     `json:"budget,omitempty"`
	Commits      int     `json:"commits"`
	FailureRate  float64 `json:"failure_rate"`
	InfraRate    float64 `json:"infra_rate"`
	Trials       int     `json:"trials"`
	Correct      float64 `json:"correct"`
	Wrong        float64 `json:"wrong"`
	Inconclusive float64 `json:"inconclusive"`
	WrongUpper95 float64 `json:"wrong_upper95"`
	RunsMedian   int     `json:"runs_median"`
	RunsP95      int     `json:"runs_p95"`
}

type verdict int

const (
	verdictGood verdict = iota
	verdictBad
	verdictSkip
)

// gitBisect bisects as `git bisect run` does with a probe script that
// reports good, bad or skip (exit 125). After a skip it tests the untested
// revision nearest the midpoint; git picks a pseudo-random one near it.
// When only skipped revisions separate good from bad, git cannot name the
// first bad commit: the result is -1, inconclusive.
func (w *world) gitBisect(probe func(index int) verdict) int {
	good, bad := 0, len(w.revisions)-1
	skipped := map[int]bool{}
	for bad-good > 1 {
		center, next := (good+bad)/2, -1
		for d := 0; next < 0 && (center-d > good || center+d < bad); d++ {
			if i := center - d; i > good && !skipped[i] {
				next = i
			} else if i := center + d; i < bad && !skipped[i] {
				next = i
			}
		}
		if next < 0 {
			return -1
		}
		switch probe(next) {
		case verdictGood:
			good = next
		case verdictBad:
			bad = next
		default:
			skipped[next] = true
		}
	}
	return bad
}

// gitSkip is one run per probe, with infrastructure failures skipped.
func (w *world) gitSkip() (int, int) {
	runs := 0
	found := w.gitBisect(func(index int) verdict {
		runs++
		switch w.run(index).Category {
		case regress.Pass:
			return verdictGood
		case regress.Infrastructure:
			return verdictSkip
		}
		return verdictBad
	})
	return found, runs
}

// gitRerun runs each probe until n test results, bad at the first failure.
// Infrastructure failures are retried; five in a row skip the revision.
const infraRetries = 5

func (w *world) gitRerun(n int) (int, int) {
	runs := 0
	found := w.gitBisect(func(index int) verdict {
		results, infraInARow := 0, 0
		for results < n {
			runs++
			switch w.run(index).Category {
			case regress.Pass:
				results, infraInARow = results+1, 0
			case regress.Infrastructure:
				if infraInARow++; infraInARow == infraRetries {
					return verdictSkip
				}
			default:
				return verdictBad
			}
		}
		return verdictGood
	})
	return found, runs
}

// wrongUpper95 is the one-sided 95% Clopper-Pearson upper bound on a rate
// observed k times in n trials: one minus the lower bound for the n-k others.
func wrongUpper95(k, n int) float64 {
	return 1 - regress.FailureRateLowerBound(n-k, n, 0.05)
}

func summarize(name string, reruns, budget, commits int, p, infra float64, outcomes []string, runs []int) stats {
	s := stats{Strategy: name, Reruns: reruns, Budget: budget, Commits: commits, FailureRate: p, InfraRate: infra, Trials: len(outcomes)}
	wrong := 0
	for _, o := range outcomes {
		switch o {
		case "correct":
			s.Correct++
		case "wrong":
			s.Wrong++
			wrong++
		default:
			s.Inconclusive++
		}
	}
	n := float64(len(outcomes))
	s.Correct, s.Wrong, s.Inconclusive = s.Correct/n, s.Wrong/n, s.Inconclusive/n
	s.WrongUpper95 = wrongUpper95(wrong, len(outcomes))
	sort.Ints(runs)
	s.RunsMedian, s.RunsP95 = runs[len(runs)/2], runs[min(len(runs)-1, len(runs)*95/100)]
	return s
}

// rerunSweep is the set of N for git-rerun.
var rerunSweep = []int{1, 2, 3, 5, 10, 20, 30, 50, 100}

func main() {
	trials := flag.Int("trials", 1000, "simulated bisections per configuration")
	flag.Parse()
	out := json.NewEncoder(os.Stdout)
	for _, commits := range []int{96, 1000} {
		for _, p := range []float64{1, 0.5, 0.25, 0.1} {
			for _, infra := range []float64{0, 0.1} {
				results := map[string][]string{}
				spent := map[string][]int{}
				budget := 0
				for trial := range *trials {
					seed := uint64(trial)*7919 + uint64(commits)
					revisions := make([]string, commits)
					for i := range revisions {
						revisions[i] = fmt.Sprintf("r%04d", i)
					}
					culprit := 1 + rand.New(rand.NewPCG(seed, 1)).IntN(commits-1)
					judge := func(found int) string {
						switch found {
						case culprit:
							return "correct"
						case -1:
							return "inconclusive"
						}
						return "wrong"
					}
					record := func(name string, found, runs int) {
						results[name] = append(results[name], judge(found))
						spent[name] = append(spent[name], runs)
					}
					newWorld := func(stream uint64) *world {
						return &world{culprit: culprit, p: p, infra: infra, rng: rand.New(rand.NewPCG(seed, stream)), revisions: revisions}
					}
					for _, n := range []int{1, 5} {
						found, runs := newWorld(uint64(n)).naive(n)
						record(map[int]string{1: "one-shot", 5: "rerun-5"}[n], found, runs)
					}
					found, runs := newWorld(200).gitSkip()
					record("git-skip", found, runs)
					for _, n := range rerunSweep {
						found, runs := newWorld(300 + uint64(n)).gitRerun(n)
						record(fmt.Sprintf("git-rerun-%d", n), found, runs)
					}
					w := newWorld(99)
					index := map[string]int{}
					for i, r := range revisions {
						index[r] = i
					}
					runner := regress.RunnerFunc(func(_ context.Context, rev string, _ int) (regress.Observation, error) {
						return w.run(index[rev]), nil
					})
					r, err := regress.Bisect(context.Background(), regress.BisectConfig{Revisions: revisions}, runner)
					outcome := "inconclusive"
					if err == nil && r.Conclusion == "CULPRIT" {
						outcome = judge(index[r.Culprit])
					}
					budget = r.Budget
					results["remex"] = append(results["remex"], outcome)
					spent["remex"] = append(spent["remex"], r.Runs)
				}
				for _, name := range []string{"one-shot", "rerun-5", "git-skip"} {
					_ = out.Encode(summarize(name, 0, 0, commits, p, infra, results[name], spent[name]))
				}
				for _, n := range rerunSweep {
					_ = out.Encode(summarize("git-rerun", n, 0, commits, p, infra, results[fmt.Sprintf("git-rerun-%d", n)], spent[fmt.Sprintf("git-rerun-%d", n)]))
				}
				_ = out.Encode(summarize("remex", 0, budget, commits, p, infra, results["remex"], spent["remex"]))
			}
		}
	}
}
