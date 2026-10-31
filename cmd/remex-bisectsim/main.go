// Command remex-bisectsim measures how often a bisection names the right
// culprit, and how many test runs it spends, on simulated histories.
//
// A history has N commits with the culprit placed uniformly at random. At
// and after the culprit the test fails with probability p (one signature);
// before it, it passes. Independently, each run hits an infrastructure
// failure with probability infra. Three strategies:
//
//	remex      regress.Bisect: calibrate on the known-bad revision, size each
//	         probe from a Clopper-Pearson bound, confirm the boundary afresh
//	one-shot git bisect run: one run per probe, any failure counts as bad
//	rerun-5  five runs per probe, bad if any of them failed
//
// One-shot and rerun-5 count infrastructure failures as test failures, as a
// plain `git bisect run` script that only sees an exit code does.
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
	Commits      int     `json:"commits"`
	FailureRate  float64 `json:"failure_rate"`
	InfraRate    float64 `json:"infra_rate"`
	Trials       int     `json:"trials"`
	Correct      float64 `json:"correct"`
	Wrong        float64 `json:"wrong"`
	Inconclusive float64 `json:"inconclusive"`
	RunsMedian   int     `json:"runs_median"`
	RunsP95      int     `json:"runs_p95"`
}

func summarize(name string, commits int, p, infra float64, outcomes []string, runs []int) stats {
	s := stats{Strategy: name, Commits: commits, FailureRate: p, InfraRate: infra, Trials: len(outcomes)}
	for _, o := range outcomes {
		switch o {
		case "correct":
			s.Correct++
		case "wrong":
			s.Wrong++
		default:
			s.Inconclusive++
		}
	}
	n := float64(len(outcomes))
	s.Correct, s.Wrong, s.Inconclusive = s.Correct/n, s.Wrong/n, s.Inconclusive/n
	sort.Ints(runs)
	s.RunsMedian, s.RunsP95 = runs[len(runs)/2], runs[min(len(runs)-1, len(runs)*95/100)]
	return s
}

func main() {
	trials := flag.Int("trials", 1000, "simulated bisections per configuration")
	flag.Parse()
	out := json.NewEncoder(os.Stdout)
	for _, commits := range []int{96, 1000} {
		for _, p := range []float64{1, 0.5, 0.25, 0.1} {
			for _, infra := range []float64{0, 0.1} {
				results := map[string][]string{}
				spent := map[string][]int{}
				for trial := range *trials {
					seed := uint64(trial)*7919 + uint64(commits)
					revisions := make([]string, commits)
					for i := range revisions {
						revisions[i] = fmt.Sprintf("r%04d", i)
					}
					culprit := 1 + rand.New(rand.NewPCG(seed, 1)).IntN(commits-1)
					judge := func(found int) string {
						if found == culprit {
							return "correct"
						}
						return "wrong"
					}
					for _, n := range []int{1, 5} {
						name := map[int]string{1: "one-shot", 5: "rerun-5"}[n]
						w := &world{culprit: culprit, p: p, infra: infra, rng: rand.New(rand.NewPCG(seed, uint64(n))), revisions: revisions}
						found, runs := w.naive(n)
						results[name] = append(results[name], judge(found))
						spent[name] = append(spent[name], runs)
					}
					w := &world{culprit: culprit, p: p, infra: infra, rng: rand.New(rand.NewPCG(seed, 99)), revisions: revisions}
					index := map[string]int{}
					for i, r := range revisions {
						index[r] = i
					}
					runner := regress.RunnerFunc(func(_ context.Context, rev string, _ int) (regress.Observation, error) {
						return w.run(index[rev]), nil
					})
					r, err := regress.Bisect(context.Background(), regress.BisectConfig{Revisions: revisions, Budget: 5000}, runner)
					outcome := "inconclusive"
					if err == nil && r.Conclusion == "CULPRIT" {
						outcome = judge(index[r.Culprit])
					}
					results["remex"] = append(results["remex"], outcome)
					spent["remex"] = append(spent["remex"], r.Runs)
				}
				for _, name := range []string{"one-shot", "rerun-5", "remex"} {
					_ = out.Encode(summarize(name, commits, p, infra, results[name], spent[name]))
				}
			}
		}
	}
}
