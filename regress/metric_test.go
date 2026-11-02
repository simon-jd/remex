package regress

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
)

// timingRunner simulates a kernel time: revisions from culprit on take
// slowdown times longer. Each run has relative noise, a slow drift of
// drift per run, and an infrastructure failure with probability infra.
type timingRunner struct {
	revisions       map[string]int
	culprit         int
	slowdown, noise float64
	drift, infra    float64
	failAt          int // a revision whose test fails; -1 for none
	rng             *rand.Rand
	runs            int
	higherIsBetter  bool
	withoutMetricAt int
}

func newTimingRunner(n, culprit int, slowdown float64, seed uint64) (*timingRunner, []string) {
	revisions := make([]string, n)
	index := map[string]int{}
	for i := range revisions {
		revisions[i] = fmt.Sprintf("r%d", i)
		index[revisions[i]] = i
	}
	return &timingRunner{
		revisions: index, culprit: culprit, slowdown: slowdown, noise: .01, infra: .1,
		failAt: -1, withoutMetricAt: -1, rng: rand.New(rand.NewPCG(seed, 1)),
	}, revisions
}

func (r *timingRunner) Run(_ context.Context, revision string, _ int) (Observation, error) {
	r.runs++
	i := r.revisions[revision]
	if r.rng.Float64() < r.infra {
		return Observation{Category: Infrastructure}, nil
	}
	if i == r.failAt {
		return Observation{Category: TestFailure, Signature: "wrong-answer"}, nil
	}
	if i == r.withoutMetricAt {
		return Observation{Category: Pass}, nil
	}
	v := 1.0
	if i >= r.culprit {
		v *= r.slowdown
	}
	v *= (1 + r.drift*float64(r.runs)) * (1 + r.noise*r.rng.NormFloat64())
	if r.higherIsBetter {
		v = 1 / v
	}
	return Observation{Category: Pass, Metrics: map[string]float64{"kernel_ms": v}}, nil
}

func metricConfig(revisions []string) BisectConfig {
	return BisectConfig{Revisions: revisions, Metric: &MetricConfig{Name: "kernel_ms"}}
}

func TestMetricBisectFindsTimingRegression(t *testing.T) {
	for _, slowdown := range []float64{1.08, 1.3, 3} {
		for seed := range uint64(40) {
			runner, revisions := newTimingRunner(64, 1+int(seed*7)%63, slowdown, seed)
			r, err := Bisect(context.Background(), metricConfig(revisions), runner)
			want := revisions[runner.culprit]
			if err != nil || r.Conclusion != ConclusionCulprit || r.Culprit != want {
				t.Fatalf("slowdown %v seed %d: %s %s, want %s (%s)", slowdown, seed, r.Conclusion, r.Culprit, want, r.Reason)
			}
			if last := r.Probes[len(r.Probes)-1]; last.Purpose != PurposeConfirmCulprit || last.P == nil || *last.P >= .05 {
				t.Fatalf("confirmation probe %+v", last)
			}
		}
	}
}

// Drift smaller than half the regression shifts every value but leaves the
// classification to the nearer endpoint intact.
func TestMetricBisectToleratesDrift(t *testing.T) {
	runner, revisions := newTimingRunner(100, 61, 1.3, 7)
	runner.drift = .0005 // 5% over 100 runs
	r, err := Bisect(context.Background(), metricConfig(revisions), runner)
	if err != nil || r.Culprit != "r61" {
		t.Fatalf("%s %s (%s)", r.Conclusion, r.Culprit, r.Reason)
	}
}

func TestMetricBisectHigherIsBetter(t *testing.T) {
	runner, revisions := newTimingRunner(32, 9, 1.3, 3)
	runner.higherIsBetter = true
	c := metricConfig(revisions)
	c.Metric.HigherIsBetter = true
	r, err := Bisect(context.Background(), c, runner)
	if err != nil || r.Culprit != "r9" || r.Shift < .2 {
		t.Fatalf("%s %s shift %v (%s)", r.Conclusion, r.Culprit, r.Shift, r.Reason)
	}
	// Read as a time, a throughput drop is an improvement, not a regression.
	c.Metric.HigherIsBetter = false
	runner, _ = newTimingRunner(32, 9, 1.3, 3)
	runner.higherIsBetter = true
	if r, _ := Bisect(context.Background(), c, runner); r.Conclusion != ConclusionInconclusive {
		t.Fatalf("a throughput drop read as a time regression: %+v", r)
	}
}

func TestMetricBisectRefusesUnmeasurableEndpoints(t *testing.T) {
	runner, revisions := newTimingRunner(16, 8, 1.02, 1) // below the 5% minimum shift
	if r, err := Bisect(context.Background(), metricConfig(revisions), runner); err != nil || r.Conclusion != ConclusionInconclusive {
		t.Fatalf("2%% shift: %+v %v", r, err)
	}
	runner, revisions = newTimingRunner(16, 8, 2, 1)
	runner.withoutMetricAt = 15
	r, err := Bisect(context.Background(), metricConfig(revisions), runner)
	if err != nil || r.Conclusion != ConclusionInconclusive || len(r.Probes) != 2 {
		t.Fatalf("known-bad without the metric: %+v %v", r, err)
	}
	if _, err := Bisect(context.Background(), BisectConfig{Revisions: revisions, Metric: &MetricConfig{}}, runner); err == nil {
		t.Fatal("metric without a name accepted")
	}
}

// A revision whose test fails cannot be measured; next to the boundary it
// leaves a range instead of a guess.
func TestMetricBisectSkipsFailingRevisions(t *testing.T) {
	runner, revisions := newTimingRunner(16, 9, 2, 5)
	runner.failAt = 8
	r, err := Bisect(context.Background(), metricConfig(revisions), runner)
	if err != nil || r.Conclusion != ConclusionRange || len(r.Range) != 2 || r.Range[0] != "r8" || r.Range[1] != "r9" {
		t.Fatalf("%s %v (%s)", r.Conclusion, r.Range, r.Reason)
	}
}

func TestMetricBisectStopsAtBudget(t *testing.T) {
	runner, revisions := newTimingRunner(64, 30, 2, 2)
	c := metricConfig(revisions)
	c.Budget = 25
	r, err := Bisect(context.Background(), c, runner)
	if err != nil || r.Conclusion != ConclusionInconclusive || r.Runs != 25 || runner.runs != 25 {
		t.Fatalf("%s after %d runs (%s)", r.Conclusion, r.Runs, r.Reason)
	}
}

func TestParseMetrics(t *testing.T) {
	got := ParseMetrics("x\nREMEX_METRIC kernel_ms=1.5\n  REMEX_METRIC kernel_ms = 1.25 \nREMEX_METRIC bad=abc\nREMEX_METRIC =3\n")
	if len(got) != 1 || got["kernel_ms"] != 1.25 {
		t.Fatalf("%v", got)
	}
}

// Small cases counted by hand: with no ties every arrangement of the m+n
// values is equally likely, so P(U >= u) is a count over C(m+n, m).
func TestMannWhitneyExactSmallCases(t *testing.T) {
	cases := []struct {
		x, y []float64
		want float64
	}{
		{[]float64{3}, []float64{1, 2}, 1. / 3},           // U = 2 of {0, 1, 2}
		{[]float64{2, 3}, []float64{1, 4}, 4. / 6},        // U = 2; U is 0, 1, 2, 2, 3, 4 over the 6 arrangements
		{[]float64{5, 6, 7}, []float64{1, 2, 3}, 1. / 20}, // U = 9, the maximum
		{[]float64{1, 2, 3}, []float64{5, 6, 7}, 1},       // U = 0
	}
	for _, c := range cases {
		if got := MannWhitneyGreater(c.x, c.y); math.Abs(got-c.want) > 1e-12 {
			t.Fatalf("%v vs %v: p = %v, want %v", c.x, c.y, got, c.want)
		}
	}
}
