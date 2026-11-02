package regress

import (
	"context"
	"fmt"
	"math"
)

// MetricConfig makes a bisection look for a performance regression instead
// of a failure. Every passing run must report the named metric (a
// "REMEX_METRIC name=value" line in the test log, or Observation.Metrics
// from a probe command). The known-good and known-bad revisions are measured
// first and must differ by at least MinShift. A probed revision then counts
// as BAD when its median is nearer the known-bad median and a one-sided
// Mann-Whitney U test puts it above the known-good values at Alpha, and as
// GOOD in the mirror case. A revision that is neither after MaxRuns values,
// or whose test fails, is skipped.
type MetricConfig struct {
	Name string `json:"name"`
	// By default a larger value (a time) is worse.
	HigherIsBetter bool `json:"higher_is_better,omitempty"`
	// Values per probe (default 10); an unsettled probe gets Runs more, up
	// to MaxRuns (default 3*Runs). Confirmation takes 2*Runs fresh values.
	Runs    int `json:"runs,omitempty"`
	MaxRuns int `json:"max_runs,omitempty"`
	// Smallest relative change between the endpoints' medians that counts
	// as a regression (default 0.05).
	MinShift float64 `json:"min_shift,omitempty"`
}

func bisectMetric(ctx context.Context, c BisectConfig, runner Runner, r BisectResult) (BisectResult, error) {
	m := *c.Metric
	if m.Runs == 0 {
		m.Runs = 10
	}
	if m.MaxRuns == 0 {
		m.MaxRuns = 3 * m.Runs
	}
	if m.MinShift == 0 {
		m.MinShift = .05
	}
	if m.Name == "" || m.Runs < 3 || m.MaxRuns < m.Runs || m.MinShift < 0 {
		return r, fmt.Errorf("invalid metric configuration")
	}
	r.Metric = &m
	exhausted := false
	// worse orients values so that larger is always worse.
	worse := func(v float64) float64 {
		if m.HigherIsBetter {
			return -v
		}
		return v
	}
	// sample runs a revision until it has n metric values. It stops early,
	// leaving the probe unclassified, on a failing test or a missing value.
	sample := func(p *Probe, values []float64, n int) ([]float64, bool) {
		for len(values) < n {
			if r.Runs >= c.Budget || ctx.Err() != nil {
				exhausted = true
				return values, false
			}
			o, err := runner.Run(ctx, p.Revision, c.Seed+r.Runs)
			r.Runs++
			if err != nil {
				o = Observation{Category: Infrastructure, Detail: err.Error()}
			}
			p.Observations = append(p.Observations, o)
			p.Counts[o.Category]++
			switch o.Category {
			case Pass:
				v, ok := o.Metrics[m.Name]
				if !ok || math.IsNaN(v) || math.IsInf(v, 0) {
					return values, false
				}
				values = append(values, worse(v))
			case TestFailure, Timeout:
				return values, false
			default: // Infrastructure and cancellation consume budget but give no value.
			}
		}
		return values, true
	}
	newProbe := func(index int, purpose Purpose) Probe {
		return Probe{
			Revision:     c.Revisions[index],
			Purpose:      purpose,
			Decision:     DecisionSkip,
			Counts:       map[Category]int{},
			Observations: []Observation{},
		}
	}
	finish := func(p Probe, values []float64) Probe {
		if len(values) > 0 {
			med := worse(median(values))
			p.Median = &med
		}
		r.Probes = append(r.Probes, p)
		return p
	}

	bad := len(c.Revisions) - 1
	goodProbe, badProbe := newProbe(0, PurposeKnownGood), newProbe(bad, PurposeCalibration)
	goodValues, okGood := sample(&goodProbe, nil, m.Runs)
	badValues, okBad := sample(&badProbe, nil, m.Runs)
	if okGood && okBad {
		goodProbe.Decision, badProbe.Decision = DecisionGood, DecisionBad
	}
	goodProbe, badProbe = finish(goodProbe, goodValues), finish(badProbe, badValues)
	if exhausted {
		r.Reason = "budget exhausted or context cancelled during calibration"
		return r, nil
	}
	if !okGood || !okBad {
		r.Reason = "an endpoint failed its test or did not report the metric"
		return r, nil
	}
	goodMedian, badMedian := median(goodValues), median(badValues)
	shift := (badMedian - goodMedian) / math.Abs(goodMedian)
	pEndpoints := MannWhitneyGreater(badValues, goodValues)
	badProbe.P = &pEndpoints
	r.Probes[len(r.Probes)-1] = badProbe
	r.Shift = shift
	if pEndpoints >= c.Alpha || shift < m.MinShift {
		r.Reason = fmt.Sprintf("known-bad revision is not measurably worse (shift %.3f, p %.2g)", shift, pEndpoints)
		return r, nil
	}
	midpoint := (goodMedian + badMedian) / 2

	// classify takes values until the probe's side of the midpoint is
	// significant against the opposite endpoint, or MaxRuns is reached.
	classify := func(index int, purpose Purpose, n, max int) Probe {
		p := newProbe(index, purpose)
		var values []float64
		for target := n; ; target = min(target+m.Runs, max) {
			var ok bool
			if values, ok = sample(&p, values, target); !ok {
				return finish(p, values)
			}
			var pValue float64
			decision := DecisionBad
			if median(values) >= midpoint {
				pValue = MannWhitneyGreater(values, goodValues)
			} else {
				decision = DecisionGood
				pValue = MannWhitneyGreater(badValues, values)
			}
			p.P = &pValue
			if pValue < c.Alpha {
				p.Decision = decision
				return finish(p, values)
			}
			if target >= max {
				return finish(p, values)
			}
		}
	}

	good := 0
	skipped := map[int]bool{}
	for bad-good > 1 && !exhausted {
		mid := (good + bad) / 2
		if skipped[mid] {
			mid = -1
			for i := good + 1; i < bad; i++ {
				if !skipped[i] {
					mid = i
					break
				}
			}
			if mid < 0 {
				break
			}
		}
		switch classify(mid, PurposeSearch, m.Runs, m.MaxRuns).Decision {
		case DecisionGood:
			good = mid
		case DecisionBad:
			bad = mid
		default:
			skipped[mid] = true
		}
	}
	r.Range = append([]string(nil), c.Revisions[good+1:bad+1]...)
	if exhausted {
		r.Reason = "budget exhausted or context cancelled"
		return r, nil
	}
	if bad-good > 1 {
		r.Conclusion = ConclusionRange
		r.Reason = "revisions that could not be classified separate the boundary"
		return r, nil
	}
	parent := classify(good, PurposeConfirmParent, 2*m.Runs, 2*m.Runs)
	child := classify(bad, PurposeConfirmCulprit, 2*m.Runs, 2*m.Runs)
	if exhausted || parent.Decision != DecisionGood || child.Decision != DecisionBad {
		r.Reason = "fresh confirmation failed or budget exhausted"
		return r, nil
	}
	r.Conclusion = ConclusionCulprit
	r.Culprit = c.Revisions[bad]
	r.Reason = "fresh values put the parent with the known-good revision and the culprit with the known-bad one"
	return r, nil
}
