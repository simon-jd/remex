package regress

import (
	"context"
	"fmt"
	"math"
)

type Runner interface {
	Run(context.Context, string, int) (Observation, error)
}
type RunnerFunc func(context.Context, string, int) (Observation, error)

func (f RunnerFunc) Run(c context.Context, r string, s int) (Observation, error) { return f(c, r, s) }

type BisectConfig struct {
	Revisions []string `json:"revisions"` // oldest known-good through known-bad, inclusive
	Signature string   `json:"signature"`
	Alpha     float64  `json:"alpha"`
	// Calibration reruns the known-bad revision until it has failed
	// CalibrationFailures times (inverse binomial sampling), or for at most
	// CalibrationRuns valid runs. A rare flake thus gets enough runs for a
	// usable failure-rate bound, and a deterministic failure only a few.
	CalibrationFailures int `json:"calibration_failures"`
	CalibrationRuns     int `json:"calibration_runs"`
	Budget              int `json:"budget"` // every invocation, including infrastructure failures
	Seed                int `json:"seed"`
	// Metric, if set, bisects a performance regression in the named metric
	// instead of a failure; the calibration settings above are then unused.
	Metric *MetricConfig `json:"metric,omitempty"`
}

// Decision is a probe's verdict on one revision.
type Decision string

const (
	DecisionGood Decision = "GOOD"
	DecisionBad  Decision = "BAD"
	// DecisionSkip: the probe ran out of budget or saw another failure
	// signature, so the revision stays unclassified.
	DecisionSkip Decision = "SKIP"
)

// Purpose names the step of a bisection that ran a probe.
type Purpose string

const (
	PurposeCalibration    Purpose = "calibration"
	PurposeKnownGood      Purpose = "known-good"
	PurposeSearch         Purpose = "search"
	PurposeConfirmParent  Purpose = "confirm-parent"
	PurposeConfirmCulprit Purpose = "confirm-culprit"
)

// Conclusion is the outcome of a bisection.
type Conclusion string

const (
	ConclusionCulprit      Conclusion = "CULPRIT"
	ConclusionRange        Conclusion = "RANGE"
	ConclusionInconclusive Conclusion = "INCONCLUSIVE"
)

type Probe struct {
	Revision     string           `json:"revision"`
	Purpose      Purpose          `json:"purpose"`
	Decision     Decision         `json:"decision"`
	Observations []Observation    `json:"observations"`
	Counts       map[Category]int `json:"counts"`
	// Metric bisection only: the median of the probe's values and the
	// p-value its decision rests on.
	Median *float64 `json:"median,omitempty"`
	P      *float64 `json:"p,omitempty"`
}
type BisectResult struct {
	Revisions             []string   `json:"revisions"`
	Conclusion            Conclusion `json:"conclusion"`
	Culprit               string     `json:"culprit,omitempty"`
	Range                 []string   `json:"range,omitempty"`
	Reason                string     `json:"reason"`
	Signature             string     `json:"signature"`
	FailureRateLowerBound float64    `json:"failure_rate_lower_bound"`
	K                     int        `json:"k"`
	Runs                  int        `json:"runs"`
	Budget                int        `json:"budget"`
	Probes                []Probe    `json:"probes"`
	// Metric bisection only: the settings used and the endpoints' relative
	// median shift.
	Metric *MetricConfig `json:"metric,omitempty"`
	Shift  float64       `json:"shift,omitempty"`
}

// FailureRateLowerBound is an exact one-sided Clopper-Pearson binomial bound.
// It is conditional on independent observations from a stationary failure rate.
func FailureRateLowerBound(failures, trials int, alpha float64) float64 {
	if failures <= 0 || trials < failures || alpha <= 0 || alpha >= 1 {
		return 0
	}
	lo, hi := 0.0, 1.0
	for iter := 0; iter < 70; iter++ {
		p := (lo + hi) / 2
		// log-sum-exp avoids underflow for large calibration samples.
		maxLog := math.Inf(-1)
		logs := make([]float64, trials-failures+1)
		lgN, _ := math.Lgamma(float64(trials + 1))
		for x := failures; x <= trials; x++ {
			lgX, _ := math.Lgamma(float64(x + 1))
			lgRest, _ := math.Lgamma(float64(trials - x + 1))
			l := lgN - lgX - lgRest + float64(x)*math.Log(p) + float64(trials-x)*math.Log1p(-p)
			logs[x-failures] = l
			if l > maxLog {
				maxLog = l
			}
		}
		sum := 0.0
		for _, l := range logs {
			sum += math.Exp(l - maxLog)
		}
		tail := math.Exp(maxLog) * sum
		if tail < alpha {
			lo = p
		} else {
			hi = p
		}
	}
	return lo
}

func Bisect(ctx context.Context, c BisectConfig, runner Runner) (BisectResult, error) {
	if c.Alpha == 0 {
		c.Alpha = .05
	}
	if c.CalibrationFailures == 0 {
		c.CalibrationFailures = 10
	}
	if c.CalibrationRuns == 0 {
		c.CalibrationRuns = 200
	}
	if c.Budget == 0 {
		// Covers the 95th percentile of runs that cmd/remex-bisectsim needs
		// for a 10% flake in 1,000 commits with 10% infrastructure noise.
		c.Budget = 2000
	}
	r := BisectResult{
		Conclusion: ConclusionInconclusive,
		Revisions:  append([]string(nil), c.Revisions...),
		Signature:  c.Signature,
		Budget:     c.Budget,
		Probes:     []Probe{},
	}
	if len(c.Revisions) < 2 ||
		c.Alpha <= 0 ||
		c.Alpha >= 1 ||
		c.CalibrationRuns < 1 ||
		c.CalibrationRuns > 10000 ||
		c.CalibrationFailures < 1 ||
		c.Budget < 1 ||
		runner == nil {
		return r, fmt.Errorf("invalid bisection configuration")
	}
	seen := map[string]bool{}
	for _, rev := range c.Revisions {
		if rev == "" || seen[rev] {
			return r, fmt.Errorf("revisions must be nonempty and unique")
		}
		seen[rev] = true
	}
	if c.Metric != nil {
		return bisectMetric(ctx, c, runner, r)
	}
	exhausted := false
	// probe runs a revision until n valid runs, or until enoughFailures
	// matching failures (0: no limit), or, with earlyBad, the first one.
	probe := func(index, n int, purpose Purpose, earlyBad bool, enoughFailures int) Probe {
		p := Probe{
			Revision:     c.Revisions[index],
			Purpose:      purpose,
			Decision:     DecisionSkip,
			Counts:       map[Category]int{},
			Observations: []Observation{},
		}
		valid, failures := 0, 0
		other := false
		for valid < n {
			if r.Runs >= c.Budget || ctx.Err() != nil {
				exhausted = true
				break
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
				valid++
			case TestFailure, Timeout:
				if r.Signature == "" && purpose == PurposeCalibration {
					r.Signature = o.Signature
				}
				if o.Signature != "" && o.Signature == r.Signature {
					valid++
					failures++
					if earlyBad || (enoughFailures > 0 && failures >= enoughFailures) {
						p.Decision = DecisionBad
						r.Probes = append(r.Probes, p)
						return p
					}
				} else {
					other = true
				}
			default: // Infrastructure and cancellation consume budget but no statistical evidence.
			}
			// A different failure signature makes this revision unclassifiable.
			if other {
				break
			}
		}
		if !exhausted && !other && valid == n {
			if failures > 0 {
				p.Decision = DecisionBad
			} else {
				p.Decision = DecisionGood
			}
		}
		r.Probes = append(r.Probes, p)
		return p
	}
	bad := len(c.Revisions) - 1
	cal := probe(bad, c.CalibrationRuns, PurposeCalibration, false, c.CalibrationFailures)
	if exhausted {
		r.Reason = "budget exhausted or context cancelled during calibration"
		return r, nil
	}
	if cal.Decision != DecisionBad || r.Signature == "" {
		r.Reason = "known-bad revision did not reproduce the target signature"
		return r, nil
	}
	failures, trials := 0, 0
	for _, o := range cal.Observations {
		if o.Category == Pass {
			trials++
		} else if (o.Category == TestFailure || o.Category == Timeout) && o.Signature == r.Signature {
			failures++
			trials++
		}
	}
	// Stopping at the r-th failure keeps this bound exact: the chance that the
	// r-th failure comes within n runs is P(Binomial(n, p) >= r).
	r.FailureRateLowerBound = FailureRateLowerBound(failures, trials, c.Alpha)
	if r.FailureRateLowerBound <= 0 {
		r.Reason = "no positive failure-rate bound"
		return r, nil
	}
	kFloat := math.Ceil(math.Log(c.Alpha) / math.Log1p(-r.FailureRateLowerBound))
	if math.IsInf(kFloat, 0) || kFloat > float64(c.Budget) {
		r.Reason = "budget insufficient for conservative probe size"
		return r, nil
	}
	r.K = int(kFloat)
	if r.K < 1 {
		r.K = 1
	}
	goodProbe := probe(0, r.K, PurposeKnownGood, false, 0)
	if goodProbe.Decision != DecisionGood {
		r.Reason = "known-good baseline is not confirmed"
		return r, nil
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
		p := probe(mid, r.K, PurposeSearch, true, 0)
		switch p.Decision {
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
		r.Reason = "different failure signatures leave unclassifiable commits"
		return r, nil
	}
	// Confirmation is fresh, complete 2k samples on each side; early stopping is forbidden.
	parent := probe(good, 2*r.K, PurposeConfirmParent, false, 0)
	child := probe(bad, 2*r.K, PurposeConfirmCulprit, false, 0)
	if exhausted || parent.Decision != DecisionGood || child.Decision != DecisionBad {
		r.Reason = "fresh 2k confirmation failed or budget exhausted"
		return r, nil
	}
	r.Conclusion = ConclusionCulprit
	r.Culprit = c.Revisions[bad]
	r.Reason = "adjacent revisions passed fresh 2k confirmation"
	return r, nil
}
