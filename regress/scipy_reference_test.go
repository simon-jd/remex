package regress

import (
	"encoding/json"
	"math"
	"os"
	"testing"
)

// ScipyReference holds the values scripts/scipy-reference.py computed with
// scipy; testdata/scipy-reference.json records the scipy version.
type ScipyReference struct {
	Lower []struct {
		Failures, Trials int
		Alpha, Lower     float64
	} `json:"clopper_pearson_lower"`
	Upper []struct {
		K, N    int
		Upper95 float64
	} `json:"clopper_pearson_upper95"`
	MannWhitney []struct {
		X, Y       []float64
		Exact      *float64
		Asymptotic float64
	} `json:"mann_whitney_greater"`
}

func ReadScipyReference(t *testing.T, path string) ScipyReference {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r ScipyReference
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	if len(r.Lower) == 0 || len(r.Upper) == 0 || len(r.MannWhitney) == 0 {
		t.Fatal("empty scipy reference")
	}
	return r
}

func TestFailureRateLowerBoundMatchesScipy(t *testing.T) {
	for _, c := range ReadScipyReference(t, "testdata/scipy-reference.json").Lower {
		if got := FailureRateLowerBound(c.Failures, c.Trials, c.Alpha); math.Abs(got-c.Lower) > 1e-9 {
			t.Errorf("%d/%d at alpha %v: %.12g, scipy %.12g", c.Failures, c.Trials, c.Alpha, got, c.Lower)
		}
	}
}

func TestMannWhitneyMatchesScipy(t *testing.T) {
	for _, c := range ReadScipyReference(t, "testdata/scipy-reference.json").MannWhitney {
		if got := mannWhitneyAsymptotic(c.X, c.Y); math.Abs(got-c.Asymptotic) > 1e-12 {
			t.Errorf("asymptotic %v vs %v: %.15g, scipy %.15g", c.X, c.Y, got, c.Asymptotic)
		}
		if c.Exact == nil {
			if _, ties := mannWhitneyU(c.X, c.Y); !ties {
				t.Errorf("%v vs %v: no ties, but scipy gave no exact value", c.X, c.Y)
			}
			continue
		}
		if got := MannWhitneyGreater(c.X, c.Y); math.Abs(got-*c.Exact) > 1e-12*math.Max(1, *c.Exact) {
			t.Errorf("exact %v vs %v: %.15g, scipy %.15g", c.X, c.Y, got, *c.Exact)
		}
	}
}
