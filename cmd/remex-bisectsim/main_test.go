package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"
)

func tenRevisions() *world {
	w := &world{}
	for i := range 10 {
		w.revisions = append(w.revisions, fmt.Sprintf("r%d", i))
	}
	return w
}

// A deterministic culprit at 5: skipping a revision away from the boundary
// still finds it, and skipping the only revision that separates good from
// bad leaves git unable to name it.
func TestGitBisectSkip(t *testing.T) {
	for _, c := range []struct{ skip, want int }{{6, 5}, {2, 5}, {4, -1}} {
		probe := func(index int) verdict {
			switch {
			case index == c.skip:
				return verdictSkip
			case index >= 5:
				return verdictBad
			}
			return verdictGood
		}
		if got := tenRevisions().gitBisect(probe); got != c.want {
			t.Errorf("skip %d: found %d, want %d", c.skip, got, c.want)
		}
	}
}

// Zero wrong answers in 1,000 trials bounds the wrong rate at 1-0.05^(1/1000).
func TestWrongUpper95(t *testing.T) {
	want := 1 - math.Pow(0.05, 1.0/1000)
	if got := wrongUpper95(0, 1000); math.Abs(got-want) > 1e-6 {
		t.Fatalf("wrongUpper95(0, 1000) = %v, want %v", got, want)
	}
	if got := wrongUpper95(1000, 1000); got != 1 {
		t.Fatalf("wrongUpper95(1000, 1000) = %v, want 1", got)
	}
}

// The reported upper bound matches scipy's beta.ppf(0.95, k + 1, n - k)
// (regress/testdata/scipy-reference.json, from scripts/scipy-reference.py).
func TestWrongUpper95MatchesScipy(t *testing.T) {
	raw, err := os.ReadFile("../../regress/testdata/scipy-reference.json")
	if err != nil {
		t.Fatal(err)
	}
	var reference struct {
		Upper []struct {
			K, N    int
			Upper95 float64
		} `json:"clopper_pearson_upper95"`
	}
	if err := json.Unmarshal(raw, &reference); err != nil || len(reference.Upper) == 0 {
		t.Fatalf("scipy reference: %v", err)
	}
	for _, c := range reference.Upper {
		if got := wrongUpper95(c.K, c.N); math.Abs(got-c.Upper95) > 1e-9 {
			t.Errorf("wrongUpper95(%d, %d) = %.12g, scipy %.12g", c.K, c.N, got, c.Upper95)
		}
	}
}
