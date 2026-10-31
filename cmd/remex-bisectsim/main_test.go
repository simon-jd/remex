package main

import (
	"fmt"
	"math"
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
