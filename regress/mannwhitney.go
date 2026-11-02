package regress

import (
	"math"
	"sort"
)

// MannWhitneyGreater returns the one-sided Mann-Whitney U p-value for the
// alternative that x is stochastically greater than y. Without ties and with
// at most 50 values per sample it is exact; otherwise it uses the normal
// approximation with tie and continuity corrections. These are scipy's
// mannwhitneyu(x, y, alternative="greater") with method="exact" and
// method="asymptotic".
func MannWhitneyGreater(x, y []float64) float64 {
	u, ties := mannWhitneyU(x, y)
	if !ties && len(x) <= 50 && len(y) <= 50 {
		return mannWhitneyExact(u, len(x), len(y))
	}
	return mannWhitneyAsymptotic(x, y)
}

// mannWhitneyU returns U for x (the number of pairs with x above y, ties
// counting one half) and whether any two values tie.
func mannWhitneyU(x, y []float64) (float64, bool) {
	ranks, ties := rank(append(append([]float64(nil), x...), y...))
	sum := 0.0
	for _, r := range ranks[:len(x)] {
		sum += r
	}
	n := float64(len(x))
	return sum - n*(n+1)/2, ties
}

// rank returns 1-based ranks, averaged over ties.
func rank(values []float64) ([]float64, bool) {
	order := make([]int, len(values))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool { return values[order[i]] < values[order[j]] })
	ranks := make([]float64, len(values))
	ties := false
	for i := 0; i < len(order); {
		j := i + 1
		for j < len(order) && values[order[j]] == values[order[i]] {
			j++
		}
		if j-i > 1 {
			ties = true
		}
		for k := i; k < j; k++ {
			ranks[order[k]] = float64(i+j+1) / 2
		}
		i = j
	}
	return ranks, ties
}

// mannWhitneyExact returns P(U >= u) when the m + n values are exchangeable.
// p[j][k] is the distribution of U for samples of i and j values, built up
// one i at a time from P_{i,j}(k) = i/(i+j) P_{i-1,j}(k-j) + j/(i+j) P_{i,j-1}(k).
func mannWhitneyExact(u float64, m, n int) float64 {
	prev := make([][]float64, n+1)
	for j := range prev {
		prev[j] = []float64{1} // zero values in x: U is 0
	}
	for i := 1; i <= m; i++ {
		cur := make([][]float64, n+1)
		cur[0] = []float64{1}
		for j := 1; j <= n; j++ {
			d := make([]float64, i*j+1)
			a, b := float64(i)/float64(i+j), float64(j)/float64(i+j)
			for k, p := range prev[j] {
				d[k+j] += a * p
			}
			for k, p := range cur[j-1] {
				d[k] += b * p
			}
			cur[j] = d
		}
		prev = cur
	}
	tail := 0.0
	for k, p := range prev[n] {
		if float64(k) >= u {
			tail += p
		}
	}
	return math.Min(tail, 1)
}

func mannWhitneyAsymptotic(x, y []float64) float64 {
	u, _ := mannWhitneyU(x, y)
	m, n := float64(len(x)), float64(len(y))
	all := append(append([]float64(nil), x...), y...)
	sort.Float64s(all)
	tieTerm := 0.0
	for i := 0; i < len(all); {
		j := i + 1
		for j < len(all) && all[j] == all[i] {
			j++
		}
		t := float64(j - i)
		tieTerm += t*t*t - t
		i = j
	}
	total := m + n
	sd := math.Sqrt(m * n / 12 * ((total + 1) - tieTerm/(total*(total-1))))
	if sd == 0 {
		return 1
	}
	z := (u - m*n/2 - 0.5) / sd
	return 0.5 * math.Erfc(z/math.Sqrt2)
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), values...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}
