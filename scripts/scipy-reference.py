#!/usr/bin/env python3
"""Reference values from scipy for the statistics in regress/.

    python3 scripts/scipy-reference.py > regress/testdata/scipy-reference.json

(With uv: uv run --no-project --with scipy python scripts/scipy-reference.py.)

regress/scipy_reference_test.go checks FailureRateLowerBound (the one-sided
Clopper-Pearson lower bound, beta.ppf(alpha, k, n - k + 1)), the matching
upper bound that cmd/remex-bisectsim reports, and MannWhitneyGreater
(mannwhitneyu with alternative="greater", exact and asymptotic) against
these values.
"""
import json
import sys

import numpy as np
import scipy
from scipy import stats

bounds = []
for failures, trials in [(1, 1), (1, 20), (5, 20), (10, 10), (10, 37), (10, 200), (10, 2000), (3, 1000),
                         (50, 60), (199, 200), (999, 1000)]:
    for alpha in (0.05, 0.01):
        bounds.append(dict(failures=failures, trials=trials, alpha=alpha,
                           lower=float(stats.beta.ppf(alpha, failures, trials - failures + 1))))

upper = []
for k, n in [(0, 1000), (1, 1000), (1, 16000), (7, 1000), (37, 1000), (999, 1000)]:
    upper.append(dict(k=k, n=n, upper95=float(stats.beta.ppf(0.95, k + 1, n - k))))

rng = np.random.default_rng(20261102)
samples = [
    ([3.0], [1.0, 2.0]),
    ([1.1, 2.2, 3.3, 4.4], [0.5, 2.5, 6.0]),
    (list(rng.normal(1.0, 0.1, 8)), list(rng.normal(1.1, 0.1, 8))),
    (list(rng.normal(1.3, 0.05, 10)), list(rng.normal(1.0, 0.05, 10))),
    (list(rng.normal(1.0, 0.05, 20)), list(rng.normal(1.02, 0.05, 30))),
    (list(rng.normal(1.0, 0.05, 50)), list(rng.normal(1.0, 0.05, 50))),
]
mann_whitney = []
for x, y in samples:
    x, y = [round(v, 6) for v in x], [round(v, 6) for v in y]
    mann_whitney.append(dict(x=x, y=y, exact=float(stats.mannwhitneyu(x, y, alternative="greater", method="exact").pvalue),
                             asymptotic=float(stats.mannwhitneyu(x, y, alternative="greater", method="asymptotic").pvalue)))
# Ties force the asymptotic method, with scipy's tie correction.
for x, y in [([1, 2, 2, 3, 3, 3], [2, 3, 4, 4, 5]), ([1.5] * 6 + [2.5] * 4, [1.5] * 3 + [3.5] * 7)]:
    mann_whitney.append(dict(x=x, y=y, asymptotic=float(stats.mannwhitneyu(x, y, alternative="greater",
                                                                          method="asymptotic").pvalue)))

json.dump(dict(scipy=scipy.__version__, clopper_pearson_lower=bounds, clopper_pearson_upper95=upper,
               mann_whitney_greater=mann_whitney), sys.stdout, indent=1)
print()
