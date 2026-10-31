// remex-metrics summarizes measured scheduler timestamps and original REAPI
// execution metadata. It does not treat inferred or missing phases as zero.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"
)

type distribution struct {
	Count int     `json:"count"`
	Sum   float64 `json:"sum_seconds"`
	P50   float64 `json:"p50_seconds"`
	P95   float64 `json:"p95_seconds"`
	Max   float64 `json:"max_seconds"`
}

func summarize(values []float64) distribution {
	sort.Float64s(values)
	d := distribution{Count: len(values)}
	if len(values) == 0 {
		return d
	}
	for _, v := range values {
		d.Sum += v
	}
	d.P50 = values[int(math.Ceil(float64(len(values))*.5))-1]
	d.P95 = values[int(math.Ceil(float64(len(values))*.95))-1]
	d.Max = values[len(values)-1]
	return d
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	file := flag.String("ledger", "", "original ledger export")
	inv := flag.String("invocation", "", "optional exact Bazel invocation UUID")
	flag.Parse()
	raw, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var ledger struct {
		Executions []struct {
			ID            string    `json:"id"`
			Created       time.Time `json:"created_at"`
			Winner        string    `json:"winner_attempt_id"`
			InvocationIDs []string  `json:"invocation_ids"`
		} `json:"executions"`
		Attempts []struct {
			ID          string     `json:"id"`
			ExecutionID string     `json:"execution_id"`
			Started     *time.Time `json:"command_started_at"`
			Result      []byte     `json:"result"`
			Resources   string     `json:"resource_usage_json"`
		} `json:"attempts"`
		Events []struct {
			Kind      string    `json:"kind"`
			AttemptID string    `json:"attempt_id"`
			Time      time.Time `json:"time"`
		} `json:"events"`
	}
	if err = json.Unmarshal(raw, &ledger); err != nil {
		return err
	}
	created := map[string]time.Time{}
	winner := map[string]string{}
	for _, e := range ledger.Executions {
		match := *inv == ""
		for _, id := range e.InvocationIDs {
			if id == *inv {
				match = true
			}
		}
		if match {
			created[e.ID] = e.Created
			winner[e.ID] = e.Winner
		}
	}
	leases, published := map[string]time.Time{}, map[string]time.Time{}
	for _, e := range ledger.Events {
		if e.Kind == "lease_granted" {
			leases[e.AttemptID] = e.Time
		}
		if e.Kind == "ac_published" {
			published[e.AttemptID] = e.Time
		}
	}
	metrics := map[string][]float64{}
	add := func(name string, start, end time.Time) {
		if !start.IsZero() && !end.IsZero() && !end.Before(start) {
			metrics[name] = append(metrics[name], end.Sub(start).Seconds())
		}
	}
	missing := 0
	attempts := 0
	cpuSeconds := 0.0
	for _, a := range ledger.Attempts {
		c, ok := created[a.ExecutionID]
		if !ok {
			continue
		}
		attempts++
		add("queue_to_lease", c, leases[a.ID])
		if a.Started != nil {
			add("lease_to_command", leases[a.ID], *a.Started)
		}
		if len(a.Result) == 0 {
			missing++
			continue
		}
		var result re.ActionResult
		if err = proto.Unmarshal(a.Result, &result); err != nil {
			return err
		}
		m := result.ExecutionMetadata
		if m == nil {
			missing++
			continue
		}
		if m.InputFetchStartTimestamp != nil && m.InputFetchCompletedTimestamp != nil {
			add("input_fetch", m.InputFetchStartTimestamp.AsTime(), m.InputFetchCompletedTimestamp.AsTime())
		}
		if m.ExecutionStartTimestamp != nil && m.ExecutionCompletedTimestamp != nil {
			add("command_execution", m.ExecutionStartTimestamp.AsTime(), m.ExecutionCompletedTimestamp.AsTime())
		}
		if m.OutputUploadStartTimestamp != nil && m.OutputUploadCompletedTimestamp != nil {
			add("output_upload", m.OutputUploadStartTimestamp.AsTime(), m.OutputUploadCompletedTimestamp.AsTime())
		}
		if winner[a.ExecutionID] == a.ID && m.WorkerCompletedTimestamp != nil {
			add("report_to_ac_published", m.WorkerCompletedTimestamp.AsTime(), published[a.ID])
		}
		var resources struct {
			CPU map[string]float64 `json:"cpu"`
		}
		if json.Unmarshal([]byte(a.Resources), &resources) == nil {
			cpuSeconds += resources.CPU["usage_usec"] / 1e6
		}
	}
	distributions := map[string]distribution{}
	for name, values := range metrics {
		distributions[name] = summarize(values)
	}
	output := map[string]any{"source": *file, "invocation": *inv, "executions": len(created), "attempts": attempts, "missing_result_metadata": missing, "worker_command_cpu_seconds": cpuSeconds, "distributions": distributions, "scope": "Wall-clock phase deltas require synchronized clocks; this report's single-host benchmark satisfies that condition. Missing phases are excluded, not inferred as zero. report_to_ac_published includes CAS verification, DB commit, and AC RPC, not only scheduler CPU."}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}
