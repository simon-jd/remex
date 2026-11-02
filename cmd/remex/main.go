package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/simon-jd/remex/regress"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "remex:", err)
		os.Exit(1)
	}
}
func readJSON(name string, v any) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(v)
}
func output(v any) error { e := json.NewEncoder(os.Stdout); e.SetIndent("", "  "); return e.Encode(v) }
func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: remex classify|select|bisect|export|explain [flags]")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch args[0] {
	case "classify":
		bep := f.String("bep", "", "Bazel build event JSON file")
		ledger := f.String("ledger", "", "optional ledger JSON scoped to this invocation")
		signature := f.String("signature", "", "explicit normalized failure signature")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		file, err := os.Open(*bep)
		if err != nil {
			return err
		}
		defer file.Close()
		events, err := regress.ReadBEP(file)
		if err != nil {
			return err
		}
		var attempts []regress.AttemptEvidence
		if *ledger != "" {
			raw, err := os.ReadFile(*ledger)
			if err != nil {
				return err
			}
			attempts, err = regress.ParseLedgerEvidence(raw, "")
			if err != nil {
				return err
			}
		}
		return output(regress.Classify(events, attempts, *signature))
	case "select":
		input := f.String("input", "", "selection request JSON")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		var request regress.SelectionRequest
		if err := readJSON(*input, &request); err != nil {
			return err
		}
		return output(regress.SelectHardware(request))
	case "bisect":
		config := f.String("config", "", "JSON containing bisect and runner objects")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		var request struct {
			Bisect regress.BisectConfig `json:"bisect"`
			Runner regress.BazelRunner  `json:"runner"`
			Good   string               `json:"good"`
			Bad    string               `json:"bad"`
		}
		if err := readJSON(*config, &request); err != nil {
			return err
		}
		if len(request.Bisect.Revisions) == 0 {
			revisions, err := regress.GitHistory(ctx, request.Runner.Repository, request.Good, request.Bad)
			if err != nil {
				return err
			}
			request.Bisect.Revisions = revisions
		}
		if request.Bisect.Metric != nil && request.Runner.Metric == "" {
			request.Runner.Metric = request.Bisect.Metric.Name
		}
		result, err := regress.Bisect(ctx, request.Bisect, request.Runner)
		if err != nil {
			return err
		}
		return output(result)
	case "export":
		inputs := f.String("inputs", "", "materialized action input root")
		manifest := f.String("manifest", "", "bundle metadata JSON")
		out := f.String("out", "", "new bundle directory")
		ledger := f.String("ledger", "", "ledger export for exact Action.input_root_digest export")
		execution := f.String("execution", "", "execution ID in ledger")
		cas := f.String("cas", "", "original CAS host:port (or grpc:// endpoint)")
		revision := f.String("revision", "", "source revision annotation")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		var b regress.Bundle
		if *manifest != "" {
			if err := readJSON(*manifest, &b); err != nil {
				return err
			}
		}
		if *revision != "" {
			b.Revision = *revision
		}
		if *out == "" {
			return fmt.Errorf("--out is required")
		}
		if *ledger != "" {
			if *inputs != "" || *execution == "" || *cas == "" {
				return fmt.Errorf("ledger export requires --execution and --cas; --inputs must be omitted")
			}
			file, err := os.Open(*ledger)
			if err != nil {
				return err
			}
			defer file.Close()
			if err := regress.ExportActionBundle(ctx, file, *execution, *cas, *out, b); err != nil {
				return err
			}
		} else {
			if *inputs == "" || *manifest == "" || *cas != "" || *execution != "" {
				return fmt.Errorf("materialized export requires --inputs and --manifest; CAS export requires --ledger")
			}
			if err := regress.ExportBundle(*inputs, *out, b); err != nil {
				return err
			}
		}
		return output(map[string]string{"bundle": *out})
	case "explain":
		ledger := f.String("ledger", "", "offline ledger export JSON")
		endpoint := f.String("endpoint", "", "scheduler HTTP base URL")
		id := f.String("execution", "", "execution ID")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if *endpoint != "" {
			client := http.Client{Timeout: 15 * time.Second}
			req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(*endpoint, "/")+"/export", nil)
			if err != nil {
				return err
			}
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("scheduler: %s", resp.Status)
			}
			return explain(resp.Body, *id)
		}
		file, err := os.Open(*ledger)
		if err != nil {
			return err
		}
		defer file.Close()
		return explain(file, *id)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func explain(reader io.Reader, id string) error {
	var ledger struct {
		Executions []map[string]any `json:"executions"`
		Events     []map[string]any `json:"events"`
	}
	if err := json.NewDecoder(reader).Decode(&ledger); err != nil {
		return err
	}
	if id == "" {
		return output(ledger)
	}
	var execution map[string]any
	var events []map[string]any
	for _, e := range ledger.Executions {
		if e["id"] == id {
			execution = e
		}
	}
	if execution == nil {
		return fmt.Errorf("execution %q not found", id)
	}
	for _, e := range ledger.Events {
		if e["execution_id"] == id {
			events = append(events, e)
		}
	}
	return output(map[string]any{"execution": execution, "events": events})
}
