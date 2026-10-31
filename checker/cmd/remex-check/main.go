package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/simon-jd/remex/checker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "remex-check:", err)
		os.Exit(1)
	}
}
func run() error {
	ledgerPath := flag.String("ledger", "", "ledger export JSON (required)")
	evidencePaths := flag.String("evidence", "", "comma-separated independent normalized JSONL logs")
	regressionPaths := flag.String("regression", "", "comma-separated bisection report JSON files")
	flag.Parse()
	f, err := os.Open(*ledgerPath)
	if err != nil {
		return err
	}
	var ledger checker.Ledger
	err = json.NewDecoder(f).Decode(&ledger)
	f.Close()
	if err != nil {
		return err
	}
	var evidence []checker.Evidence
	if *evidencePaths != "" {
		for _, name := range strings.Split(*evidencePaths, ",") {
			f, err := os.Open(name)
			if err != nil {
				return err
			}
			dec := json.NewDecoder(f)
			for {
				var row checker.Evidence
				err := dec.Decode(&row)
				if err == io.EOF {
					break
				}
				if err != nil {
					f.Close()
					return err
				}
				evidence = append(evidence, row)
			}
			f.Close()
		}
	}
	var reports []checker.RegressionReport
	if *regressionPaths != "" {
		for _, name := range strings.Split(*regressionPaths, ",") {
			f, err := os.Open(name)
			if err != nil {
				return err
			}
			var report checker.RegressionReport
			err = json.NewDecoder(f).Decode(&report)
			f.Close()
			if err != nil {
				return err
			}
			reports = append(reports, report)
		}
	}
	report := checker.Check(ledger, evidence, reports)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return err
	}
	return report.Error()
}
