package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/simon-jd/remex/checker"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "remex-adapt:", err)
		os.Exit(1)
	}
}
func run() error {
	input := flag.String("capture", "", "original recorder JSONL")
	flag.Parse()
	file, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer file.Close()
	records, err := checker.ReadCapture(file)
	if err != nil {
		return err
	}
	result, err := checker.AdaptCapture(records)
	if err != nil {
		return err
	}
	for _, gap := range result.Gaps {
		fmt.Fprintln(os.Stderr, "evidence gap:", gap)
	}
	encoder := json.NewEncoder(os.Stdout)
	for _, row := range result.Evidence {
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	return nil
}
