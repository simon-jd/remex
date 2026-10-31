// restartprobe replays an expired worker token against an isolated test service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	w "github.com/simon-jd/remex/api/worker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	address := flag.String("scheduler", "", "isolated test scheduler")
	worker := flag.String("worker", "", "expired worker ID")
	attempt := flag.String("attempt", "", "expired attempt ID")
	epoch := flag.Int64("epoch", 0, "expired epoch")
	expected := flag.String("expected-complete", "FENCED", "FENCED for expired tokens, ACCEPTED for replaying the committed winner")
	flag.Parse()
	if *address == "" || *worker == "" || *attempt == "" || *epoch < 1 {
		panic("all stale token fields are required")
	}
	conn, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := w.NewWorkerServiceClient(conn)
	heartbeat, err := client.Heartbeat(ctx, &w.HeartbeatRequest{WorkerId: *worker, AttemptId: *attempt, Epoch: *epoch, Phase: w.HeartbeatRequest_RUNNING})
	if err != nil {
		panic(err)
	}
	complete, err := client.Complete(ctx, &w.CompleteRequest{WorkerId: *worker, AttemptId: *attempt, Epoch: *epoch, CgroupEmpty: true, AttemptDir: "/independent/stale-replay/" + *attempt})
	if err != nil {
		panic(err)
	}
	source := "deliberate old-token replay after scheduler recovery"
	if *expected == "ACCEPTED" {
		source = "deliberate duplicate Complete replay of the committed winner"
	}
	result := map[string]any{"attempt_id": *attempt, "epoch": *epoch, "heartbeat_decision": heartbeat.Decision.String(), "complete_decision": complete.Decision.String(), "complete_message": complete.Message, "source": source}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		panic(err)
	}
	if heartbeat.Decision != w.HeartbeatResponse_FENCED || complete.Decision.String() != *expected {
		fmt.Fprintln(os.Stderr, "unexpected completed/expired token replay outcome")
		os.Exit(1)
	}
}
