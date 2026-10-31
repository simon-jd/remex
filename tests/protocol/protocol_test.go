package protocol_test

import (
	"context"
	"io"
	"testing"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"github.com/simon-jd/remex/tests/protocol/testkit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDisconnectedExecuteCanReconnectThroughWaitExecution(t *testing.T) {
	h := testkit.New(t, nil)
	capabilities, err := h.Capabilities.GetCapabilities(h.Ctx, &re.GetCapabilitiesRequest{InstanceName: testkit.Instance})
	if err != nil || !capabilities.GetExecutionCapabilities().GetExecEnabled() {
		t.Fatalf("capabilities = %v, %v", capabilities, err)
	}
	ctx, cancel := context.WithCancel(h.Ctx)
	stream := h.Execute(ctx)
	op, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	lease := h.Lease()
	output := h.Cache.Put([]byte("independent output\n"))
	req := h.Request(lease, &re.ActionResult{StdoutDigest: output})
	if r := h.Complete(req); r.Decision != w.CompleteResponse_ACCEPTED {
		t.Fatal(r)
	}
	wait, err := h.Execution.WaitExecution(h.Ctx, &re.WaitExecutionRequest{Name: op.Name})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := wait.Recv()
	if err != nil {
		t.Fatal(err)
	}
	var response re.ExecuteResponse
	if !completed.Done || completed.GetResponse().UnmarshalTo(&response) != nil || response.GetStatus().GetCode() != 0 || response.GetResult().GetStdoutDigest().GetHash() != output.Hash {
		t.Fatalf("completed operation = %v", completed)
	}
	if _, err := wait.Recv(); err != io.EOF {
		t.Fatalf("terminal stream = %v", err)
	}
	if h.Cache.Writes() != 1 {
		t.Fatalf("AC writes = %d", h.Cache.Writes())
	}
}

func TestCorruptedActionCannotCreateExecution(t *testing.T) {
	h := testkit.New(t, nil)
	h.Cache.Corrupt(h.Digest)
	_, err := h.Execute(h.Ctx).Recv()
	if status.Code(err) != codes.DataLoss {
		t.Fatalf("corrupt Action error = %v", err)
	}
	if len(h.Scheduler.Export().Executions) != 0 || h.Cache.Writes() != 0 {
		t.Fatal("corruption reached ledger or AC")
	}
}

func TestUnknownOperationHasNotFoundStreamStatus(t *testing.T) {
	h := testkit.New(t, nil)
	stream, err := h.Execution.WaitExecution(h.Ctx, &re.WaitExecutionRequest{Name: "executions/does-not-exist"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown operation = %v", err)
	}
}
