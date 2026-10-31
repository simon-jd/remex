package faults_test

import (
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRealWorkerMetadataReflectsObservedPhases(t *testing.T) {
	before := time.Now()
	h := newRealWorker(t, false)
	stream := h.execute(t, []string{"/bin/sh", "-c", "sleep 0.15; printf measured-phase-output"}, 5*time.Second)
	for {
		op, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if !op.Done {
			continue
		}
		after := time.Now()
		var response re.ExecuteResponse
		if err := op.GetResponse().UnmarshalTo(&response); err != nil {
			t.Fatal(err)
		}
		if response.GetStatus().GetCode() != 0 || response.GetResult().GetExitCode() != 0 {
			t.Fatalf("command failed: %v", &response)
		}
		metadata := response.GetResult().GetExecutionMetadata()
		if metadata == nil || metadata.Worker != "independent-cpp" {
			t.Fatalf("missing actual worker metadata: %v", metadata)
		}
		// The queue time is the scheduler's durable creation time, passed on
		// with the lease: after submission and no later than the worker start.
		if q := metadata.QueuedTimestamp; q == nil || q.AsTime().Before(before.Add(-time.Second)) || q.AsTime().After(metadata.WorkerStartTimestamp.AsTime()) {
			t.Fatalf("queued timestamp %v is not the scheduler's creation time (worker start %v)", q, metadata.WorkerStartTimestamp)
		}
		phases := []struct {
			name      string
			timestamp *timestamppb.Timestamp
		}{
			{"worker_start", metadata.WorkerStartTimestamp},
			{"input_fetch_start", metadata.InputFetchStartTimestamp},
			{"input_fetch_completed", metadata.InputFetchCompletedTimestamp},
			{"execution_start", metadata.ExecutionStartTimestamp},
			{"execution_completed", metadata.ExecutionCompletedTimestamp},
			{"output_upload_start", metadata.OutputUploadStartTimestamp},
			{"output_upload_completed", metadata.OutputUploadCompletedTimestamp},
			{"worker_completed", metadata.WorkerCompletedTimestamp},
		}
		last := before
		nanosecondsObserved := false
		for _, phase := range phases {
			if phase.timestamp == nil || phase.timestamp.CheckValid() != nil {
				t.Fatalf("missing or invalid %s timestamp", phase.name)
			}
			observed := phase.timestamp.AsTime()
			if observed.Before(last) || observed.After(after) {
				t.Fatalf("phase %s=%s outside monotonic observed wall-time order [%s,%s]", phase.name, observed, last, after)
			}
			nanosecondsObserved = nanosecondsObserved || phase.timestamp.Nanos != 0
			last = observed
			t.Logf("%s=%s", phase.name, observed.Format(time.RFC3339Nano))
		}
		if !nanosecondsObserved {
			t.Fatal("all phase timestamps lost sub-second precision")
		}
		duration := metadata.ExecutionCompletedTimestamp.AsTime().Sub(metadata.ExecutionStartTimestamp.AsTime())
		if duration < 140*time.Millisecond {
			t.Fatalf("execution timestamp duration %s cannot contain actual 150ms sleep", duration)
		}
		t.Logf("observed execution duration=%s", duration)
		return
	}
}
