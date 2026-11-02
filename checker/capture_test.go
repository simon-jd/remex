package checker

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type recordingCache struct {
	re.UnimplementedActionCacheServer
}

func (recordingCache) UpdateActionResult(
	ctx context.Context,
	r *re.UpdateActionResultRequest,
) (*re.ActionResult, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get("x-remex-attempt-id")) == 0 {
		return nil, context.Canceled
	}
	return r.ActionResult, nil
}

func TestTransparentRecorderCapturesActualGRPCFrames(t *testing.T) {
	backendListener := bufconn.Listen(1 << 20)
	backend := grpc.NewServer()
	re.RegisterActionCacheServer(backend, recordingCache{})
	go backend.Serve(backendListener)
	defer backend.Stop()
	dial := func(listener *bufconn.Listener) *grpc.ClientConn {
		t.Helper()
		conn, err := grpc.NewClient(
			"passthrough:///buffer",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		)
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}
	upstream := dial(backendListener)
	defer upstream.Close()
	var raw bytes.Buffer
	recorder := NewRecorder(&raw, "")
	proxyListener := bufconn.Listen(1 << 20)
	proxy := recorder.Server("cache", upstream)
	go proxy.Serve(proxyListener)
	defer proxy.Stop()
	clientConn := dial(proxyListener)
	defer clientConn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(
		ctx,
		metadata.Pairs("x-remex-attempt-id", "a", "x-remex-execution-id", "e", "x-remex-epoch", "1"),
	)
	want := &re.ActionResult{StdoutRaw: []byte("actual grpc transport")}
	got, err := re.NewActionCacheClient(clientConn).UpdateActionResult(
		ctx,
		&re.UpdateActionResultRequest{
			InstanceName: "tenant-a",
			ActionDigest: &re.Digest{Hash: "key", SizeBytes: 3},
			ActionResult: want,
		},
	)
	if err != nil || !proto.Equal(want, got) {
		t.Fatalf("proxy changed RPC: %v %v", got, err)
	}
	records, err := ReadCapture(bytes.NewReader(raw.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	adapted, err := AdaptCapture(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(adapted.Evidence) != 1 ||
		adapted.Evidence[0].Kind != "ac_write" ||
		adapted.Evidence[0].InstanceName != "tenant-a" {
		t.Fatalf("unexpected capture: %+v", adapted)
	}
	var observed re.ActionResult
	if err = proto.Unmarshal(adapted.Evidence[0].Result, &observed); err != nil || !proto.Equal(want, &observed) {
		t.Fatal("raw result lost")
	}
	if acAfterCommit(adapted.Evidence[0], Event{Sequence: 1}) {
		t.Fatal("missing durable snapshot must not establish ordering")
	}
}

func TestRecorderTerminalStatusClosesCaptureForCall(t *testing.T) {
	var raw bytes.Buffer
	r := NewRecorder(&raw, "")
	for _, kind := range []string{"start", "request", "end"} {
		if err := r.write(CaptureRecord{CallID: 1, Method: "/example.Service/Call", Kind: kind}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.write(CaptureRecord{
		CallID: 1,
		Method: "/example.Service/Call",
		Kind:   "request_forwarded",
	}); err == nil {
		t.Fatal("late forwarding goroutine appended after terminal status")
	}
	records, err := ReadCapture(bytes.NewReader(raw.Bytes()))
	if err != nil || len(records) != 3 || records[2].Kind != "end" {
		t.Fatalf("terminal framing: %+v %v", records, err)
	}
}

func TestAmbiguousACWriteRemainsMachineReadableGap(t *testing.T) {
	request, _ := proto.Marshal(&re.UpdateActionResultRequest{
		InstanceName: "tenant-a",
		ActionResult: &re.ActionResult{ExitCode: 1},
	})
	method := "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult"
	records := []CaptureRecord{
		{CaptureID: "c", Sequence: 1, CallID: 1, Method: method, Kind: "start"},
		{CaptureID: "c", Sequence: 2, CallID: 1, Method: method, Kind: "request", Message: request},
		{CaptureID: "c", Sequence: 3, CallID: 1, Method: method, Kind: "end", StatusCode: 4},
	}
	adapted, err := AdaptCapture(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(adapted.Evidence) != 1 || adapted.Evidence[0].Kind != "ac_write_attempt" {
		t.Fatal(adapted)
	}
	l, rows := goodFixture()
	rows = append(rows, adapted.Evidence...)
	if result := Check(l, rows, nil); result.OK || len(result.MissingEvidence) == 0 {
		t.Fatal("ambiguous mutating RPC accepted")
	}
	records = records[:2]
	adapted, err = AdaptCapture(records)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range adapted.Evidence {
		found = found || row.Kind == "evidence_gap"
	}
	if !found {
		t.Fatal("truncated capture gap omitted from machine-readable output")
	}
}

func TestCapturedDurableSnapshotsEstablishOnlyCorrectBoundary(t *testing.T) {
	l, rows := goodFixture()
	before := l
	before.Events = append([]Event{}, l.Events[:2]...)
	before.Executions = append([]Execution{}, l.Executions...)
	before.Executions[0].WinnerAttemptID = ""
	for i := range rows {
		rows[i].OrderDomain = "capture"
		rows[i].RawRecordIDs = []int64{1, 2, 3}
		rows[i].Sequence *= 100
	}
	rows[3].LedgerSnapshot = &before
	rows[4].LedgerSnapshot = &l
	if r := Check(l, rows, nil); !r.OK {
		data, _ := json.Marshal(r)
		t.Fatal(string(data))
	}
	rows[3].LedgerSnapshot = &l
	if r := Check(l, rows, nil); r.OK {
		t.Fatal("CAS observed after durable commit accepted")
	}
}

func TestFencedSubmissionNeedNotBeStored(t *testing.T) {
	l, rows := goodFixture()
	l.Executions[0].WinnerAttemptID = ""
	l.Executions[0].CurrentEpoch = 2
	l.Attempts[0].Published = false
	l.Attempts[0].Result = nil
	l.Attempts[0].ResultDigests = nil
	l.Attempts[0].AttemptDir = ""
	l.Events = l.Events[:2]
	rows = rows[:3]
	rows[2].CompleteDecision = "FENCED"
	if r := Check(l, rows, nil); !r.OK {
		t.Fatalf("rejected submission wrongly required in ledger: %+v", r)
	}
}

func TestRawCacheInstanceMustMatchExecution(t *testing.T) {
	l, rows := goodFixture()
	rows[4].InstanceName = "different"
	if r := Check(l, rows, nil); r.OK {
		t.Fatal("cross-instance cache write accepted")
	}
}

func TestCaptureFramingCannotLoseOrMergeRPCs(t *testing.T) {
	method := "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult"
	base := []CaptureRecord{
		{CaptureID: "c", Sequence: 1, CallID: 1, Endpoint: "cache", Method: method, Kind: "start"},
		{CaptureID: "c", Sequence: 2, CallID: 1, Endpoint: "cache", Method: method, Kind: "request", Message: []byte{}},
		{CaptureID: "c", Sequence: 3, CallID: 1, Endpoint: "cache", Method: method, Kind: "end", StatusCode: 4},
	}
	tests := map[string]func([]CaptureRecord) []CaptureRecord{
		"missing-start": func(r []CaptureRecord) []CaptureRecord {
			r = r[1:]
			for i := range r {
				r[i].Sequence = int64(i + 1)
			}
			return r
		},
		"gap": func(r []CaptureRecord) []CaptureRecord { return []CaptureRecord{r[0], r[2]} },
		"non-one-start": func(r []CaptureRecord) []CaptureRecord {
			for i := range r {
				r[i].Sequence += 10
			}
			return r
		},
		"duplicate-start": func(r []CaptureRecord) []CaptureRecord { r[1].Kind = "start"; return r },
		"duplicate-end": func(r []CaptureRecord) []CaptureRecord {
			copy := r[2]
			copy.Sequence = 4
			return append(r, copy)
		},
		"method-changed":   func(r []CaptureRecord) []CaptureRecord { r[1].Method = "/other/Call"; return r },
		"endpoint-changed": func(r []CaptureRecord) []CaptureRecord { r[1].Endpoint = "worker"; return r },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			records := mutate(append([]CaptureRecord(nil), base...))
			if _, err := AdaptCapture(records); err == nil {
				t.Fatal("malformed capture silently accepted")
			}
		})
	}
}

func TestWarmTreeComesFromActualBatchReadResponse(t *testing.T) {
	l, _ := goodFixture()
	treeBytes, _ := proto.Marshal(&re.Tree{Root: &re.Directory{}})
	digest := hashBytes(treeBytes)
	resultBytes, _ := proto.Marshal(&re.ActionResult{
		OutputDirectories: []*re.OutputDirectory{
			{Path: "out", TreeDigest: &re.Digest{Hash: digest.Hash, SizeBytes: digest.SizeBytes}},
		},
	})
	batchRequest, _ := proto.Marshal(&re.BatchReadBlobsRequest{
		Digests: []*re.Digest{{Hash: digest.Hash, SizeBytes: digest.SizeBytes}},
	})
	batchResponse, _ := proto.Marshal(&re.BatchReadBlobsResponse{
		Responses: []*re.BatchReadBlobsResponse_Response{
			{Digest: &re.Digest{Hash: digest.Hash, SizeBytes: digest.SizeBytes}, Data: treeBytes},
		},
	})
	bytesField := func(wire []byte, n protowire.Number, data []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(wire, n, protowire.BytesType), data)
	}
	numberField := func(wire []byte, n protowire.Number, value uint64) []byte {
		return protowire.AppendVarint(protowire.AppendTag(wire, n, protowire.VarintType), value)
	}
	lease := numberField(nil, 1, 1)
	lease = bytesField(lease, 2, []byte("e"))
	lease = bytesField(lease, 3, []byte("a"))
	lease = numberField(lease, 4, 1)
	ad := bytesField(nil, 1, []byte(l.Executions[0].ActionDigest.Hash))
	ad = numberField(ad, 2, uint64(l.Executions[0].ActionDigest.SizeBytes))
	lease = bytesField(lease, 7, ad)
	lease = bytesField(lease, 8, l.Executions[0].Action)
	complete := bytesField(nil, 2, []byte("a"))
	complete = numberField(complete, 3, 1)
	complete = bytesField(complete, 4, resultBytes)
	complete = bytesField(complete, 12, []byte("/attempt/a"))
	var records []CaptureRecord
	add := func(call int64, method, kind string, data []byte) {
		records = append(
			records,
			CaptureRecord{
				CaptureID: "batch",
				Sequence:  int64(len(records) + 1),
				CallID:    call,
				Method:    method,
				Kind:      kind,
				Message:   data,
			},
		)
	}
	lm := "/remex.worker.v1.WorkerService/Lease"
	add(1, lm, "start", nil)
	add(1, lm, "request", []byte{})
	add(1, lm, "response", lease)
	add(1, lm, "end", nil)
	bm := "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs"
	add(2, bm, "start", nil)
	add(2, bm, "request", batchRequest)
	add(2, bm, "response", batchResponse)
	add(2, bm, "end", nil)
	cm := "/remex.worker.v1.WorkerService/Complete"
	add(3, cm, "start", nil)
	add(3, cm, "request", complete)
	add(3, cm, "request_forwarded", nil)
	add(3, cm, "response", []byte{})
	add(3, cm, "end", nil)
	adapted, err := AdaptCapture(records)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range adapted.Evidence {
		if row.Kind == "result_reported" {
			found = len(row.Trees) == 1 && bytes.Equal(row.Trees[0].Data, treeBytes)
		}
	}
	if !found {
		t.Fatalf("warm BatchRead Tree evidence lost: %+v", adapted)
	}
}

func TestIndependentKernelOOMCannotBeHidden(t *testing.T) {
	l, rows := goodFixture()
	oom := int64(1)
	rows[1].OOMKill = &oom
	if r := Check(l, rows, nil); r.OK {
		t.Fatal("ledger hid independently observed OOM")
	}
}

func TestPostCommitDuplicateFencedIsNotAStaleCommit(t *testing.T) {
	l, rows := goodFixture()
	duplicate := rows[2]
	duplicate.CompleteDecision = "FENCED"
	duplicate.LedgerSnapshot = &l
	rows = append(rows, duplicate)
	if r := Check(l, rows, nil); !r.OK {
		t.Fatalf("post-commit duplicate falsely violated fencing: %+v", r)
	}
}
