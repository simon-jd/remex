package checker

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

func TestFailedCompleteRetainsSubmissionWithoutInventingDecision(t *testing.T) {
	_, rows := goodFixture()
	b := func(data []byte, n protowire.Number, value []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(data, n, protowire.BytesType), value)
	}
	v := func(data []byte, n protowire.Number, value uint64) []byte {
		return protowire.AppendVarint(protowire.AppendTag(data, n, protowire.VarintType), value)
	}
	lease := v(nil, 1, 1)
	lease = b(lease, 2, []byte("e"))
	lease = b(lease, 3, []byte("a"))
	lease = v(lease, 4, 1)
	complete := b(nil, 2, []byte("a"))
	complete = v(complete, 3, 1)
	complete = b(complete, 4, rows[2].Result)
	complete = b(complete, 12, []byte("/attempt/a"))
	var records []CaptureRecord
	add := func(call int64, method, kind string, message []byte) {
		records = append(
			records,
			CaptureRecord{
				CaptureID: "unknown",
				Sequence:  int64(len(records) + 1),
				CallID:    call,
				Method:    method,
				Kind:      kind,
				Message:   message,
			},
		)
	}
	lm := "/remex.worker.v1.WorkerService/Lease"
	cm := "/remex.worker.v1.WorkerService/Complete"
	add(1, lm, "start", nil)
	add(1, lm, "request", []byte{})
	add(1, lm, "response", lease)
	add(1, lm, "end", nil)
	add(2, cm, "start", nil)
	add(2, cm, "request", complete)
	add(2, cm, "cgroup_observation", nil)
	records[len(records)-1].CgroupEvents = "populated 0\n"
	records[len(records)-1].MemoryEvents = "oom_kill 0\n"
	add(2, cm, "request_forwarded", nil)
	add(2, cm, "end", nil)
	records[len(records)-1].StatusCode = 14
	adapted, err := AdaptCapture(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(adapted.Gaps) != 0 {
		t.Fatal(adapted.Gaps)
	}
	found, empty := false, false
	for _, row := range adapted.Evidence {
		if row.Kind == "result_reported" {
			found = row.CompleteDecision == "UNKNOWN" && bytes.Equal(row.Result, rows[2].Result) && row.Sequence == 8
		}
		if row.Kind == "cgroup_empty" {
			empty = row.Empty && row.Sequence == 7 && row.OOMKill != nil && *row.OOMKill == 0
		}
	}
	if !found || !empty {
		t.Fatalf("lost raw uncertain submission: %+v", adapted)
	}
	truncated, err := AdaptCapture(records[:len(records)-1])
	if err != nil {
		t.Fatal(err)
	}
	if len(truncated.Gaps) == 0 {
		t.Fatal("truncated capture accepted as a complete transport failure")
	}
}

func TestUnknownCompleteSupportsCommittedWinnerButNotUnprovedContent(t *testing.T) {
	l, rows := goodFixture()
	rows[2].CompleteDecision = "UNKNOWN"
	if r := Check(l, rows, nil); !r.OK {
		t.Fatalf("lost ACK prevents verification of independently proven winner: %+v", r)
	}
	rows[2].Result = []byte{0xff}
	if r := Check(l, rows, nil); r.OK {
		t.Fatal("unknown response hid malformed committed result")
	}
	l, rows = goodFixture()
	rows[2].CompleteDecision = "UNKNOWN"
	rows[2].Result = nil
	if r := Check(l, rows, nil); r.OK {
		t.Fatal("unknown response replaced missing committed result")
	}
	l, rows = goodFixture()
	rows[2].CompleteDecision = "UNKNOWN"
	rows[1].Empty = false
	if r := Check(l, rows, nil); r.OK {
		t.Fatal("unknown response bypassed cgroup evidence")
	}
}

func TestUnknownUncommittedSubmissionDoesNotRequireStoredResult(t *testing.T) {
	l, rows := goodFixture()
	l.Events = l.Events[:2]
	l.Executions[0].WinnerAttemptID = ""
	l.Executions[0].State = "Running"
	l.Attempts[0].Published = false
	l.Attempts[0].Result = nil
	l.Attempts[0].ResultDigests = nil
	l.Attempts[0].AttemptDir = ""
	l.Attempts[0].Status = "UNKNOWN"
	rows = rows[:3]
	rows[2].CompleteDecision = "UNKNOWN"
	if r := Check(l, rows, nil); !r.OK {
		t.Fatalf("unconfirmed request required accepted result fields: %+v", r)
	}
	rows[1].Empty = false
	if r := Check(l, rows, nil); r.OK {
		t.Fatal("unconfirmed report bypassed independent cgroup emptiness")
	}
}
