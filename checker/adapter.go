package checker

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"
)

type CaptureAdaptation struct {
	Evidence []Evidence `json:"evidence"`
	Gaps     []string   `json:"gaps"`
}
type capturedCall struct {
	start               CaptureRecord
	requests, responses []CaptureRecord
	end                 *CaptureRecord
	observations        []CaptureRecord
	forwarded           []CaptureRecord
}

func ReadCapture(reader io.Reader) ([]CaptureRecord, error) {
	var records []CaptureRecord
	decoder := json.NewDecoder(reader)
	for {
		var row CaptureRecord
		err := decoder.Decode(&row)
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, row)
	}
}

// AdaptCapture uses only intercepted original protocol traffic and independent
// local observations. No final ledger is an input, so missing traffic cannot be
// replaced with a scheduler-derived success claim.
func AdaptCapture(records []CaptureRecord) (CaptureAdaptation, error) {
	out := CaptureAdaptation{Evidence: []Evidence{}, Gaps: []string{}}
	calls := map[int64]*capturedCall{}
	captureID := ""
	previous := int64(0)
	for _, record := range records {
		if record.Sequence != previous+1 {
			return out, fmt.Errorf(
				"capture sequences must start at 1 and be contiguous: got %d after %d",
				record.Sequence,
				previous,
			)
		}
		previous = record.Sequence
		if captureID == "" {
			captureID = record.CaptureID
		}
		if record.CaptureID != captureID || captureID == "" {
			return out, fmt.Errorf("capture ID mismatch")
		}
		call := calls[record.CallID]
		if call == nil {
			if record.CallID <= 0 || record.Kind != "start" {
				return out, fmt.Errorf("capture call %d is missing its start record", record.CallID)
			}
			call = &capturedCall{}
			calls[record.CallID] = call
		} else {
			if record.Kind == "start" {
				return out, fmt.Errorf("duplicate start for call %d", record.CallID)
			}
			if record.Method != call.start.Method || record.Endpoint != call.start.Endpoint {
				return out, fmt.Errorf("method or endpoint changed within call %d", record.CallID)
			}
			if call.end != nil {
				return out, fmt.Errorf("duplicate end or record after end for call %d", record.CallID)
			}
		}
		switch record.Kind {
		case "start":
			call.start = record
		case "request":
			call.requests = append(call.requests, record)
		case "response":
			call.responses = append(call.responses, record)
		case "request_forwarded":
			call.forwarded = append(call.forwarded, record)
		case "end":
			copy := record
			call.end = &copy
		default:
			call.observations = append(call.observations, record)
		}
	}
	var ordered []*capturedCall
	for _, call := range calls {
		ordered = append(ordered, call)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].start.Sequence < ordered[j].start.Sequence })
	good := func(c *capturedCall) bool {
		return c.start.Kind == "start" && c.end != nil && c.end.StatusCode == 0 && len(c.requests) > 0
	}
	gap := func(c *capturedCall, text string) {
		out.Gaps = append(out.Gaps, fmt.Sprintf("call %d %s: %s", c.start.CallID, c.start.Method, text))
	}
	meta := func(c *capturedCall, key string) string {
		values := c.start.Metadata[key]
		if len(values) == 0 {
			return ""
		}
		return values[0]
	}
	identity := func(c *capturedCall) Evidence {
		epoch, _ := strconv.ParseInt(meta(c, "x-remex-epoch"), 10, 64)
		anchor, _ := strconv.ParseInt(meta(c, "x-remex-ledger-sequence"), 10, 64)
		return Evidence{
			ExecutionID:  meta(c, "x-remex-execution-id"),
			AttemptID:    meta(c, "x-remex-attempt-id"),
			Epoch:        epoch,
			OrderDomain:  captureID,
			LedgerAnchor: anchor,
		}
	}
	// Gather complete, digest-verified ByteStream read/write contents. Partial or
	// compressed resources remain gaps instead of silently becoming Tree proofs.
	blobs := map[Digest][]byte{}
	for _, call := range ordered {
		if !good(call) {
			continue
		}
		method := call.start.Method
		if method == "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs" {
			for _, response := range call.responses {
				var batch re.BatchReadBlobsResponse
				if err := proto.Unmarshal(response.Message, &batch); err != nil {
					return out, err
				}
				for _, blob := range batch.Responses {
					if blob.GetStatus().GetCode() == 0 && blob.Compressor == re.Compressor_IDENTITY {
						digest := modelDigest(blob.Digest)
						if hashBytes(blob.Data) == digest {
							blobs[digest] = append([]byte{}, blob.Data...)
						}
					}
				}
			}
			continue
		}
		if method != "/google.bytestream.ByteStream/Read" && method != "/google.bytestream.ByteStream/Write" {
			continue
		}
		first, err := wireFields(call.requests[0].Message)
		if err != nil {
			return out, err
		}
		resource := string(wireData(first, 1))
		parts := strings.Split(resource, "/")
		if len(parts) < 3 || parts[len(parts)-3] != "blobs" {
			continue
		}
		size, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
		if err != nil {
			continue
		}
		digest := Digest{Hash: parts[len(parts)-2], SizeBytes: size}
		var data []byte
		valid := true
		if strings.HasSuffix(method, "/Read") {
			if wireNumber(first, 2) != 0 {
				continue
			}
			for _, response := range call.responses {
				fields, err := wireFields(response.Message)
				if err != nil {
					return out, err
				}
				data = append(data, wireData(fields, 10)...)
			}
		} else {
			for _, request := range call.requests {
				fields, err := wireFields(request.Message)
				if err != nil {
					return out, err
				}
				if wireNumber(fields, 2) != int64(len(data)) {
					valid = false
					break
				}
				data = append(data, wireData(fields, 10)...)
			}
			if len(call.responses) != 1 {
				valid = false
			} else {
				fields, err := wireFields(call.responses[0].Message)
				if err != nil {
					return out, err
				}
				valid = valid && wireNumber(fields, 1) == int64(len(data))
			}
		}
		if valid && hashBytes(data) == digest {
			blobs[digest] = data
		}
	}
	leases := map[string]Evidence{}
	// Unscoped verified content also supplies Action/Command evidence for cache
	// hits, where no Lease exists. Identity is the actual content digest, not a
	// fabricated execution ID from the final ledger.
	var capturedBlobs []Blob
	for digest, data := range blobs {
		capturedBlobs = append(capturedBlobs, Blob{Digest: digest, Data: data})
	}
	sort.Slice(
		capturedBlobs,
		func(i, j int) bool { return capturedBlobs[i].Digest.Hash < capturedBlobs[j].Digest.Hash },
	)
	if len(capturedBlobs) > 0 {
		out.Evidence = append(
			out.Evidence,
			Evidence{Source: "cas", Kind: "blob_content", OrderDomain: captureID, Blobs: capturedBlobs},
		)
	}
	for _, call := range ordered {
		if !good(call) || call.start.Method != "/remex.worker.v1.WorkerService/Lease" || len(call.responses) != 1 {
			continue
		}
		request, err := wireFields(call.requests[0].Message)
		if err != nil {
			return out, err
		}
		response, err := wireFields(call.responses[0].Message)
		if err != nil {
			return out, err
		}
		if wireNumber(response, 1) == 0 {
			continue
		}
		properties := map[string]string{}
		for _, entry := range request[2] {
			fields, err := wireFields(entry.bytes)
			if err != nil {
				return out, err
			}
			properties[string(wireData(fields, 1))] = string(wireData(fields, 2))
		}
		df, err := wireFields(wireData(response, 7))
		if err != nil {
			return out, err
		}
		digest := Digest{Hash: string(wireData(df, 1)), SizeBytes: wireNumber(df, 2)}
		var snapshot struct {
			UUID   string `json:"uuid"`
			CC     string `json:"compute_capability"`
			Driver string `json:"driver_version"`
		}
		_ = json.Unmarshal(wireData(request, 3), &snapshot)
		verified := snapshot.UUID != "" && snapshot.CC == properties["gpu.cc"] && strings.Split(
			snapshot.Driver,
			".",
		)[0] == properties["gpu.driver_major"]
		row := Evidence{
			Source:         "worker",
			Kind:           "lease_verified",
			Sequence:       call.requests[0].Sequence,
			ExecutionID:    string(wireData(response, 2)),
			AttemptID:      string(wireData(response, 3)),
			Epoch:          wireNumber(response, 4),
			ActionDigest:   digest,
			Action:         wireData(response, 8),
			Command:        wireData(response, 9),
			WorkerPlatform: properties,
			DeviceVerified: verified,
			OrderDomain:    captureID,
			RawRecordIDs:   []int64{call.requests[0].Sequence, call.responses[0].Sequence},
		}
		row.InstanceName = string(wireData(response, 6))
		leases[row.AttemptID] = row
		out.Evidence = append(out.Evidence, row)
	}
	for _, call := range ordered {
		// A transport failure does not erase an observed Complete submission.
		// The scheduler may have committed it before its response was lost.
		if call.start.Method == "/remex.worker.v1.WorkerService/Complete" && len(call.requests) > 1 {
			gap(call, "unary Complete has multiple request frames")
			continue
		}
		unknownComplete := call.start.Method == "/remex.worker.v1.WorkerService/Complete" &&
			call.end != nil &&
			call.end.StatusCode != 0 &&
			len(call.requests) == 1
		if !good(call) && !unknownComplete {
			if call.start.Kind == "start" && call.end == nil {
				gap(call, "unfinished RPC")
			}
			if strings.HasSuffix(call.start.Method, "/UpdateActionResult") && len(call.requests) > 0 {
				row := identity(call)
				row.Source = "ac"
				row.Kind = "ac_write_attempt"
				row.Detail = "captured mutating AC request has unknown outcome"
				row.Sequence = call.requests[0].Sequence
				var request re.UpdateActionResultRequest
				if proto.Unmarshal(call.requests[0].Message, &request) == nil {
					row.InstanceName = request.InstanceName
					row.ActionDigest = modelDigest(request.ActionDigest)
					row.Result, _ = proto.Marshal(request.ActionResult)
				}
				out.Evidence = append(out.Evidence, row)
			}
			continue
		}
		method := call.start.Method
		switch method {
		case "/remex.worker.v1.WorkerService/Complete":
			request, err := wireFields(call.requests[0].Message)
			if err != nil {
				return out, err
			}
			attempt := string(wireData(request, 2))
			lease, ok := leases[attempt]
			if !ok {
				gap(call, "Complete has no captured Lease identity")
				continue
			}
			status := int32(wireNumber(request, 5))
			row := Evidence{
				Source:       "worker",
				Kind:         "result_reported",
				Sequence:     call.requests[0].Sequence,
				ExecutionID:  lease.ExecutionID,
				AttemptID:    attempt,
				Epoch:        wireNumber(request, 3),
				Result:       append([]byte{}, wireData(request, 4)...),
				StatusCode:   &status,
				AttemptDir:   string(wireData(request, 12)),
				OrderDomain:  captureID,
				RawRecordIDs: []int64{call.requests[0].Sequence},
			}
			if unknownComplete {
				row.CompleteDecision = "UNKNOWN"
				row.Detail = "Complete was submitted; transport ended without a confirmed decision"
			} else if len(call.responses) != 1 {
				gap(call, "Complete response missing")
				continue
			} else {
				response, err := wireFields(call.responses[0].Message)
				if err != nil {
					return out, err
				}
				switch wireNumber(response, 1) {
				case 0:
					row.CompleteDecision = "ACCEPTED"
				case 1:
					row.CompleteDecision = "FENCED"
				case 2:
					row.CompleteDecision = "REJECTED"
				default:
					gap(call, "unknown Complete decision")
					continue
				}
				row.RawRecordIDs = append(row.RawRecordIDs, call.responses[0].Sequence)
			}
			attachSnapshot(&row, call, "ledger_after_complete")
			if len(call.forwarded) > 0 {
				row.Sequence = call.forwarded[0].Sequence
				row.RawRecordIDs = append(row.RawRecordIDs, row.Sequence)
			} else {
				gap(call, "request forwarding boundary missing")
			}
			var result re.ActionResult
			resultErr := proto.Unmarshal(row.Result, &result)
			if resultErr != nil && row.CompleteDecision == "ACCEPTED" {
				return out, resultErr
			}
			if resultErr == nil && (row.CompleteDecision == "ACCEPTED" || unknownComplete) {
				for _, dir := range result.OutputDirectories {
					d := modelDigest(dir.TreeDigest)
					if data, ok := blobs[d]; ok {
						row.Trees = append(row.Trees, Blob{Digest: d, Data: data})
					} else if !unknownComplete {
						gap(call, "output Tree content was not fully captured: "+d.Hash)
					}
				}
			}
			out.Evidence = append(out.Evidence, row)
			for _, observation := range call.observations {
				if observation.Kind != "cgroup_observation" {
					continue
				}
				if observation.Error != "" {
					gap(call, observation.Error)
					continue
				}
				empty := counter(observation.CgroupEvents, "populated") == "0"
				oom, err := strconv.ParseInt(counter(observation.MemoryEvents, "oom_kill"), 10, 64)
				if err != nil || oom < 0 {
					gap(call, "independent memory.events oom_kill counter missing or invalid")
					continue
				}
				out.Evidence = append(
					out.Evidence,
					Evidence{
						Source:       "worker",
						Kind:         "cgroup_empty",
						Sequence:     observation.Sequence,
						ExecutionID:  lease.ExecutionID,
						AttemptID:    attempt,
						Epoch:        row.Epoch,
						AttemptDir:   row.AttemptDir,
						Empty:        empty,
						OOMKill:      &oom,
						OrderDomain:  captureID,
						RawRecordIDs: []int64{observation.Sequence},
					},
				)
			}
		case "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs":
			row := identity(call)
			if row.AttemptID == "" {
				continue
			}
			if len(call.responses) != 1 {
				gap(call, "FindMissingBlobs response missing")
				continue
			}
			var request re.FindMissingBlobsRequest
			var response re.FindMissingBlobsResponse
			if err := proto.Unmarshal(call.requests[0].Message, &request); err != nil {
				return out, err
			}
			if err := proto.Unmarshal(call.responses[0].Message, &response); err != nil {
				return out, err
			}
			missing := map[Digest]bool{}
			for _, d := range response.MissingBlobDigests {
				missing[modelDigest(d)] = true
			}
			for _, d := range request.BlobDigests {
				if !missing[modelDigest(d)] {
					row.Digests = append(row.Digests, modelDigest(d))
				}
			}
			row.Source = "cas"
			row.InstanceName = request.InstanceName
			row.Kind = "cas_verified"
			row.Sequence = call.responses[0].Sequence
			row.RawRecordIDs = []int64{call.requests[0].Sequence, call.responses[0].Sequence}
			attachSnapshot(&row, call, "ledger_after_cas")
			out.Evidence = append(out.Evidence, row)
		case "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult":
			row := identity(call)
			var request re.UpdateActionResultRequest
			if err := proto.Unmarshal(call.requests[0].Message, &request); err != nil {
				return out, err
			}
			row.Source = "ac"
			row.InstanceName = request.InstanceName
			row.Kind = "ac_write"
			row.Sequence = call.requests[0].Sequence
			row.ActionDigest = modelDigest(request.ActionDigest)
			row.Result, _ = proto.Marshal(request.ActionResult)
			if row.Result == nil {
				row.Result = []byte{}
			}
			row.RawRecordIDs = []int64{call.requests[0].Sequence}
			if len(call.forwarded) > 0 {
				row.Sequence = call.forwarded[0].Sequence
				row.RawRecordIDs = append(row.RawRecordIDs, row.Sequence)
			}
			attachSnapshot(&row, call, "ledger_before_ac")
			if row.AttemptID == "" {
				gap(call, "AC publication lacks execution/attempt/epoch correlation metadata")
			}
			out.Evidence = append(out.Evidence, row)
		}
	}
	for _, detail := range out.Gaps {
		out.Evidence = append(
			out.Evidence,
			Evidence{Source: "capture", Kind: "evidence_gap", Detail: detail, OrderDomain: captureID},
		)
	}
	sort.SliceStable(out.Evidence, func(i, j int) bool { return out.Evidence[i].Sequence < out.Evidence[j].Sequence })
	return out, nil
}

func attachSnapshot(row *Evidence, call *capturedCall, kind string) {
	for _, observation := range call.observations {
		if observation.Kind == kind && observation.Error == "" && observation.Message != nil {
			var ledger Ledger
			if json.Unmarshal(observation.Message, &ledger) == nil && ledger.SchemaVersion == 1 {
				row.LedgerSnapshot = &ledger
				row.RawRecordIDs = append(row.RawRecordIDs, observation.Sequence)
			}
		}
	}
}
func counter(text, key string) string {
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == key {
			return fields[1]
		}
	}
	return ""
}
