#!/usr/bin/env python3
"""Independent read-only assertions against original remex-record protobuf frames."""
import argparse
import base64
import hashlib
import json
from pathlib import Path


def wire(data):
    fields = {}
    offset = 0
    def number():
        nonlocal offset
        value, shift = 0, 0
        while True:
            byte = data[offset]
            offset += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
            shift += 7
            if shift > 70:
                raise ValueError("invalid protobuf varint")
    while offset < len(data):
        tag = number()
        field, kind = tag >> 3, tag & 7
        if kind == 0:
            value = number()
        elif kind in (1, 5):
            size = 8 if kind == 1 else 4
            value = data[offset:offset + size]
            offset += size
        elif kind == 2:
            size = number()
            value = data[offset:offset + size]
            offset += size
        else:
            raise ValueError("unsupported protobuf wire type")
        if offset > len(data):
            raise ValueError("truncated protobuf frame")
        fields.setdefault(field, []).append(value)
    return fields


def verify(directory, fault):
    rows = [json.loads(line) for line in (directory / "raw-capture.jsonl").read_text().splitlines()]
    observations = [json.loads(line) for line in (directory / "observations.jsonl").read_text().splitlines()]
    digest = next(row["digest"] for row in observations if row["kind"] == "input_uploaded")
    calls = {}
    for row in rows:
        calls.setdefault(row["call_id"], []).append(row)
    reads, writes, completes = [], [], []
    for frames in calls.values():
        method = frames[0]["method"]
        requests = [wire(base64.b64decode(row["message"] or "")) for row in frames if row["kind"] == "request"]
        responses = [wire(base64.b64decode(row["message"] or "")) for row in frames if row["kind"] == "response"]
        ends = [row for row in frames if row["kind"] == "end"]
        if len(ends) != 1 or not requests:
            continue
        base = {"call_id": frames[0]["call_id"], "start_sequence": frames[0]["sequence"], "end_sequence": ends[0]["sequence"], "rpc_status": ends[0]["status_code"]}
        if method.endswith("ByteStream/Read"):
            data = b"".join(response.get(10, [b""])[0] for response in responses)
            reads.append(dict(base, resource=requests[0][1][0].decode(), returned_size=len(data), returned_sha256=hashlib.sha256(data).hexdigest()))
        if method.endswith("ByteStream/Write"):
            data = b"".join(request.get(10, [b""])[0] for request in requests)
            writes.append(dict(base, resource=requests[0][1][0].decode(), sent_size=len(data), sent_sha256=hashlib.sha256(data).hexdigest(), committed_size=responses[0].get(1, [0])[0] if responses else None))
        if method.endswith("WorkerService/Complete"):
            completes.append(dict(base, attempt_id=requests[0][2][0].decode(), status_code=requests[0].get(5, [0])[0], status_message=requests[0].get(6, [b""])[0].decode()))
    result = {"fault": fault, "input_digest": digest}
    if fault == "cas-corruption":
        corrupted = [read for read in reads if "/blobs/" + digest + "/" in read["resource"] and read["rpc_status"] == 0 and read["returned_sha256"] != digest]
        reports = [call for call in completes if call["status_code"] == 13 and "digest mismatch" in call["status_message"]]
        assert len(corrupted) == 3 and len(reports) == 3, "raw frames do not establish three original corrupt reads and worker digest failures"
        result.update(original_corrupted_reads=corrupted, original_worker_failure_reports=reports)
    elif fault == "cas-eviction":
        missing = [read for read in reads if read["rpc_status"] == 5]
        reports = [call for call in completes if call["status_code"] == 13 and "ByteStream.Read: 5" in call["status_message"]]
        assert len(missing) == 3 and len(reports) == 3, "raw frames do not establish three real missing-input reads and worker failures"
        result.update(original_missing_reads=missing, original_worker_failure_reports=reports)
    else:
        old = json.loads((directory / "ledger-before-kill.json").read_text())["attempts"][0]["id"]
        gate = json.loads((directory / "gate-observed.json").read_text())
        successful = [write for write in writes if write["resource"] == gate["resource"] and write["rpc_status"] == 0 and write["committed_size"] == write["sent_size"]]
        assert len(successful) == 1 and not any(call["attempt_id"] == old for call in completes), "no-Complete after persisted final upload is not established"
        result.update(killed_attempt=old, complete_calls_for_killed_attempt=0, original_final_upload=successful[0])
    result["ok"] = True
    (directory / "fault-rpc-proof.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--fault", required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.artifacts, args.fault), indent=2))
