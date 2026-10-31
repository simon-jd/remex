package checker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

// CaptureRecord stores original protobuf frames, not scheduler-derived facts.
// Records from both listeners share one append-only sequence and capture ID.
type CaptureRecord struct {
	CaptureID    string              `json:"capture_id"`
	Sequence     int64               `json:"sequence"`
	Time         string              `json:"time"`
	CallID       int64               `json:"call_id"`
	Endpoint     string              `json:"endpoint"`
	Method       string              `json:"method"`
	Kind         string              `json:"kind"`
	Metadata     map[string][]string `json:"metadata,omitempty"`
	Message      []byte              `json:"message"`
	StatusCode   int32               `json:"status_code"`
	Error        string              `json:"error,omitempty"`
	CgroupPath   string              `json:"cgroup_path,omitempty"`
	CgroupEvents string              `json:"cgroup_events,omitempty"`
	MemoryEvents string              `json:"memory_events,omitempty"`
}

type Recorder struct {
	mu         sync.Mutex
	writer     io.Writer
	sequence   int64
	calls      atomic.Int64
	id         string
	CgroupRoot string
	LedgerURL  string
	ended      map[int64]bool
}

func NewRecorder(w io.Writer, cgroupRoot string) *Recorder {
	return &Recorder{writer: w, id: fmt.Sprintf("capture-%d", time.Now().UnixNano()), CgroupRoot: cgroupRoot, ended: map[int64]bool{}}
}
func (r *Recorder) write(row CaptureRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// The forwarding goroutines may observe cancellation concurrently. Once the
	// terminal status has been recorded, no later frame can be part of that RPC.
	// Its request is already retained, so uncertain mutations remain auditable.
	if r.ended[row.CallID] {
		return context.Canceled
	}
	if row.Kind == "end" {
		r.ended[row.CallID] = true
	}
	r.sequence++
	row.Sequence = r.sequence
	row.CaptureID = r.id
	row.Time = time.Now().UTC().Format(time.RFC3339Nano)
	return json.NewEncoder(r.writer).Encode(row)
}

type rawFrame struct{ data []byte }
type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }
func (rawCodec) Marshal(value any) ([]byte, error) {
	frame, ok := value.(*rawFrame)
	if !ok {
		return nil, fmt.Errorf("recorder codec expected raw frame")
	}
	return frame.data, nil
}
func (rawCodec) Unmarshal(data []byte, value any) error {
	frame, ok := value.(*rawFrame)
	if !ok {
		return fmt.Errorf("recorder codec expected raw frame")
	}
	frame.data = append(frame.data[:0], data...)
	return nil
}

// Server transparently relays unary and streaming RPCs through one upstream
// connection. Capture failure fails the RPC rather than silently losing evidence.
func (r *Recorder) Server(endpoint string, upstream *grpc.ClientConn) *grpc.Server {
	return grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.MaxRecvMsgSize(256<<20), grpc.MaxSendMsgSize(256<<20), grpc.UnknownServiceHandler(func(_ any, in grpc.ServerStream) error {
		method, ok := grpc.MethodFromServerStream(in)
		if !ok {
			return status.Error(codes.Internal, "RPC method missing")
		}
		call := r.calls.Add(1)
		md, _ := metadata.FromIncomingContext(in.Context())
		md = md.Copy()
		base := CaptureRecord{CallID: call, Endpoint: endpoint, Method: method}
		row := base
		row.Kind = "start"
		row.Metadata = map[string][]string(md)
		if err := r.write(row); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(in.Context(), md))
		defer cancel()
		out, err := upstream.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method, grpc.ForceCodec(rawCodec{}), grpc.MaxCallRecvMsgSize(256<<20), grpc.MaxCallSendMsgSize(256<<20))
		if err != nil {
			row = base
			row.Kind = "end"
			row.StatusCode = int32(status.Code(err))
			row.Error = err.Error()
			if logErr := r.write(row); logErr != nil {
				return status.Error(codes.Internal, logErr.Error())
			}
			return err
		}
		requests := make(chan error, 1)
		go func() {
			for {
				frame := &rawFrame{}
				err := in.RecvMsg(frame)
				if err == io.EOF {
					requests <- out.CloseSend()
					return
				}
				if err != nil {
					requests <- err
					return
				}
				record := base
				record.Kind = "request"
				record.Message = frame.data
				if err = r.write(record); err != nil {
					requests <- err
					cancel()
					return
				}
				if method == "/remex.worker.v1.WorkerService/Complete" {
					if err = r.observeCgroup(base, frame.data); err != nil {
						requests <- err
						cancel()
						return
					}
				}
				if strings.HasSuffix(method, "/UpdateActionResult") {
					if err = r.observeLedger(ctx, base, "ledger_before_ac"); err != nil {
						requests <- err
						cancel()
						return
					}
				}
				if err = out.SendMsg(frame); err != nil {
					requests <- err
					return
				}
				record = base
				record.Kind = "request_forwarded"
				if err = r.write(record); err != nil {
					requests <- err
					cancel()
					return
				}
			}
		}()
		responses := make(chan error, 1)
		go func() {
			headers, err := out.Header()
			if err == nil {
				err = in.SendHeader(headers)
			}
			if err != nil {
				responses <- err
				return
			}
			for {
				frame := &rawFrame{}
				err := out.RecvMsg(frame)
				if err != nil {
					in.SetTrailer(out.Trailer())
					if err == io.EOF {
						err = nil
					}
					responses <- err
					return
				}
				record := base
				record.Kind = "response"
				record.Message = frame.data
				if err = r.write(record); err != nil {
					responses <- err
					return
				}
				if strings.HasSuffix(method, "/FindMissingBlobs") && len(md.Get("x-remex-attempt-id")) > 0 {
					if err = r.observeLedger(ctx, base, "ledger_after_cas"); err != nil {
						responses <- err
						return
					}
				}
				if method == "/remex.worker.v1.WorkerService/Complete" {
					if err = r.observeLedger(ctx, base, "ledger_after_complete"); err != nil {
						responses <- err
						return
					}
				}
				if err = in.SendMsg(frame); err != nil {
					responses <- err
					return
				}
			}
		}()
		select {
		case err = <-requests:
			if err == nil {
				err = <-responses
			}
		case err = <-responses:
		}
		row = base
		row.Kind = "end"
		row.StatusCode = int32(status.Code(err))
		if err != nil {
			row.Error = err.Error()
		}
		if logErr := r.write(row); logErr != nil {
			return status.Error(codes.Internal, logErr.Error())
		}
		return err
	}))
}

func (r *Recorder) observeLedger(ctx context.Context, base CaptureRecord, kind string) error {
	row := base
	row.Kind = kind
	if r.LedgerURL == "" {
		row.Error = "durable ledger observation URL missing"
		return r.write(row)
	}
	client := http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequestWithContext(ctx, "GET", r.LedgerURL, nil)
	if err != nil {
		row.Error = err.Error()
		return r.write(row)
	}
	response, err := client.Do(request)
	if err != nil {
		row.Error = err.Error()
		return r.write(row)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		row.Error = response.Status
		return r.write(row)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 64<<20))
	if err != nil {
		row.Error = err.Error()
	} else {
		var l Ledger
		if err = json.Unmarshal(data, &l); err != nil {
			row.Error = err.Error()
		} else {
			row.Message = data
		}
	}
	return r.write(row)
}

func (r *Recorder) observeCgroup(base CaptureRecord, request []byte) error {
	row := base
	row.Kind = "cgroup_observation"
	fields, err := wireFields(request)
	if err != nil {
		row.Error = err.Error()
		return r.write(row)
	}
	var usage struct {
		CgroupPath string `json:"cgroup_path"`
	}
	_ = json.Unmarshal(wireData(fields, 10), &usage)
	row.CgroupPath = usage.CgroupPath
	if usage.CgroupPath == "" || r.CgroupRoot == "" {
		row.Error = "independent cgroup path or allowed root missing"
		return r.write(row)
	}
	root, err := filepath.EvalSymlinks(r.CgroupRoot)
	if err != nil {
		row.Error = err.Error()
		return r.write(row)
	}
	path, err := filepath.EvalSymlinks(usage.CgroupPath)
	if err != nil {
		row.Error = err.Error()
		return r.write(row)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == "." {
		row.Error = "cgroup path is outside allowed attempt subtree"
		return r.write(row)
	}
	events, err := os.ReadFile(filepath.Join(path, "cgroup.events"))
	if err != nil {
		row.Error = err.Error()
	} else {
		row.CgroupEvents = string(events)
	}
	memory, err := os.ReadFile(filepath.Join(path, "memory.events"))
	if err != nil {
		row.Error = err.Error()
	} else {
		row.MemoryEvents = string(memory)
	}
	return r.write(row)
}

type wireValue struct {
	bytes  []byte
	number uint64
	kind   protowire.Type
}

func wireFields(raw []byte) (map[protowire.Number][]wireValue, error) {
	fields := map[protowire.Number][]wireValue{}
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		raw = raw[n:]
		v := wireValue{kind: typ}
		var count int
		switch typ {
		case protowire.BytesType:
			v.bytes, count = protowire.ConsumeBytes(raw)
		case protowire.VarintType:
			v.number, count = protowire.ConsumeVarint(raw)
		default:
			count = protowire.ConsumeFieldValue(num, typ, raw)
		}
		if count < 0 {
			return nil, protowire.ParseError(count)
		}
		fields[num] = append(fields[num], v)
		raw = raw[count:]
	}
	return fields, nil
}
func wireData(fields map[protowire.Number][]wireValue, num protowire.Number) []byte {
	values := fields[num]
	if len(values) == 0 {
		return []byte{}
	}
	return values[len(values)-1].bytes
}
func wireNumber(fields map[protowire.Number][]wireValue, num protowire.Number) int64 {
	values := fields[num]
	if len(values) == 0 {
		return 0
	}
	return int64(values[len(values)-1].number)
}
