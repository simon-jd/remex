// actionprobe is a standalone official-REAPI client used for precise command
// timeout/cancellation tests. It does not import scheduler implementation code.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func main() {
	scheduler := flag.String("scheduler", "", "actual scheduler endpoint")
	cache := flag.String("cas", "", "independent recorder cache endpoint")
	instance := flag.String("instance", "", "unique test namespace")
	argv := flag.String("argv", "", "JSON array of actual command arguments")
	timeout := flag.Duration("timeout", 5*time.Second, "original REAPI Action timeout")
	out := flag.String("out", "", "original client artifacts directory")
	flag.Parse()
	var args []string
	if err := json.Unmarshal([]byte(*argv), &args); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(*cache, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	cas := re.NewContentAddressableStorageClient(conn)
	put := func(name string, message proto.Message) *re.Digest {
		data, err := proto.Marshal(message)
		if err != nil {
			panic(err)
		}
		if err = os.WriteFile(filepath.Join(*out, name+".pb"), data, 0600); err != nil {
			panic(err)
		}
		sum := sha256.Sum256(data)
		digest := &re.Digest{Hash: fmt.Sprintf("%x", sum), SizeBytes: int64(len(data))}
		response, err := cas.BatchUpdateBlobs(ctx, &re.BatchUpdateBlobsRequest{InstanceName: *instance, DigestFunction: re.DigestFunction_SHA256, Requests: []*re.BatchUpdateBlobsRequest_Request{{Digest: digest, Data: data}}})
		if err != nil || len(response.GetResponses()) != 1 || response.Responses[0].GetStatus().GetCode() != 0 {
			panic(fmt.Sprintf("upload: %v %v", response, err))
		}
		return digest
	}
	digest := put("action", &re.Action{CommandDigest: put("command", &re.Command{Arguments: args}), InputRootDigest: put("input-root", &re.Directory{}), Timeout: durationpb.New(*timeout), Platform: &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "cpu"}}}})
	control, err := grpc.NewClient(*scheduler, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer control.Close()
	stream, err := re.NewExecutionClient(control).Execute(ctx, &re.ExecuteRequest{InstanceName: *instance, ActionDigest: digest, SkipCacheLookup: true})
	if err != nil {
		panic(err)
	}
	for {
		op, err := stream.Recv()
		if err != nil {
			panic(err)
		}
		if !op.Done {
			continue
		}
		var result re.ExecuteResponse
		if err = op.GetResponse().UnmarshalTo(&result); err != nil {
			panic(err)
		}
		data, err := proto.Marshal(&result)
		if err != nil {
			panic(err)
		}
		if err = os.WriteFile(filepath.Join(*out, "execute-response.pb"), data, 0600); err != nil {
			panic(err)
		}
		logs := map[string]string{}
		for name, d := range map[string]*re.Digest{"stdout": result.GetResult().GetStdoutDigest(), "stderr": result.GetResult().GetStderrDigest()} {
			if d == nil {
				continue
			}
			r, err := cas.BatchReadBlobs(ctx, &re.BatchReadBlobsRequest{InstanceName: *instance, Digests: []*re.Digest{d}})
			if err != nil || len(r.GetResponses()) != 1 || r.Responses[0].GetStatus().GetCode() != 0 {
				panic(fmt.Sprintf("read original %s: %v %v", name, r, err))
			}
			sum := sha256.Sum256(r.Responses[0].Data)
			if fmt.Sprintf("%x", sum) != d.Hash || int64(len(r.Responses[0].Data)) != d.SizeBytes {
				panic("original log digest mismatch")
			}
			logs[name] = string(r.Responses[0].Data)
		}
		if err = json.NewEncoder(os.Stdout).Encode(map[string]any{"execution_id": op.Name, "status_code": result.GetStatus().GetCode(), "status_message": result.GetStatus().GetMessage(), "exit_code": result.GetResult().GetExitCode(), "action_digest": digest, "logs": logs}); err != nil {
			panic(err)
		}
		return
	}
}
