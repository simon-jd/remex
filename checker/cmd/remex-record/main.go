package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/simon-jd/remex/checker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "remex-record:", err)
		os.Exit(1)
	}
}
func run() error {
	cacheListen := flag.String("cache-listen", "127.0.0.1:19093", "local cache proxy address (empty disables)")
	cacheTarget := flag.String("cache-target", "127.0.0.1:19092", "original CAS/AC address")
	workerListen := flag.String("worker-listen", "127.0.0.1:18981", "local worker protocol proxy address (empty disables)")
	workerTarget := flag.String("worker-target", "127.0.0.1:18980", "original worker service address")
	output := flag.String("out", "", "new raw JSONL capture file")
	cgroupRoot := flag.String("cgroup-root", "", "allowed delegated cgroup root for local observer")
	ledgerURL := flag.String("ledger-url", "", "durable read-only ledger URL; must not take scheduler execution mutex")
	flag.Parse()
	f, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	recorder := checker.NewRecorder(f, *cgroupRoot)
	recorder.LedgerURL = *ledgerURL
	errors := make(chan error, 2)
	var servers []*grpc.Server
	for _, endpoint := range []struct{ name, listen, target string }{{"cache", *cacheListen, *cacheTarget}, {"worker", *workerListen, *workerTarget}} {
		if endpoint.listen == "" {
			continue
		}
		listener, err := net.Listen("tcp", endpoint.listen)
		if err != nil {
			return err
		}
		defer listener.Close()
		conn, err := grpc.NewClient(endpoint.target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		server := recorder.Server(endpoint.name, conn)
		servers = append(servers, server)
		go func() { errors <- server.Serve(listener) }()
		fmt.Fprintln(os.Stderr, endpoint.name, endpoint.listen, "->", endpoint.target)
	}
	if len(servers) == 0 {
		return fmt.Errorf("at least one listener is required")
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	select {
	case err := <-errors:
		return err
	case <-stop:
		for _, s := range servers {
			s.Stop()
		}
		return f.Sync()
	}
}
