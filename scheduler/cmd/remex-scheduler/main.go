package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"github.com/simon-jd/remex/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func run() error {
	listen := flag.String("listen", ":8980", "REAPI and worker gRPC listen address")
	admin := flag.String("admin", "127.0.0.1:8081", "trusted admin HTTP address")
	cas := flag.String("cas", "127.0.0.1:9092", "bazel-remote gRPC endpoint")
	postgres := flag.String("postgres", os.Getenv("REMEX_POSTGRES"), "PostgreSQL DSN")
	memory := flag.Bool("memory", false, "explicitly use a non-durable development ledger")
	lease := flag.Duration("lease-duration", 30*time.Second, "worker lease duration")
	grace := flag.Duration("recovery-grace", 30*time.Second, "monotonic grace after scheduler restart")
	maxAttempts := flag.Int("max-attempts", 3, "infrastructure retry attempt budget")
	flag.Parse()
	if *lease < 300*time.Millisecond || *grace <= 0 || *maxAttempts <= 0 {
		return fmt.Errorf("invalid lease/grace/attempt settings")
	}
	if *postgres == "" && !*memory {
		return fmt.Errorf("--postgres or --memory is required")
	}
	if *postgres != "" && *memory {
		return fmt.Errorf("--postgres and --memory are mutually exclusive")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	var store scheduler.Store
	var err error
	if *memory {
		store = scheduler.NewMemoryStore()
		log.Print("development memory ledger: state is not durable")
	} else {
		store, err = scheduler.OpenPostgres(ctx, *postgres)
		if err != nil {
			return err
		}
	}
	defer store.Close()
	conn, err := grpc.NewClient(
		*cas,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)),
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	s, err := scheduler.New(
		ctx,
		store,
		scheduler.NewGRPCCache(conn),
		scheduler.Config{CASEndpoint: *cas, LeaseDuration: *lease, RecoveryGrace: *grace, MaxAttempts: *maxAttempts},
	)
	if err != nil {
		return err
	}
	defer s.Close() // before store.Close: writes what the log still holds
	server := grpc.NewServer(grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20))
	re.RegisterExecutionServer(server, s)
	re.RegisterCapabilitiesServer(server, s)
	w.RegisterWorkerServiceServer(server, s)
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	adminLn, err := net.Listen("tcp", *admin)
	if err != nil {
		return err
	}
	defer adminLn.Close()
	httpServer := &http.Server{Handler: s.AdminHandler(), ReadHeaderTimeout: 5 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- server.Serve(ln) }()
	go func() { errs <- httpServer.Serve(adminLn) }()
	go s.Run(ctx, func(err error) { log.Printf("maintenance: %v", err) })
	log.Printf("scheduler gRPC=%s admin=%s cas=%s", *listen, *admin, *cas)
	select {
	case <-ctx.Done():
	case err = <-s.Failure():
		cancel()
	case err = <-errs:
		cancel()
	}
	server.Stop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
