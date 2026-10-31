package scheduler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

func (s *Scheduler) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	export := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Export())
	}
	mux.HandleFunc("GET /export", export)
	mux.HandleFunc("GET /ledger", export)
	// What the durable store holds, read on its own connection: an audit read
	// never blocks commits, and its failure never stops the scheduler.
	mux.HandleFunc("GET /audit/durable-export", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
		defer cancel()
		snapshotter, ok := s.store.(interface {
			Snapshot(context.Context) (*Ledger, error)
		})
		if !ok {
			http.Error(w, "store has no independent snapshot", http.StatusNotImplemented)
			return
		}
		ledger, err := snapshotter.Snapshot(ctx)
		if err != nil {
			http.Error(w, "durable ledger unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ledger)
	})
	explain := func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			id = strings.TrimPrefix(r.URL.Path, "/v1/executions/")
			if !strings.HasPrefix(id, "executions/") {
				id = "executions/" + id
			}
		}
		l := s.Export()
		e := l.execution(id)
		if e == nil {
			http.Error(w, "unknown execution", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(e)
	}
	mux.HandleFunc("GET /explain", explain)
	mux.HandleFunc("GET /v1/executions/", explain)
	mux.HandleFunc("POST /cancel", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Cancel(r.Context(), r.URL.Query().Get("id")); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}
