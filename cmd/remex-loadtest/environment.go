package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/jackc/pgx/v5"
)

// settings is a repeatable -env key=value flag.
type settings map[string]string

func (s settings) String() string { return fmt.Sprint(map[string]string(s)) }
func (s settings) Set(value string) error {
	key, v, ok := strings.Cut(value, "=")
	if !ok || key == "" {
		return fmt.Errorf("want key=value, got %q", value)
	}
	s[key] = v
	return nil
}

// environment describes the host and store a run measured, so that records
// from different machines or settings are not compared by accident.
func environment(ctx context.Context, dsn string, extra settings) map[string]any {
	env := map[string]any{"go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "cpus": runtime.NumCPU()}
	if model := procField("/proc/cpuinfo", "model name"); model != "" {
		env["cpu_model"] = model
	}
	if memory := procField("/proc/meminfo", "MemTotal"); memory != "" {
		env["memory"] = memory
	}
	if release, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		env["kernel"] = strings.TrimSpace(string(release))
	}
	if dsn != "" {
		env["postgres"] = postgresSettings(ctx, dsn)
	}
	for key, value := range extra {
		env[key] = value
	}
	return env
}

// procField returns the value of the first "key: value" line in a /proc file.
func procField(path, key string) string {
	text, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(text), "\n") {
		name, value, ok := strings.Cut(line, ":")
		if ok && strings.TrimSpace(name) == key {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// postgresSettings reads the server version and the settings that decide
// what a commit costs.
func postgresSettings(ctx context.Context, dsn string) map[string]string {
	out := map[string]string{}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	defer conn.Close(ctx)
	for _, name := range []string{"server_version", "fsync", "synchronous_commit", "wal_sync_method", "full_page_writes", "commit_delay", "shared_buffers", "max_wal_size"} {
		var value string
		if err := conn.QueryRow(ctx, "SHOW "+name).Scan(&value); err == nil {
			out[name] = value
		}
	}
	return out
}
