package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrStoreUnavailable means durable ownership or commit outcome is no longer
// trustworthy. The scheduler must stop rather than serve its old memory state.
var ErrStoreUnavailable = errors.New("durable store unavailable; restart and reload required")

// PostgresStore owns a dedicated connection and its session advisory lock.
// A save writes one change's rows and events in a single transaction.
type PostgresStore struct {
	mu           sync.Mutex
	conn         *pgx.Conn
	dsn          string
	lastSequence int64
	loaded       bool
	failed       error
}

func OpenPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	p := &PostgresStore{conn: c, dsn: dsn}
	var locked bool
	err = c.QueryRow(ctx, "SELECT pg_try_advisory_lock(503050001)").Scan(&locked)
	if err != nil || !locked {
		_ = c.Close(ctx)
		return nil, fmt.Errorf("single scheduler ownership unavailable: %v", err)
	}
	_, err = c.Exec(ctx, `CREATE TABLE IF NOT EXISTS remex_ledger (id integer PRIMARY KEY CHECK (id=1), data jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS executions (id text PRIMARY KEY, data jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS attempts (id text PRIMARY KEY, execution_id text NOT NULL REFERENCES executions(id), epoch bigint NOT NULL, data jsonb NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS attempts_execution_epoch ON attempts(execution_id,epoch);
CREATE TABLE IF NOT EXISTS events (sequence bigint PRIMARY KEY, kind text NOT NULL, data jsonb NOT NULL);`)
	if err != nil {
		_ = c.Close(ctx)
		return nil, err
	}
	return p, nil
}
func (p *PostgresStore) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.conn.Close(ctx)
}
func (p *PostgresStore) fatal(err error) error {
	if p.failed == nil {
		p.failed = fmt.Errorf("%w: %v", ErrStoreUnavailable, err)
	}
	return p.failed
}
func (p *PostgresStore) Load(ctx context.Context) (*Ledger, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.load(ctx)
}
func (p *PostgresStore) load(ctx context.Context) (*Ledger, error) {
	if p.failed != nil {
		return nil, p.failed
	}
	l, err := readLedger(ctx, p.conn)
	if err != nil {
		return nil, p.fatal(err)
	}
	p.lastSequence = int64(len(l.Events))
	p.loaded = true
	return l, nil
}

// Snapshot reads the durable ledger on a separate connection, for audits.
// It never blocks saves, and its errors are returned, not treated as a loss
// of ownership.
func (p *PostgresStore) Snapshot(ctx context.Context) (*Ledger, error) {
	c, err := pgx.Connect(ctx, p.dsn)
	if err != nil {
		return nil, err
	}
	defer c.Close(context.WithoutCancel(ctx))
	return readLedger(ctx, c)
}
func readLedger(ctx context.Context, conn *pgx.Conn) (*Ledger, error) {
	// One snapshot for all three tables. An audit read runs while the leader
	// commits, and separate statements could each see a different commit:
	// an attempt already committed beside an execution that is not.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	l := NewLedger()
	// Earlier versions atomically wrote the same normalized tables plus a full
	// snapshot. Read those tables so upgrades do not depend on stale snapshots.
	rows, err := tx.Query(ctx, "SELECT data FROM executions ORDER BY data->>'created_at', id")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		var e Execution
		if err = json.Unmarshal(raw, &e); err != nil {
			rows.Close()
			return nil, err
		}
		l.Executions = append(l.Executions, &e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = tx.Query(ctx, "SELECT data FROM attempts ORDER BY execution_id,epoch")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		var a Attempt
		if err = json.Unmarshal(raw, &a); err != nil {
			rows.Close()
			return nil, err
		}
		l.Attempts = append(l.Attempts, &a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = tx.Query(ctx, "SELECT data FROM events ORDER BY sequence")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		var ev Event
		if err = json.Unmarshal(raw, &ev); err != nil {
			rows.Close()
			return nil, err
		}
		if ev.Sequence != int64(len(l.Events)+1) {
			rows.Close()
			return nil, fmt.Errorf("non-contiguous event sequence %d", ev.Sequence)
		}
		l.Events = append(l.Events, ev)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	return l, nil
}
func (p *PostgresStore) Save(caller context.Context, c *Change) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed != nil {
		return p.failed
	}
	// Client cancellation must not interrupt a ledger commit or close the sole
	// leader connection. This bounded transaction has its own lifecycle.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(caller), 30*time.Second)
	defer cancel()
	if !p.loaded {
		if _, err := p.load(ctx); err != nil {
			return err
		}
	}
	if err := checkSequence(p.lastSequence, c.Events); err != nil {
		return err
	}
	batch := &pgx.Batch{}
	for _, e := range c.Executions {
		raw, err := json.Marshal(e)
		if err != nil {
			return err
		}
		batch.Queue("INSERT INTO executions(id,data) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET data=excluded.data", e.ID, raw)
	}
	for _, a := range c.Attempts {
		raw, err := json.Marshal(a)
		if err != nil {
			return err
		}
		batch.Queue("INSERT INTO attempts(id,execution_id,epoch,data) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET data=excluded.data", a.ID, a.ExecutionID, a.Epoch, raw)
	}
	for _, ev := range c.Events {
		raw, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		batch.Queue("INSERT INTO events(sequence,kind,data) VALUES($1,$2,$3)", ev.Sequence, ev.Kind, raw)
	}
	if batch.Len() == 0 {
		return nil
	}
	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return p.fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	results := tx.SendBatch(ctx, batch)
	if err = results.Close(); err != nil {
		return p.fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return p.fatal(err)
	}
	p.lastSequence += int64(len(c.Events))
	return nil
}
