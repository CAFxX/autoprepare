package autoprepare

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
	"sync/atomic"
)

// Constructor, destructors and options

const (
	DefaultMaxQueryLen     = 4096
	DefaultMaxPreparedStmt = 16
	DefaultMaxStmt         = 1024
	defaultWrkThreshold    = 5000
)

// New creates a new SQLStmtCache, with the provided options, that wraps the provided *sql.DB instance.
func New(db *sql.DB, opts ...SQLStmtCacheOpt) (*SQLStmtCache, error) {
	c := &SQLStmtCache{
		c:            db,
		maxPS:        DefaultMaxPreparedStmt,
		maxSqlLen:    DefaultMaxQueryLen,
		maxStmt:      DefaultMaxStmt,
		stmt:         make(map[string]*stmt),
		wrkThreshold: defaultWrkThreshold,
	}

	// apply user-supplied options
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	// automatically call Close() to destroy all PSs if the user
	// forgets to do it
	runtime.SetFinalizer(c, func(_c *SQLStmtCache) {
		_c.Close()
	})

	return c, nil
}

type SQLStmtCacheOpt func(*SQLStmtCache) error

// WithMaxPreparedStmt specifies the maximum number of prepared statements
// that will exist at any one time. It defaults to DefaultMaxPreparedStmt.
// Some databases (e.g. mysql) have limits to how many statements can be
// prepared at any one time, across all clients and connections: be sure not
// to set this number too high, or to use too many concurrent connections,
// or to use too many concurrent clients.
func WithMaxPreparedStmt(max int) SQLStmtCacheOpt {
	return func(c *SQLStmtCache) error {
		if max > 1<<12 {
			return errors.New("WithMaxPreparedStmt should be no more than 4096")
		}
		if max <= 0 {
			return errors.New("WithMaxPreparedStmt should be more than 0")
		}
		c.maxPS = uint32(max)
		return nil
	}
}

// WithMaxStmt specifies a soft upper limit on how many different SQL statements
// to track to be able to pick the most frequently used one, that will be promoted
// to a prepared statement. It defaults to DefaultMaxStmt.
func WithMaxStmt(max int) SQLStmtCacheOpt {
	return func(c *SQLStmtCache) error {
		if max > 1<<16 {
			return errors.New("WithMaxStmt should be no more than 65536")
		}
		if max < 128 {
			return errors.New("WithMaxStmt should be at least 128")
		}
		c.maxStmt = max
		return nil
	}
}

// WithMaxQueryLen specifies the maximum length of a SQL statement to be considered
// by autoprepare. Statements longer than this number are executed as-is and no
// prepared statements are ever cached. It defaults to DefaultMaxQueryLen.
func WithMaxQueryLen(max int) SQLStmtCacheOpt {
	return func(c *SQLStmtCache) error {
		if max > 1<<20 {
			return errors.New("WithMaxQueryLen should be no more than 1048576")
		}
		if max < 32 {
			return errors.New("WithMaxQueryLen should be at least 32")
		}
		c.maxSqlLen = max
		return nil
	}
}

// Close closes and frees all resources associated with the prepared statement cache.
// The SQLStmtCache should not be used after Close() has been called.
func (c *SQLStmtCache) Close() {
	c.l.Lock()
	defer c.l.Unlock()
	if c.stmt == nil {
		return
	}
	for _, s := range c.stmt {
		if ps := s.get(); ps != nil {
			s.put(nil)
			s.wait()
			atomic.AddUint32(&c.psCount, ^uint32(0))
			atomic.AddUint64(&c.stats.Unprepared, 1)
			ps.Close()
		}
	}
	c.stmt = nil
}

// Query functions

// QueryContext is equivalent to (*sql.DB).QueryContext, but it transparently creates and uses
// prepared statements for the most frequently-executed queries.
func (c *SQLStmtCache) QueryContext(ctx context.Context, sql string, values ...interface{}) (*sql.Rows, error) {
	s := c.getPS(ctx, sql)
	ps := s.acquire()
	if ps == nil {
		atomic.AddUint64(&c.stats.Misses, 1)
		return c.c.QueryContext(ctx, sql, values...)
	}
	defer s.release()
	atomic.AddUint64(&c.stats.Hits, 1)
	return ps.QueryContext(ctx, values...)
}

// QueryRowContext is equivalent to (*sql.DB).QueryRowContext, but it transparently creates and uses
// prepared statements for the most frequently-executed queries.
func (c *SQLStmtCache) QueryRowContext(ctx context.Context, sql string, values ...interface{}) *sql.Row {
	s := c.getPS(ctx, sql)
	ps := s.acquire()
	if ps == nil {
		atomic.AddUint64(&c.stats.Misses, 1)
		return c.c.QueryRowContext(ctx, sql, values...)
	}
	defer s.release()
	atomic.AddUint64(&c.stats.Hits, 1)
	return ps.QueryRowContext(ctx, values...)
}

// ExecContext is equivalent to (*sql.DB).ExecContext, but it transparently creates and uses
// prepared statements for the most frequently-executed queries.
func (c *SQLStmtCache) ExecContext(ctx context.Context, sql string, values ...interface{}) (sql.Result, error) {
	s := c.getPS(ctx, sql)
	ps := s.acquire()
	if ps == nil {
		atomic.AddUint64(&c.stats.Misses, 1)
		return c.c.ExecContext(ctx, sql, values...)
	}
	defer s.release()
	atomic.AddUint64(&c.stats.Hits, 1)
	return ps.ExecContext(ctx, values...)
}

// QueryContextTx is equivalent to tx.QueryContext, but it transparently creates and uses
// prepared statements for the most frequently-executed queries.
func (c *SQLStmtCache) QueryContextTx(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) (*sql.Rows, error) {
	s := c.getPS(ctx, sql)
	ps := s.acquire()
	if ps == nil {
		atomic.AddUint64(&c.stats.Misses, 1)
		return tx.QueryContext(ctx, sql, values...)
	}
	defer s.release()
	atomic.AddUint64(&c.stats.Hits, 1)
	return tx.StmtContext(ctx, ps).QueryContext(ctx, values...)
}

// QueryRowContextTx is equivalent to tx.QueryRowContext, but it transparently creates and uses
// prepared statements for the most frequently-executed queries.
func (c *SQLStmtCache) QueryRowContextTx(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) *sql.Row {
	s := c.getPS(ctx, sql)
	ps := s.acquire()
	if ps == nil {
		atomic.AddUint64(&c.stats.Misses, 1)
		return tx.QueryRowContext(ctx, sql, values...)
	}
	defer s.release()
	atomic.AddUint64(&c.stats.Hits, 1)
	return tx.StmtContext(ctx, ps).QueryRowContext(ctx, values...)
}

// ExecContextTx is equivalent to tx.ExecContext, but it transparently creates and uses
// prepared statements for the most frequently-executed queries.
func (c *SQLStmtCache) ExecContextTx(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) (sql.Result, error) {
	s := c.getPS(ctx, sql)
	ps := s.acquire()
	if ps == nil {
		atomic.AddUint64(&c.stats.Misses, 1)
		return tx.ExecContext(ctx, sql, values...)
	}
	defer s.release()
	atomic.AddUint64(&c.stats.Hits, 1)
	return tx.StmtContext(ctx, ps).ExecContext(ctx, values...)
}

// Statistics functions

type SQLStmtCacheStats struct {
	Prepared   uint64 // number of autoprepared statements created (Prepare() calls issued)
	Unprepared uint64 // number of autoprepared statements deleted (sql.(*Stmt).Close() calls issued)
	Hits       uint64 // number of SQL queries that used automatically-prepared statements
	Misses     uint64 // number of SQL queries executed raw
	Skips      uint64 // number of SQL queries that do not qualify for caching
}

// GetStats returns statistics about the state and effectiveness of the prepared statements cache.
func (c *SQLStmtCache) GetStats() SQLStmtCacheStats {
	return SQLStmtCacheStats{
		Hits:       atomic.LoadUint64(&c.stats.Hits),
		Misses:     atomic.LoadUint64(&c.stats.Misses),
		Skips:      atomic.LoadUint64(&c.stats.Skips),
		Prepared:   atomic.LoadUint64(&c.stats.Prepared),
		Unprepared: atomic.LoadUint64(&c.stats.Unprepared),
	}
}
