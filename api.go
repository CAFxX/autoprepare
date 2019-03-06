package sqlstmtcache

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
)

// Constructor, destructors and options

const (
	DefaultMaxQueryLen     = 4096
	DefaultMaxPreparedStmt = 16
	DefaultMaxStmt         = 1024
)

func New(db *sql.DB, opts ...SQLStmtCacheOpt) (*SQLStmtCache, error) {
	c := &SQLStmtCache{
		c:            db,
		maxPS:        DefaultMaxPreparedStmt,
		maxSqlLen:    DefaultMaxQueryLen,
		maxStmt:      DefaultMaxStmt,
		stmt:         make(map[string]*stmt),
		wrkThreshold: 5000,
	}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	runtime.SetFinalizer(c, func(_c *SQLStmtCache) {
		_c.Close()
	})
	return c, nil
}

type SQLStmtCacheOpt func(*SQLStmtCache) error

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
			c.psCount-- // FIXME:atomic
			ps.Close()
		}
	}
	c.stmt = nil
}

// Query functions

func (c *SQLStmtCache) QueryContext(ctx context.Context, sql string, values ...interface{}) (*sql.Rows, error) {
	s := c.getPS(ctx, sql)
	ps := s.acquire()
	if ps == nil {
		return c.c.QueryContext(ctx, sql, values...)
	}
	defer s.release()
	return ps.QueryContext(ctx, values...)
}

func (c *SQLStmtCache) ExecContext(ctx context.Context, sql string, values ...interface{}) (sql.Result, error) {
	s := c.getPS(ctx, sql)
	ps := s.acquire()
	if ps == nil {
		return c.c.ExecContext(ctx, sql, values...)
	}
	defer s.release()
	return ps.ExecContext(ctx, values...)
}

func (c *SQLStmtCache) QueryContextTx(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) (*sql.Rows, error) {
	s := c.getPS(ctx, sql)
	ps := s.acquire()
	if ps == nil {
		return tx.QueryContext(ctx, sql, values...)
	}
	defer s.release()
	return tx.StmtContext(ctx, ps).QueryContext(ctx, values...)
}

func (c *SQLStmtCache) ExecContextTx(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) (sql.Result, error) {
	s := c.getPS(ctx, sql)
	ps := s.acquire()
	if ps == nil {
		return tx.ExecContext(ctx, sql, values...)
	}
	defer s.release()
	return tx.StmtContext(ctx, ps).ExecContext(ctx, values...)
}
