package sqlstmtcache

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// TODO: call wrk() during GC, and have it more aggressive (eventually all PS should be closed)

type SQLStmtCache struct {
	l    sync.RWMutex
	stmt map[string]*stmt // protected by l

	psCount   uint32 // current number of prepared statements
	hit       uint32 // number of lookups since last wrk start
	wrkStatus uint32 // 0 wrk is not running, 1 wrk is running

	// configuration; constant after New() returns
	c            *sql.DB // database connection
	maxPS        uint32  // maximum number of prepared statements
	maxSqlLen    int     // maximum length of SQL statements to be cached
	maxStmt      int     // maximum number of tracked statements
	wrkThreshold uint32  // number of queries before a backgorund update
}

type stmt struct {
	ps  atomic.Value // *sql.Stmt
	hit uint32
	q   string
}

func (c *SQLStmtCache) QueryContext(ctx context.Context, sql string, values ...interface{}) (*sql.Rows, error) {
	ps := c.getPS(ctx, sql)
	if ps == nil {
		return c.c.QueryContext(ctx, sql, values...)
	}
	return ps.QueryContext(ctx, values...)
}

func (c *SQLStmtCache) ExecContext(ctx context.Context, sql string, values ...interface{}) (sql.Result, error) {
	ps := c.getPS(ctx, sql)
	if ps == nil {
		return c.c.ExecContext(ctx, sql, values...)
	}
	return ps.ExecContext(ctx, values...)
}

func (c *SQLStmtCache) QueryContextTx(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) (*sql.Rows, error) {
	ps := c.getPS(ctx, sql)
	if ps == nil {
		return tx.QueryContext(ctx, sql, values...)
	}
	return tx.StmtContext(ctx, ps).QueryContext(ctx, values...)
}

func (c *SQLStmtCache) ExecContextTx(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) (sql.Result, error) {
	ps := c.getPS(ctx, sql)
	if ps == nil {
		return tx.ExecContext(ctx, sql, values...)
	}
	return tx.StmtContext(ctx, ps).ExecContext(ctx, values...)
}

func (c *SQLStmtCache) getPS(ctx context.Context, query string) *sql.Stmt {
	if len(query) > c.maxSqlLen {
		return nil
	}

	c.l.RLock() // FIXME: ctx
	s, ok := c.stmt[query]
	c.l.RUnlock()

	hit := atomic.AddUint32(&c.hit, 1)
	if hit > c.wrkThreshold && atomic.CompareAndSwapUint32(&c.hit, hit, 0) {
		if atomic.CompareAndSwapUint32(&c.wrkStatus, 0, 1) {
			go func() {
				defer atomic.StoreUint32(&c.wrkStatus, 0)
				c.wrk()
			}()
		}
	}

	if !ok {
		c.l.Lock() // FIXME: ctx
		if len(c.stmt) < c.maxStmt {
			if s, ok = c.stmt[query]; !ok {
				c.stmt[query] = &stmt{hit: 1, q: query}
			}
		}
		c.l.Unlock()
		if !ok {
			return nil
		}
	}

	atomic.AddUint32(&s.hit, 1)
	return s.get()
}

func (c *SQLStmtCache) wrk() {
	victim, replacement := c.getCandidates()
	if victim != nil && c.psCount >= c.maxPS {
		s := victim.get()
		victim.put(nil)
		c.psCount--
		// FIXME: ensure no-one else is using s before Close()ing
		s.Close()
	}
	if replacement != nil && c.psCount < c.maxPS {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		ps, err := c.c.PrepareContext(ctx, replacement.q)
		if err == nil {
			replacement.put(ps)
			c.psCount++
		}
	}
	c.updateHits()
	c.dropStmts()
}

func (c *SQLStmtCache) getCandidates() (victim, replacement *stmt) {
	c.l.RLock()
	defer c.l.RUnlock()

	for _, s := range c.stmt {
		if s.get() != nil {
			if victim == nil || atomic.LoadUint32(&victim.hit) > atomic.LoadUint32(&s.hit) {
				victim = s
			}
		} else {
			if replacement == nil || atomic.LoadUint32(&replacement.hit) < atomic.LoadUint32(&s.hit) {
				replacement = s
			}
		}
	}

	if victim != nil && replacement == nil && atomic.LoadUint32(&victim.hit) > 0 {
		return nil, nil
	}
	if victim != nil && replacement != nil && atomic.LoadUint32(&victim.hit) >= atomic.LoadUint32(&replacement.hit) {
		return nil, nil
	}
	// TODO: do not promote replacements that represent less than a certain % of queries, e.g. p < 1/maxPS
	return
}

func (c *SQLStmtCache) updateHits() {
	c.l.RLock()
	for _, s := range c.stmt {
		var hit uint32
		for {
			hit = atomic.LoadUint32(&s.hit)
			if atomic.CompareAndSwapUint32(&s.hit, hit, hit/2) {
				break
			}
		}
	}
	c.l.RUnlock()
}

func (c *SQLStmtCache) dropStmts() {
	type _stmt struct {
		hit uint32
		q   string
	}

	c.l.RLock()

	if len(c.stmt) < c.maxStmt/2 {
		c.l.RUnlock()
		return
	}

	stmts := make([]_stmt, len(c.stmt))
	for q, s := range c.stmt {
		if s.get() == nil {
			stmts = append(stmts, _stmt{hit: atomic.LoadUint32(&s.hit), q: q})
		}
	}

	c.l.RUnlock()

	victims := len(stmts) - c.maxStmt/2

	sort.Slice(stmts, func(i, j int) bool {
		return stmts[i].hit < stmts[j].hit
	})

	c.l.Lock()
	for i, s := range stmts[:victims] {
		delete(c.stmt, s.q)
		if i%256 == 255 {
			c.l.Unlock()
			c.l.Lock()
		}
	}
	c.l.Unlock()
}

// Helpers for atomic access to prepared statement

var noPS = sql.Stmt{}

func (s *stmt) get() *sql.Stmt {
	v, _ := s.ps.Load().(*sql.Stmt)
	if v == &noPS {
		return nil
	}
	return v
}

func (s *stmt) put(v *sql.Stmt) {
	if v == nil {
		v = &noPS
	}
	s.ps.Store(v)
}

// Constructor, destructors and options

type SQLStmtCacheOpt func(*SQLStmtCache) error

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
	runtime.SetFinalizer(c, func(c *SQLStmtCache) {
		c.Close()
	})
	return c, nil
}

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
			// FIXME: ensure ps is used by no-one else before Close()ing
			ps.Close()
		}
	}
	c.stmt = nil
}
