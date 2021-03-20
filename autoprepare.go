package autoprepare

import (
	"context"
	"database/sql"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// TODO: call wrk() during GC, and have it more aggressive (eventually all PS should be closed)

// SQLStmtCache transparently caches and uses prepared SQL statements.
type SQLStmtCache struct {
	l    sync.RWMutex
	stmt map[string]*stmt // protected by l

	psCount   uint32 // current number of prepared statements
	hit       uint32 // number of lookups since last wrk start
	wrkStatus uint32 // 0 wrk is not running, 1 wrk is running

	stats SQLStmtCacheStats

	// configuration; constant after New() returns
	c            *sql.DB // database connection
	maxPS        uint32  // maximum number of prepared statements
	maxSqlLen    int     // maximum length of SQL statements to be cached
	maxStmt      int     // maximum number of tracked statements
	wrkThreshold uint32  // number of queries before starting a backgorund update
}

func (c *SQLStmtCache) getPS(ctx context.Context, query string) *stmt {
	if c.maxPS == 0 {
		return nil
	}
	if len(query) > c.maxSqlLen {
		atomic.AddUint64(&c.stats.Skips, 1)
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
				// TODO: create a new object only once in N occurrences
				c.stmt[query] = &stmt{hit: 1, q: query}
			}
		}
		c.l.Unlock()
		if !ok {
			return nil
		}
	}

	atomic.AddUint32(&s.hit, 1)
	return s
}

func (c *SQLStmtCache) wrk() {
	victim, replacement := c.getCandidates()
	if victim != nil && atomic.LoadUint32(&c.psCount) >= c.maxPS {
		ps := victim.get()
		victim.put(nil)
		victim.wait()
		atomic.AddUint32(&c.psCount, ^uint32(0))
		atomic.AddUint64(&c.stats.Unprepared, 1)
		ps.Close()
	}
	if replacement != nil && c.psCount < c.maxPS {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ps, err := c.c.PrepareContext(ctx, replacement.q)
		// TODO: blacklist for statements that fail to be prepared
		if err == nil {
			replacement.put(ps)
			atomic.AddUint32(&c.psCount, 1)
			atomic.AddUint64(&c.stats.Prepared, 1)
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
	defer c.l.RUnlock()

	for _, s := range c.stmt {
		var hit uint32
		for {
			hit = atomic.LoadUint32(&s.hit)
			if atomic.CompareAndSwapUint32(&s.hit, hit, hit/2) {
				break
			}
		}
	}
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

	// we want to delete also all statements that have 0 hits
	for _, s := range stmts[victims:] {
		if s.hit != 0 {
			break
		}
		victims++
	}

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
