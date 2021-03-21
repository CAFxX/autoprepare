package autoprepare

import (
	"database/sql"
	"sync"
)

type stmt struct {
	cond      sync.Cond
	lock      sync.Mutex
	ps        *sql.Stmt
	psHandles uint32 // number of goroutines using ps
	hit       uint64
	q         string
}

func newStmt(sql string, hit uint64) *stmt {
	s := &stmt{q: sql, hit: hit}
	s.cond.L = &s.lock
	return s
}

func (s *stmt) acquire() *sql.Stmt {
	if s == nil {
		return nil
	}
	s.lock.Lock()
	ps := s.ps
	if ps != nil {
		s.psHandles += 1
	}
	s.lock.Unlock()
	return ps
}

func (s *stmt) release() {
	s.lock.Lock()
	s.psHandles -= 1
	if s.psHandles == 0 {
		s.cond.Broadcast()
	}
	s.lock.Unlock()
}

func (s *stmt) close() {
	s.lock.Lock()
	for s.psHandles > 0 {
		s.cond.Wait()
	}
	ps := s.ps
	s.ps = nil
	s.lock.Unlock()
	ps.Close()
}

func (s *stmt) put(v *sql.Stmt) {
	if v == nil {
		panic("nil *sql.Stmt")
	}
	s.lock.Lock()
	s.ps = v
	s.lock.Unlock()
}

func (s *stmt) prepared() (prepared bool) {
	s.lock.Lock()
	prepared = s.ps != nil
	s.lock.Unlock()
	return
}
